"""Runtime-only Airflow connections. Never serialize credentials into DAGs/XCom."""
from __future__ import annotations
import base64
import json
import math
import ssl
from urllib.parse import quote, urlencode, urlsplit
from urllib.request import Request, HTTPRedirectHandler, HTTPSHandler, build_opener
from airflow.sdk import BaseHook
from airflow.sdk.log import mask_secret
from dataslate import Client


class CredentialError(ValueError):
    pass


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def _unique(pairs):
    result={}
    for key,value in pairs:
        if key in result:raise CredentialError('Duplicate OAuth response field')
        result[key]=value
    return result


def _oauth_token(connection, extra, timeout, ca_file):
    endpoint=extra.get('token_endpoint');url=urlsplit(endpoint) if isinstance(endpoint,str) else None
    if (url is None or url.scheme!='https' or not url.hostname or url.username is not None
            or url.password is not None or url.query or url.fragment or any(c in endpoint for c in '\r\n')):
        raise CredentialError('OAuth token endpoint requires HTTPS without credentials, query or fragment')
    if not connection.login or not connection.password:
        raise CredentialError('OAuth client credentials require connection login and password')
    context=ssl.create_default_context()
    if ca_file is not None:context.load_verify_locations(cafile=ca_file)
    body={'grant_type':'client_credentials'}
    for key in ('scope','audience','resource'):
        if key in extra:
            value=extra[key]
            if not isinstance(value,str) or not value.strip() or len(value)>2048 or any(c in value for c in '\r\n'):
                raise CredentialError('Invalid OAuth parameter')
            body[key]=value
    credentials=quote(connection.login,safe='')+':'+quote(connection.password,safe='')
    request=Request(endpoint,data=urlencode(body).encode(),method='POST',headers={
        'Content-Type':'application/x-www-form-urlencoded','Accept':'application/json',
        'Authorization':'Basic '+base64.b64encode(credentials.encode()).decode()})
    try:
        with build_opener(_NoRedirect(),HTTPSHandler(context=context)).open(request,timeout=timeout) as response:
            raw=response.read(32769)
            if len(raw)>32768:raise CredentialError('OAuth response exceeds limit')
        data=json.loads(raw,object_pairs_hook=_unique)
        if not isinstance(data,dict):raise CredentialError('OAuth response must be an object')
        token=data.get('access_token');expires=data.get('expires_in')
        if (data.get('token_type','').lower()!='bearer' or not isinstance(token,str) or not token.strip()
                or len(token)>16384 or any(c.isspace() for c in token)
                or type(expires) not in (int,float) or not math.isfinite(expires) or not 30<=expires<=86400):
            raise CredentialError('OAuth response lacks a usable bounded Bearer credential')
    except Exception:
        # Do not put an upstream response, URL, password or access token in task logs.
        raise CredentialError('OAuth credential acquisition failed; no task request was sent') from None
    mask_secret(token)
    return token


class DataSlateHook(BaseHook):
    conn_name_attr='dataslate_conn_id'
    default_conn_name='dataslate_default'
    conn_type='dataslate'
    hook_name='DataSlate'

    def __init__(self, dataslate_conn_id='dataslate_default'):
        super().__init__();self.dataslate_conn_id=dataslate_conn_id

    def get_conn(self):
        try:
            connection=self.get_connection(self.dataslate_conn_id)
            extra=connection.extra_dejson
        except Exception:
            raise CredentialError('DataSlate connection could not be resolved') from None
        allowed={'auth_mode','token_endpoint','scope','audience','resource','ca_file','request_timeout'}
        if not isinstance(extra,dict) or set(extra)-allowed:raise CredentialError('Unknown DataSlate connection extra')
        timeout=extra.get('request_timeout',20)
        if type(timeout) not in (int,float) or not math.isfinite(timeout) or not 1<=timeout<=120:
            raise CredentialError('Request timeout must be 1–120 seconds')
        ca_file=extra.get('ca_file')
        if ca_file is not None and (not isinstance(ca_file,str) or not ca_file.startswith('/') or '\x00' in ca_file):
            raise CredentialError('CA file must be an explicit absolute path')
        mode=extra.get('auth_mode','bearer')
        if connection.schema or connection.port:
            raise CredentialError('Use a complete DataSlate root URL in the connection host')
        # Validate the API origin before contacting the token endpoint.
        Client(connection.host,timeout=timeout,ca_file=ca_file)
        if mode=='oauth_client_credentials':
            token=_oauth_token(connection,extra,timeout,ca_file)
        elif mode=='bearer':
            if connection.login or any(key in extra for key in ('token_endpoint','scope','audience','resource')):
                raise CredentialError('Bearer mode does not accept OAuth fields')
            token=connection.password
            if not isinstance(token,str) or not token.strip() or any(c.isspace() for c in token) or len(token)>16384:
                raise CredentialError('Bearer mode requires a connection password token')
            mask_secret(token)
        else:raise CredentialError('Unsupported DataSlate authentication mode')
        return Client(connection.host,token=token,timeout=timeout,ca_file=ca_file)
