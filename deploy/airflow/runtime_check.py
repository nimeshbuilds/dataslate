"""Strict, secret-safe startup contract for the optional managed Airflow profile."""
import argparse
import base64
from importlib import import_module, metadata
import json
import os
from pathlib import Path
import re
import ssl
from urllib.parse import parse_qsl, urlsplit

TRUST_FILE = '/etc/airflow-trust/ca.crt'


def validate_database_url(value):
    """Force the same verified libpq TLS behavior in synchronous and async engines."""
    try:
        url = urlsplit(value)
        query = parse_qsl(url.query, strict_parsing=True)
        if (not isinstance(value, str) or len(value) > 8192 or any(ord(c) < 33 for c in value)
                or url.scheme != 'postgresql+psycopg' or not url.hostname
                or not url.username or not url.password or url.fragment
                or not re.fullmatch(r'/[A-Za-z][A-Za-z0-9_]{0,62}', url.path)
                or url.port is None or not 1 <= url.port <= 65535
                or len(query) != 2 or dict(query) != {'sslmode': 'verify-full', 'sslrootcert': TRUST_FILE}):
            raise ValueError()
    except Exception:
        raise ValueError('Metadata database requires the documented psycopg verified TLS URI') from None


def validate_environment(env, require_jwt=False):
    validate_database_url(env.get('AIRFLOW__DATABASE__SQL_ALCHEMY_CONN', ''))
    # No command/secret backend or async URI may override the checked URI or trust policy.
    blocked = (
        'AIRFLOW__DATABASE__SQL_ALCHEMY_CONN_CMD', 'AIRFLOW__DATABASE__SQL_ALCHEMY_CONN_SECRET',
        'AIRFLOW__DATABASE__SQL_ALCHEMY_CONN_ASYNC', 'AIRFLOW__DATABASE__SQL_ALCHEMY_CONN_ASYNC_CMD',
        'AIRFLOW__DATABASE__SQL_ALCHEMY_CONN_ASYNC_SECRET', 'PGSERVICE', 'PGSERVICEFILE',
        'AIRFLOW__DATABASE__SQL_ALCHEMY_CONNECT_ARGS', 'AIRFLOW__DATABASE__SQL_ALCHEMY_CONNECT_ARGS_ASYNC',
        'AIRFLOW__DATABASE__SQL_ALCHEMY_ENGINE_ARGS',
    )
    if any(key in env for base in blocked for key in (base, base + '_CMD', base + '_SECRET')):
        raise ValueError('Database override is not supported')
    if 'AIRFLOW__API__SECRET_KEY' not in env or (require_jwt and 'AIRFLOW__API_AUTH__JWT_SECRET' not in env):
        raise ValueError('Required API signing material must be externally managed')
    for key in ('AIRFLOW__API__SECRET_KEY', 'AIRFLOW__API_AUTH__JWT_SECRET'):
        if key in env and (len(env[key]) < 32 or len(env[key]) > 4096 or '\n' in env[key]):
            raise ValueError('Signing material must be independently generated and at least 32 characters')
    try:
        fernet = env['AIRFLOW__CORE__FERNET_KEY']
        if len(fernet) != 44 or len(base64.urlsafe_b64decode(fernet)) != 32:
            raise ValueError()
    except Exception:
        raise ValueError('A valid externally managed Fernet key is required') from None
    ssl.create_default_context(cafile=TRUST_FILE)


def verify_imports():
    expected = {'apache-airflow': '3.4.0', 'apache-airflow-providers-fab': '3.10.2',
                'apache-airflow-providers-git': '1.0.1', 'psycopg': '3.3.6',
                'dataslate': '0.1.0a1', 'dataslate-airflow-provider': '0.1.0a1'}
    for package, version in expected.items():
        if metadata.version(package) != version:
            raise ValueError('Runtime package version mismatch')
    for module in ('airflow.providers.fab.auth_manager.fab_auth_manager',
                   'airflow.providers.git.bundles.git', 'dataslate_airflow.hooks',
                   'dataslate_airflow.operators', 'psycopg'):
        import_module(module)
    from airflow.providers_manager import ProvidersManager
    if 'dataslate-airflow-provider' not in ProvidersManager().providers:
        raise ValueError('DataSlate provider discovery failed')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--imports-only', action='store_true')
    args = parser.parse_args()
    try:
        verify_imports()
        if not args.imports_only:
            validate_environment(os.environ)
    except Exception:
        raise SystemExit('DataSlate Airflow runtime verification failed') from None
    print('DataSlate Airflow runtime imports verified' if args.imports_only else 'DataSlate Airflow runtime configuration verified')
