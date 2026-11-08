"""Pure orchestration rules shared by submission and rescheduling sensors."""
from __future__ import annotations
import hashlib
import json
import re
from dataslate import DataSlateError


class OutcomeError(Exception):
    """A known failure/uncertainty needs explicit operator investigation."""


def identifier(value):
    if not isinstance(value,str) or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.:-]{0,255}',value):
        raise ValueError('Expected a bounded resource identifier')
    return value


def request_fields(operation,request):
    fields={'application':set(),'notebook':{'notebook_id','revision'},'ingestion':{'pipeline_id','revision'},
            'sql':{'notebook_id','revision','cell_id'},'publication':{'run_id'}}
    if not isinstance(operation,str) or operation not in fields or not isinstance(request,dict) or set(request)!=fields[operation]|{'workspace_id','binding_id'}:
        raise ValueError('Operation requires exactly its declared saved-resource fields')
    for key,value in request.items():
        if key=='revision':
            if type(value) is not int or value<1:raise ValueError('A positive saved revision is required')
        else:identifier(value)


def recovery_key(context,operation):
    ti=context.get('ti');run_id=context.get('run_id')
    parts=[getattr(ti,'dag_id',None),run_id,getattr(ti,'task_id',None),getattr(ti,'map_index',None),operation]
    if any(not isinstance(v,str) or not v or len(v)>512 for v in (parts[0],parts[1],parts[2])) or type(parts[3]) is not int or parts[3]<-1:
        raise ValueError('Stable Airflow DAG/run/task/map identity is required')
    # Excluding try_number preserves one logical operation across retry/clear/restart.
    return 'airflow-'+hashlib.sha256(json.dumps(parts,separators=(',',':'),ensure_ascii=False).encode()).hexdigest()


def _resource(value,request,operation,resource_id=None,minimum_revision=1):
    if (not isinstance(value,dict) or value.get('workspaceId')!=request['workspace_id']
            or value.get('bindingId')!=request['binding_id'] or type(value.get('revision')) is not int
            or value['revision']<minimum_revision or resource_id is not None and value.get('id')!=resource_id):
        raise DataSlateError('invalid_response','DataSlate operation identity did not match')
    try:identifier(value.get('id'))
    except ValueError:raise DataSlateError('invalid_response','DataSlate operation ID was invalid') from None
    if operation in ('notebook','ingestion','sql'):
        selection=value.get('ingestion' if operation=='ingestion' else 'notebook')
        expected={'id':request['pipeline_id' if operation=='ingestion' else 'notebook_id'],'revision':request['revision']}
        if operation=='sql':expected['cellId']=request['cell_id']
        if not isinstance(selection,dict) or any(selection.get(k)!=v or type(selection.get(k)) is not type(v) for k,v in expected.items()):
            raise DataSlateError('invalid_response','Saved-resource receipt did not match')
    if operation=='publication' and value.get('runId')!=request['run_id']:
        raise DataSlateError('invalid_response','Publication input run did not match')
    states={'queued','submitting','running','succeeded','failed','cancel_requested','cancelled','unknown'}
    if operation=='publication':states={'queued','checking','registering','verifying','published','failed','unknown'}
    if not isinstance(value.get('state'),str) or value['state'] not in states:
        raise DataSlateError('invalid_response','Unknown operation state')
    return value


def submit(client,operation,request,key):
    request_fields(operation,request);ws=request['workspace_id'];binding=request['binding_id']
    if operation=='application':accepted=client.create_run(ws,binding,idempotency_key=key)
    elif operation=='notebook':accepted=client.create_notebook_run(ws,binding,request['notebook_id'],request['revision'],idempotency_key=key)
    elif operation=='ingestion':accepted=client.create_ingestion_run(ws,binding,request['pipeline_id'],request['revision'],idempotency_key=key)
    elif operation=='sql':accepted=client.create_query(ws,binding,request['notebook_id'],request['revision'],request['cell_id'],idempotency_key=key)
    else:accepted=client.create_table_publication(ws,request['run_id'],binding,idempotency_key=key)
    resource_key='query' if operation=='sql' else 'publication' if operation=='publication' else 'run'
    if not isinstance(accepted,dict) or type(accepted.get('replayed')) is not bool:
        raise DataSlateError('invalid_response','Submission acceptance is unconfirmed')
    resource=_resource(accepted.get(resource_key),request,operation)
    # Only identifiers/provenance leave this task; never logs, source, storage paths or result rows.
    return {'version':1,'operation':operation,'request':dict(request),'resourceId':resource['id'],'minimumRevision':resource['revision']}


def reference_fields(reference):
    if (not isinstance(reference,dict) or set(reference)!={'version','operation','request','resourceId','minimumRevision'}
            or type(reference['version']) is not int or reference['version']!=1
            or type(reference['minimumRevision']) is not int or reference['minimumRevision']<1):
        raise ValueError('A minimal versioned DataSlate operation reference is required')
    request_fields(reference['operation'],reference['request']);identifier(reference['resourceId'])


def poll(client,reference):
    reference_fields(reference);operation=reference['operation'];request=reference['request'];ws=request['workspace_id'];rid=reference['resourceId']
    method=client.query if operation=='sql' else client.table_publication if operation=='publication' else client.run
    value=_resource(method(ws,rid),request,operation,rid,reference['minimumRevision']);state=value['state']
    if state in ('failed','cancelled'):raise OutcomeError('DataSlate operation failed or was cancelled; no replacement execution was submitted')
    if operation=='publication':
        if type(value.get('reconciliationComplete')) is not bool:raise DataSlateError('invalid_response','Publication reconciliation flag missing')
        if state=='unknown' and value['reconciliationComplete']:raise OutcomeError('Final publication outcome is unknown; investigate the same operation')
        if state=='published':
            if value.get('reconciliationComplete') is not True or value.get('publishedTableId')!=request['binding_id'] or not value.get('verifiedAt'):
                raise DataSlateError('invalid_response','Publication completion lacks verification')
            return True
        return False
    if state!='succeeded':return False
    if operation=='sql':
        if value.get('submissionStarted') is not True or not value.get('engineQueryId'):
            raise DataSlateError('invalid_response','SQL completion lacks engine identity')
        return True
    if value.get('submissionStarted') is not True or not value.get('kubernetesUid'):
        raise DataSlateError('invalid_response','Run completion lacks compute identity')
    artifact=value.get('sourceArtifact')
    if operation in ('notebook','ingestion') and not isinstance(artifact,dict):
        raise DataSlateError('invalid_response','Saved-source cleanup evidence is absent')
    if artifact is not None and (not isinstance(artifact,dict) or artifact.get('state')!='deleted'):return False
    if operation=='ingestion':
        if value.get('writerReleased') is not True:return False
        outcome=client.ingestion_outcome(ws,rid)
        if not isinstance(outcome,dict) or outcome.get('runId')!=rid or outcome.get('workspaceId')!=ws:
            raise DataSlateError('invalid_response','Ingestion evidence identity did not match')
        if outcome.get('state')=='pending':return False
        if outcome.get('state')!='verified':raise OutcomeError('Ingestion finished without verified runtime evidence')
        receipt=outcome.get('receipt')
        if (not isinstance(receipt,dict) or type(receipt.get('version')) is not int or receipt['version']!=2
                or receipt.get('runId')!=rid or receipt.get('pipelineId')!=request['pipeline_id']
                or type(receipt.get('pipelineRevision')) is not int or receipt['pipelineRevision']!=request['revision']
                or receipt.get('state')!='succeeded' or type(receipt.get('tableExists')) is not bool):
            raise DataSlateError('invalid_response','Ingestion runtime receipt is incomplete or mismatched')
    return True
