"""Commit this reviewed DAG into a versioned GitDagBundle. Replace resource IDs."""
from datetime import datetime, timezone
from airflow.sdk import DAG
from dataslate_airflow.operators import DataSlateSubmitOperator, DataSlateOperationSensor

with DAG(
    dag_id='dataslate_orders_daily', schedule='0 6 * * *',
    start_date=datetime(2026,11,1,tzinfo=timezone.utc),catchup=False,max_active_runs=1,
    render_template_as_native_obj=True,
    default_args={'retries':2},
    tags=['dataslate','engineering'],
) as dag:
    ingest=DataSlateSubmitOperator(task_id='ingest',operation='ingestion',request={
        'workspace_id':'team','binding_id':'ingest','pipeline_id':'pipeline-orders','revision':1})
    ingestion_done=DataSlateOperationSensor(task_id='await_verified_ingestion',reference=ingest.output)
    publish=DataSlateSubmitOperator(task_id='publish',operation='publication',request={
        'workspace_id':'team','binding_id':'publish-orders','run_id':ingestion_done.output['resourceId']})
    published=DataSlateOperationSensor(task_id='await_publication',reference=publish.output)
    query=DataSlateSubmitOperator(task_id='query',operation='sql',request={
        'workspace_id':'team','binding_id':'read-orders','notebook_id':'sql-orders','revision':1,'cell_id':'summary'})
    query_done=DataSlateOperationSensor(task_id='await_query',reference=query.output)
    published >> query
