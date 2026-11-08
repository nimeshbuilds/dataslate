"""Offline real-JVM qualification inside the already-built ingestion image."""
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys

spark=Path(os.environ.get('SPARK_HOME','/opt/spark'))
root=Path(__file__).resolve().parent
jars=json.loads((root/'jars.lock.json').read_text())
for entry in jars:
    path=spark/'jars'/entry['name']
    digest=hashlib.sha256()
    if path.is_file():
        with path.open('rb') as stream:
            for chunk in iter(lambda: stream.read(1024*1024), b''):
                digest.update(chunk)
    if not path.is_file() or digest.hexdigest()!=entry['sha256']:
        raise SystemExit('Missing or mismatched pinned runtime jar: '+entry['name'])
py4j=list((spark/'python/lib').glob('py4j-*-src.zip'))
if len(py4j)!=1: raise SystemExit('Expected one bundled Py4J distribution')
env=dict(os.environ)
env.update(PYTHONPATH=os.pathsep.join([str(spark/'python'),str(py4j[0]),'/opt/dataslate/python']),
           PYSPARK_PYTHON=sys.executable,AWS_EC2_METADATA_DISABLED='true',SPARK_LOCAL_IP='127.0.0.1',
           PYSPARK_SUBMIT_ARGS='--master local[2] --conf spark.ui.enabled=false --conf spark.ui.showConsoleProgress=false --conf spark.sql.shuffle.partitions=2 --conf spark.databricks.delta.snapshotPartitions=2 pyspark-shell')
version=subprocess.run([sys.executable,'-c','import pyspark; print(pyspark.__version__)'],env=env,text=True,capture_output=True,check=True).stdout.strip()
if version!='4.1.3': raise SystemExit('Unexpected runtime Spark version')
raise SystemExit(subprocess.run([sys.executable,str(root/'tests/qualify_spark.py')],env=env,check=False).returncode)
