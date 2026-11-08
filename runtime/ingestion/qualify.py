"""Run the real JVM suite with a verified local dependency set (no downloads)."""
import argparse
import json
import os
from pathlib import Path
import shlex
import subprocess
import sys

from download_jars import fetch


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--jar-directory', type=Path, required=True)
    parser.add_argument('--java-home', type=Path, default=Path(os.environ['JAVA_HOME']) if 'JAVA_HOME' in os.environ else None)
    parser.add_argument('--python', type=Path, default=Path(sys.executable))
    parser.add_argument('--scenario', help='Optional exact unittest method for focused requalification')
    args = parser.parse_args()
    if args.java_home is None or not (args.java_home/'bin'/'java').is_file():
        parser.error('an installed Java 21 JAVA_HOME is required')
    root = Path(__file__).resolve().parent
    lock = root/'jars.lock.json'
    fetch(lock, args.jar_directory, offline=True)
    python = str(args.python.absolute())  # Preserve the venv executable symlink.
    result = subprocess.run([python, '-c', 'import pyspark; print(pyspark.__version__)'], check=True, text=True, capture_output=True)
    if result.stdout.strip() != '4.1.3':
        parser.error('the selected Python must have pyspark==4.1.3 installed')
    env = dict(os.environ)
    env.update(JAVA_HOME=str(args.java_home.resolve()), PYSPARK_PYTHON=python,
               PYTHONPATH=str(root), AWS_EC2_METADATA_DISABLED='true')
    jars = ','.join(str(args.jar_directory.resolve()/entry['name']) for entry in json.loads(lock.read_text()))
    env['PYSPARK_SUBMIT_ARGS'] = shlex.join(['--master', 'local[2]', '--conf', 'spark.ui.enabled=false', '--conf', 'spark.ui.showConsoleProgress=false', '--conf', 'spark.sql.shuffle.partitions=2', '--conf', 'spark.databricks.delta.snapshotPartitions=2', '--jars', jars, 'pyspark-shell'])
    command = [python, str(root/'tests'/'qualify_spark.py')]
    if args.scenario:
        if not args.scenario.startswith('test_') or not args.scenario.replace('_', '').isalnum():
            parser.error('invalid scenario method')
        command.append('SparkQualification.' + args.scenario)
    return subprocess.run(command, env=env, check=False).returncode


if __name__ == '__main__':
    raise SystemExit(main())
