#!/usr/bin/env python3
"""Exercise database boundaries in the private qualify-core-fixtures.py cluster.

Fixture-only: this deliberately changes a test role's membership/schema ownership
and restores both. Never use against a production database. Credentials travel
through stdin, never argv, output or the report. No cloud discovery is performed.
"""
import argparse
import json
from pathlib import Path
import subprocess


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--kubeconfig', required=True, type=Path)
    p.add_argument('--context', required=True)
    p.add_argument('--private-dir', required=True, type=Path)
    p.add_argument('--output', required=True, type=Path)
    p.add_argument('--kubectl', default='kubectl')
    args = p.parse_args()
    if args.context != 'colima-dataslate-core-qualification':
        p.error('only the explicitly named disposable qualification context is supported')
    if args.output.exists():
        p.error('choose a new output file')
    credentials = json.loads((args.private_dir / 'credentials.json').read_text())
    grants = (Path(__file__).resolve().parents[1] / 'deploy/setup/runtime-role.sql').read_text()
    command = [args.kubectl, '--kubeconfig', str(args.kubeconfig), '--context', args.context,
               '-n', 'dataslate-core-fixture', 'exec', '-i', 'deployment/postgres', '--']
    results = []

    def sql(statement, role='postgres', grant_script=False, variables=None):
        flags = ['psql', '-X', '-A', '-t', '-v', 'ON_ERROR_STOP=1', '-d', 'dataslate']
        data = statement
        if role == 'postgres':
            cmd = command + flags + ['-U', role]
        else:
            # The shell consumes one password line; psql reads only the SQL that
            # follows. These fixed fixture passwords contain no newline.
            shell = 'IFS= read -r PGPASSWORD; export PGPASSWORD; export PGSSLMODE=verify-full PGSSLROOTCERT=/tls/ca.crt; exec "$@"'
            cmd = command + ['sh', '-c', shell, 'fixture-sql'] + flags + ['-h',
                'postgres.dataslate-core-fixture.svc.cluster.local', '-U', role]
            data = credentials['runtime' if role == 'dataslate_runtime' else 'migrator'] + '\n' + statement
        if grant_script:
            selected = variables if variables is not None else [
                'database=dataslate', 'migration_role=dataslate_migrator', 'runtime_role=dataslate_runtime']
            for variable in selected:
                cmd += ['-v', variable]
        return subprocess.run(cmd, input=data, text=True, capture_output=True, timeout=45)

    def check(name, good):
        if not good:
            raise RuntimeError('Fixture check failed: ' + name)
        results.append({'check': name, 'status': 'passed'})

    result = sql("SELECT current_user || '|' || ssl::text FROM pg_stat_ssl WHERE pid=pg_backend_pid();", 'dataslate_runtime')
    check('runtime identity and verified TLS connection', result.returncode == 0 and result.stdout.strip() == 'dataslate_runtime|true')
    result = sql("BEGIN; INSERT INTO dataslate_workspaces VALUES ('sql-boundary-fixture','Boundary fixture','sql-boundary-fixture',now(),now(),1); UPDATE dataslate_workspaces SET name='Changed' WHERE id='sql-boundary-fixture'; SELECT name FROM dataslate_workspaces WHERE id='sql-boundary-fixture'; DELETE FROM dataslate_workspaces WHERE id='sql-boundary-fixture'; ROLLBACK;", 'dataslate_runtime')
    check('application CRUD with runtime principal', result.returncode == 0 and 'Changed' in result.stdout)
    for name, statement in [
        ('public table DDL denied', 'CREATE TABLE public.forbidden_fixture(id int);'),
        ('database schema DDL denied', 'CREATE SCHEMA forbidden_fixture;'),
        ('migration ledger insert denied', 'INSERT INTO dataslate_schema_migrations SELECT * FROM dataslate_schema_migrations LIMIT 0;'),
        ('migration ledger update denied', 'UPDATE dataslate_schema_migrations SET version=version WHERE false;'),
        ('migration ledger delete denied', 'DELETE FROM dataslate_schema_migrations WHERE false;'),
    ]:
        result = sql('BEGIN; ' + statement + ' ROLLBACK;', 'dataslate_runtime')
        check(name, result.returncode != 0 and 'permission denied' in result.stderr)
    result = sql('SELECT count(*) FROM dataslate_schema_migrations;', 'dataslate_runtime')
    check('runtime can validate the migration ledger', result.returncode == 0 and int(result.stdout.strip()) > 0)
    result = sql(grants, 'dataslate_migrator', True)
    check('grant script is repeatable as the real TLS migration identity', result.returncode == 0)
    for name, variables in [
        ('missing database variable fails', []),
        ('missing migration identity variable fails', ['database=dataslate']),
        ('missing runtime identity variable fails', ['database=dataslate', 'migration_role=dataslate_migrator']),
    ]:
        result = sql(grants, 'dataslate_migrator', True, variables)
        check(name, result.returncode == 3 and 'DataSlate runtime role safety check failed' in result.stderr
              and 'runtime grants applied' not in result.stdout)
    result = sql(grants, 'dataslate_runtime', True)
    check('grant script rejects the runtime identity as its operator', result.returncode == 3
          and 'DataSlate runtime role safety check failed' in result.stderr)

    # Effective SET ROLE authority must be rejected even with NOINHERIT.
    try:
        result = sql('ALTER ROLE dataslate_runtime NOINHERIT; GRANT dataslate_migrator TO dataslate_runtime;')
        check('unsafe membership fixture established', result.returncode == 0)
        result = sql(grants, 'dataslate_migrator', True)
        check('grant script rejects NOINHERIT membership', result.returncode == 3 and 'must not be a member' in result.stdout)
    finally:
        restored = sql('REVOKE dataslate_migrator FROM dataslate_runtime; ALTER ROLE dataslate_runtime INHERIT;')
        if restored.returncode:
            raise RuntimeError('Could not restore private membership fixture')
    try:
        result = sql('ALTER SCHEMA public OWNER TO dataslate_runtime;')
        check('unsafe schema ownership fixture established', result.returncode == 0)
        result = sql(grants, 'dataslate_migrator', True)
        check('grant script rejects runtime schema ownership', result.returncode == 3 and 'unsafe runtime role/ownership' in result.stdout)
    finally:
        restored = sql('ALTER SCHEMA public OWNER TO pg_database_owner;')
        if restored.returncode:
            raise RuntimeError('Could not restore private schema ownership fixture')
    result = sql(grants, 'dataslate_migrator', True)
    check('runtime boundary restored after negative checks', result.returncode == 0)
    with args.output.open('x') as out:
        json.dump({'scope': 'disposable fixture only; not production qualification', 'checks': results}, out, indent=2)
        out.write('\n')
    print(f'PASS {len(results)} private database checks; no credential values recorded')


if __name__ == '__main__':
    try:
        main()
    except RuntimeError as exc:
        raise SystemExit(str(exc)) from None
    except (OSError, ValueError, subprocess.SubprocessError):
        raise SystemExit('Private database qualification could not complete; no private subprocess output emitted') from None
