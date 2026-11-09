"""Actual psql guard regression; opt in only with an isolated test database URL.

No roles, schema or privileges are changed. The full grants script is executed
with inputs that must fail before its first GRANT/REVOKE. PostgreSQL 17 ignores
an argument to \\quit, so checking the real exit status is essential.
"""
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest
from urllib.parse import parse_qsl, unquote, urlsplit

SQL = Path(__file__).with_name('runtime-role.sql')
FAILURE = 'DataSlate runtime role safety check failed'
SUCCESS = 'DataSlate runtime grants applied'
MISSING_MIGRATOR = 'dataslate_ci_guard_migration_never_exists'
MISSING_RUNTIME = 'dataslate_ci_guard_runtime_never_exists'
SERVICE = 'dataslate-guard-test'
QUERY_FIELDS = {'hostaddr', 'sslmode', 'sslrootcert', 'sslcert', 'sslkey', 'sslcrl',
                'sslcrldir', 'sslpassword', 'ssl_min_protocol_version',
                'ssl_max_protocol_version', 'channel_binding', 'gssencmode'}


def service_options(uri):
    """Translate only explicit URI settings; never permit service-file injection."""
    try:
        if not isinstance(uri, str) or len(uri) > 16384 or any(ord(c) < 32 or ord(c) == 127 for c in uri):
            raise ValueError('invalid URI')
        parsed = urlsplit(uri)
        if (parsed.scheme not in ['postgres', 'postgresql'] or not parsed.hostname
                or parsed.username is None or not parsed.path.startswith('/')
                or not parsed.path[1:] or parsed.fragment):
            raise ValueError('incomplete URI')
        port = parsed.port or 5432
        if not 1 <= port <= 65535 or parsed.port == 0:
            raise ValueError('invalid port')
        options = {'host': unquote(parsed.hostname), 'port': str(port),
                   'user': unquote(parsed.username), 'dbname': unquote(parsed.path[1:]),
                   'connect_timeout': '10', 'application_name': 'dataslate-runtime-role-guard-test',
                   'passfile': os.devnull}
        if parsed.password is not None:
            options['password'] = unquote(parsed.password)
        for key, value in parse_qsl(parsed.query, keep_blank_values=True, strict_parsing=True):
            if key not in QUERY_FIELDS or key in options:
                raise ValueError('unsupported or duplicate option')
            options[key] = value
        for key, value in options.items():
            # PG17 service files are literal key=value lines, without an escape
            # mechanism; their parser also strips trailing whitespace.
            if (any(ord(c) < 32 or ord(c) == 127 for c in value) or value != value.rstrip()
                    or len((key + '=' + value).encode('utf-8')) >= 1022
                    or (not value and key != 'password')):
                raise ValueError('unsafe service value')
        return options
    except (TypeError, ValueError, UnicodeError):
        raise RuntimeError('Test database URL has unsupported or unsafe connection settings') from None


class ServiceConfigurationTests(unittest.TestCase):
    def test_uri_preserves_explicit_host_address_tls_and_decoded_auth(self):
        options = service_options('postgresql://migrator:example%3Avalue%23suffix@postgres.example.test:55432/dataslate?hostaddr=127.0.0.1&sslmode=verify-full&sslrootcert=%2Ftmp%2Ftest%20ca.pem')
        self.assertEqual(options['host'], 'postgres.example.test')
        self.assertEqual(options['port'], '55432')
        self.assertEqual(options['hostaddr'], '127.0.0.1')
        self.assertEqual(options['sslmode'], 'verify-full')
        self.assertEqual(options['sslrootcert'], '/tmp/test ca.pem')
        self.assertEqual(options['password'], 'example:value#suffix')
        self.assertEqual(options['passfile'], os.devnull)

    def test_rejects_injection_unknown_and_duplicate_options(self):
        base = 'postgresql://user:example@localhost:5432/dataslate'
        for uri in [base.replace('example@', 'example%0ahost%3Devil@'),
                    base + '?sslrootcert=%2Ftmp%2Fca%0d%0ahost%3Devil',
                    base + '?service=other', base + '?hostaddr=127.0.0.1&hostaddr=127.0.0.2',
                    base + '?sslmode=disable&options=-c%20statement_timeout%3D0',
                    base.replace('example@', 'example%20@'), base + '\n']:
            with self.subTest(case='invalid service settings'):
                with self.assertRaisesRegex(RuntimeError, '^Test database URL has unsupported or unsafe connection settings$'):
                    service_options(uri)


@unittest.skipUnless(os.environ.get('DATASLATE_TEST_DATABASE_URL'),
                     'Set DATASLATE_TEST_DATABASE_URL to an isolated PostgreSQL test database')
class RuntimeRolePostgresTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.psql = shutil.which('psql')
        if not cls.psql:
            raise RuntimeError('Explicit PostgreSQL guard test requires psql on PATH')
        options = service_options(os.environ['DATASLATE_TEST_DATABASE_URL'])
        private = tempfile.TemporaryDirectory(prefix='dataslate-pg-guard-')
        cls.addClassCleanup(private.cleanup)  # Runs even if setUpClass fails.
        Path(private.name).chmod(0o700)
        service_file = Path(private.name) / 'pg_service.conf'
        descriptor = os.open(service_file, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(descriptor, 'w') as stream:
            stream.write('[' + SERVICE + ']\n')
            for key, value in options.items():
                stream.write(key + '=' + value + '\n')
        # Do not inherit another session's host, credentials, TLS settings,
        # PGOPTIONS or service selection. No connection values go into argv.
        cls.env = {k: v for k, v in os.environ.items()
                   if not k.startswith('PG') and k != 'DATASLATE_TEST_DATABASE_URL'}
        cls.env.update({'PGSERVICEFILE': str(service_file), 'PGSERVICE': SERVICE})
        cls.sql = SQL.read_text()
        result = cls.invoke(
            "SELECT json_build_object('database', current_database(), 'role_exists', "
            "EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '" + MISSING_MIGRATOR + "'))::text;"
        )
        if result.returncode != 0:
            raise RuntimeError('Could not read the explicitly selected test database identity')
        try:
            identity = json.loads(result.stdout)
            if (not isinstance(identity, dict) or not isinstance(identity.get('database'), str)
                    or not identity['database'] or identity.get('role_exists') is not False):
                raise ValueError('invalid identity')
            cls.database = identity['database']
        except (TypeError, ValueError):
            raise RuntimeError('Test database identity or deliberately absent role precondition is invalid') from None

    @classmethod
    def invoke(cls, sql, variables=()):
        argv = [cls.psql, '-X', '--no-password', '--quiet', '--no-align', '--tuples-only',
                '--set', 'ON_ERROR_STOP=1']
        for name, value in variables:
            argv.extend(['--set', name + '=' + value])
        try:
            result = subprocess.run(argv, input=sql, text=True, capture_output=True,
                                    env=cls.env, timeout=15, check=False)
        except (OSError, subprocess.TimeoutExpired):
            raise RuntimeError('PostgreSQL guard invocation unavailable or exceeded its 15-second bound') from None
        if len(result.stdout) + len(result.stderr) > 65536:
            raise RuntimeError('PostgreSQL guard response exceeded its output bound')
        return result

    def rejected(self, variables):
        result = self.invoke(self.sql, variables)
        self.assertEqual(result.returncode, 3, 'Guard rejection must make actual psql exit 3')
        # assertIn would include raw server stderr in a failing assertion.
        self.assertTrue(FAILURE in result.stderr, 'Expected the constant guard exception')
        self.assertFalse(SUCCESS in result.stdout or SUCCESS in result.stderr,
                         'Rejected grant script must never announce success')

    def test_missing_database_variable_exits_nonzero(self):
        self.rejected(())

    def test_missing_migration_role_variable_exits_nonzero(self):
        self.rejected((('database', self.database),))

    def test_missing_runtime_role_variable_exits_nonzero(self):
        self.rejected((('database', self.database), ('migration_role', MISSING_MIGRATOR)))

    def test_wrong_migration_identity_exits_nonzero_before_grants(self):
        self.rejected((('database', self.database), ('migration_role', MISSING_MIGRATOR),
                       ('runtime_role', MISSING_RUNTIME)))


if __name__ == '__main__':
    unittest.main()
