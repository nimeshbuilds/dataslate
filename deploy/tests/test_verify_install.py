"""Exercise the installation verifier over real HTTPS, without a cluster."""
import json
from pathlib import Path
import ssl
import subprocess
import sys
import tempfile
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

VERIFIER = Path(__file__).resolve().parents[1] / 'scripts/verify-install.py'


class VerifyInstallTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.directory = tempfile.TemporaryDirectory(prefix='dataslate-verify-')
        cls.root = Path(cls.directory.name)
        cls.cert = cls.root / 'cert.pem'
        subprocess.run(['openssl', 'req', '-x509', '-newkey', 'rsa:2048', '-nodes',
                        '-days', '1', '-keyout', str(cls.root / 'key.pem'), '-out', str(cls.cert),
                        '-subj', '/CN=localhost', '-addext', 'subjectAltName=DNS:localhost'],
                       check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=20)

    @classmethod
    def tearDownClass(cls):
        cls.directory.cleanup()

    def setUp(self):
        self.mode = 'valid'
        self.paths = []
        outer = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                outer.paths.append(self.path)
                if outer.mode == 'redirect':
                    self.send_response(302)
                    self.send_header('Location', '/redirected')
                    self.end_headers()
                    return
                if self.path == '/api/v1/auth/config':
                    payload = {'mode': 'oidc', 'issuer': 'https://identity.example.com',
                               'clientId': 'dataslate-web', 'audience': 'dataslate-api'}
                    if outer.mode == 'development':
                        payload['mode'] = 'development'
                    if outer.mode == 'malformed':
                        payload['issuer'] = 123
                    data = json.dumps(payload).encode()
                else:
                    data = b'{}'
                if outer.mode == 'oversized':
                    data = b'x' * 65537
                self.send_response(401 if self.path == '/api/v1/status' else 200)
                self.send_header('Content-Length', str(len(data)))
                self.end_headers()
                self.wfile.write(data)

            def log_message(self, *_):
                pass

        self.server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        ctx.load_cert_chain(self.cert, self.root / 'key.pem')
        self.server.socket = ctx.wrap_socket(self.server.socket, server_side=True)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.url = 'https://localhost:' + str(self.server.server_port)

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=3)

    def run_verifier(self, *, trusted=True, url=None, ca=None):
        args = [sys.executable, str(VERIFIER), '--url', url or self.url]
        if trusted:
            args += ['--ca-file', str(ca or self.cert)]
        return subprocess.run(args, capture_output=True, text=True, timeout=20)

    def test_private_trust_and_all_five_checks(self):
        result = self.run_verifier()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(len(self.paths), 5)
        self.assertIn('PASS /api/v1/status: HTTP 401', result.stdout)

    def test_untrusted_ca_denied_without_traceback(self):
        result = self.run_verifier(trusted=False)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('HTTPS connection failed', result.stderr)
        self.assertNotIn('Traceback', result.stderr)

    def test_hostname_mismatch_is_not_bypassed_by_ca(self):
        result = self.run_verifier(url=self.url.replace('localhost', '127.0.0.1'))
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(self.paths, [])

    def test_redirect_is_not_followed(self):
        self.mode = 'redirect'
        result = self.run_verifier()
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(self.paths, ['/web-healthz'])

    def test_oversized_response_is_rejected(self):
        self.mode = 'oversized'
        result = self.run_verifier()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('exceeds 64 KiB', result.stderr)

    def test_wrong_auth_mode_and_malformed_metadata(self):
        for mode in ['development', 'malformed']:
            with self.subTest(mode=mode):
                self.mode = mode
                result = self.run_verifier()
                self.assertNotEqual(result.returncode, 0)
                self.assertIn('FAIL production OIDC metadata', result.stderr)
                self.assertNotIn('Traceback', result.stderr)

    def test_invalid_ca_rejected_before_network(self):
        bad = self.root / 'invalid-ca.pem'
        bad.write_text('not a certificate')
        result = self.run_verifier(ca=bad)
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(self.paths, [])
        self.assertIn('FAIL trusted CA bundle', result.stderr)


if __name__ == '__main__':
    unittest.main()
