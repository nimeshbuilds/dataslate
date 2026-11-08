import hashlib
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch
import io

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import download_jars


class DependencyTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='dataslate-dependency-test-')
        self.root = Path(self.temp.name)
        self.lock = self.root/'lock.json'
        self.entry = {'name': 'sample-1.jar', 'url': 'https://repo.maven.apache.org/maven2/example/sample-1.jar', 'sha256': hashlib.sha256(b'jar bytes').hexdigest()}
        self.lock.write_text(json.dumps([self.entry]))

    def tearDown(self):
        self.temp.cleanup()

    def test_offline_requires_exact_checksum(self):
        with self.assertRaisesRegex(ValueError, 'missing or changed'):
            download_jars.fetch(self.lock, self.root/'jars', True)
        (self.root/'jars'/self.entry['name']).write_bytes(b'jar bytes')
        self.assertEqual(download_jars.fetch(self.lock, self.root/'jars', True), 1)
        (self.root/'jars'/self.entry['name']).write_bytes(b'changed')
        with self.assertRaisesRegex(ValueError, 'missing or changed'):
            download_jars.fetch(self.lock, self.root/'jars', True)

    def test_download_verifies_before_replacing(self):
        with patch.object(download_jars, 'urlopen', return_value=io.BytesIO(b'jar bytes')):
            self.assertEqual(download_jars.fetch(self.lock, self.root/'jars'), 1)
        self.assertEqual((self.root/'jars'/self.entry['name']).read_bytes(), b'jar bytes')

    def test_bad_download_is_not_published(self):
        with patch.object(download_jars, 'urlopen', return_value=io.BytesIO(b'corrupt')), self.assertRaisesRegex(ValueError, 'checksum mismatch'):
            download_jars.fetch(self.lock, self.root/'jars')
        self.assertEqual(list((self.root/'jars').iterdir()), [])

    def test_lock_cannot_escape_or_change_origin(self):
        for field, value in [('name', '../outside.jar'), ('url', 'https://evil.example/sample-1.jar'), ('sha256', 'not-a-hash')]:
            invalid = dict(self.entry); invalid[field] = value; self.lock.write_text(json.dumps([invalid]))
            with self.subTest(field=field), self.assertRaisesRegex(ValueError, 'invalid dependency lock'):
                download_jars.fetch(self.lock, self.root/'jars')


if __name__ == '__main__':
    unittest.main()
