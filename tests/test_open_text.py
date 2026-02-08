import gzip
import io
import os
import tempfile
import unittest
import urllib.error
from unittest import mock

from dig_open_data import open_text, resolve_uri
from dig_open_data.backends import S3HttpBackend, s3_uri_to_https_url


class TestOpenTextLocal(unittest.TestCase):
    def test_local_plain_text(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "sample.tsv")
            content = "col1\tcol2\n1\t2\n3\t4\n"
            with open(path, "w", encoding="utf-8") as handle:
                handle.write(content)

            with open_text(path) as handle:
                header = handle.readline()
                rows = list(handle)

        self.assertEqual(header, "col1\tcol2\n")
        self.assertEqual(rows, ["1\t2\n", "3\t4\n"])

    def test_local_gzip_text(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "sample.tsv.gz")
            content = "col1\tcol2\n1\t2\n"
            with gzip.open(path, "wb") as handle:
                handle.write(content.encode("utf-8"))

            with open_text(path) as handle:
                data = handle.read()

        self.assertEqual(data, content)


class TestUriResolution(unittest.TestCase):
    def test_registry_resolution(self):
        uri = "registry://dig-open-bottom-line-analysis/path/file.tsv.gz"
        resolved = resolve_uri(uri)
        self.assertEqual(
            resolved,
            "s3://dig-open-bottom-line-analysis/path/file.tsv.gz",
        )

    def test_s3_url_building(self):
        uri = "s3://dig-open-bottom-line-analysis/path/file.tsv.gz"
        url = s3_uri_to_https_url(uri)
        self.assertEqual(
            url,
            "https://dig-open-bottom-line-analysis.s3.amazonaws.com/path/file.tsv.gz",
        )


class TestS3Backend(unittest.TestCase):
    def test_exists_true(self):
        backend = S3HttpBackend()

        class DummyResponse(io.BytesIO):
            status = 200

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                self.close()

        with mock.patch("urllib.request.urlopen", return_value=DummyResponse()) as mocked:
            self.assertTrue(
                backend.exists("s3://dig-open-bottom-line-analysis/path/file.tsv.gz")
            )
            self.assertTrue(mocked.called)

    def test_exists_false(self):
        backend = S3HttpBackend()
        error = urllib.error.HTTPError(
            url="https://example.com",
            code=404,
            msg="Not Found",
            hdrs=None,
            fp=io.BytesIO(),
        )
        with mock.patch("urllib.request.urlopen", side_effect=error):
            self.assertFalse(
                backend.exists("s3://dig-open-bottom-line-analysis/path/missing.tsv.gz")
            )


if __name__ == "__main__":
    unittest.main()
