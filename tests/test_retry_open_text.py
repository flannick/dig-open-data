import gzip
import io
import tempfile
import unittest
from unittest import mock

from dig_open_data import CacheConfig, open_text
from dig_open_data.api import register_backend


class FakeBackend:
    schemes = {"fake"}

    def __init__(self, payloads: list[bytes]) -> None:
        self._payloads = payloads
        self.calls = 0

    def open_binary(self, uri: str) -> io.BytesIO:
        self.calls += 1
        index = min(self.calls - 1, len(self._payloads) - 1)
        return io.BytesIO(self._payloads[index])

    def exists(self, uri: str) -> bool:
        return True


class TestOpenTextRetries(unittest.TestCase):
    def test_retry_on_truncated_gzip(self):
        lines = [
            "col1\tcol2\n",
            "1\t2\n",
            "3\t4\n",
            "5\t6\n",
            "7\t8\n",
        ]
        content = "".join(lines).encode("utf-8")
        full_payload = gzip.compress(content)
        truncated_payload = full_payload[:-10]

        backend = FakeBackend([truncated_payload, full_payload])
        register_backend(backend)

        with open_text("fake://object", retries=2) as handle:
            read_lines = list(handle)

        self.assertEqual(read_lines, lines)
        self.assertEqual(backend.calls, 2)

    def test_retry_exhausted(self):
        content = "col1\tcol2\n1\t2\n".encode("utf-8")
        full_payload = gzip.compress(content)
        truncated_payload = full_payload[:-10]

        backend = FakeBackend([truncated_payload, truncated_payload])
        register_backend(backend)

        with self.assertRaises((gzip.BadGzipFile, EOFError, OSError)):
            with open_text("fake://object", retries=1) as handle:
                _ = handle.read()


class TestCacheConfigEnv(unittest.TestCase):
    def test_cache_env_fallback(self):
        content = "col1\tcol2\n1\t2\n".encode("utf-8")
        payload = gzip.compress(content)
        backend = FakeBackend([payload])
        register_backend(backend)

        with tempfile.TemporaryDirectory() as tmpdir:
            with mock.patch.dict(
                "os.environ",
                {
                    "DIG_OPEN_DATA_CACHE_DIR": tmpdir,
                    "DIG_OPEN_DATA_CACHE_MAX_BYTES": "1048576",
                },
                clear=False,
            ):
                with open_text("fake://object", retries=0) as handle:
                    data = handle.read()
        self.assertIn("col1", data)


class TestCacheRefresh(unittest.TestCase):
    def test_cache_refresh_forces_redownload(self):
        content1 = gzip.compress(b"a\n")
        content2 = gzip.compress(b"b\n")

        class MetaBackend(FakeBackend):
            def __init__(self, payloads, etags):
                super().__init__(payloads)
                self._etags = etags
                self._head_calls = 0

            def head_metadata(self, uri: str) -> dict:
                self._head_calls += 1
                index = min(self.calls, len(self._etags) - 1)
                return {"etag": self._etags[index], "last_modified": f"t{index}"}

        backend = MetaBackend([content1, content2], ["etag1", "etag2"])
        register_backend(backend)

        with tempfile.TemporaryDirectory() as tmpdir:
            cache = CacheConfig(dir=tmpdir, max_bytes=1024 * 1024)
            with open_text("fake://object", cache=cache, retries=0) as handle:
                first = handle.read()
            with open_text(
                "fake://object", cache=cache, retries=0, cache_refresh=True
            ) as handle:
                second = handle.read()

        self.assertNotEqual(first, second)


if __name__ == "__main__":
    unittest.main()
