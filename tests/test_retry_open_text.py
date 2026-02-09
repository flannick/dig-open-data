import gzip
import io
import unittest

from dig_open_data import open_text
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


if __name__ == "__main__":
    unittest.main()
