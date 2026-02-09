import unittest

from dig_open_data.streams import open_text_stream_with_retries


class FlakyTextStream:
    def __init__(self, data: str, fail_after: int | None = None) -> None:
        self._data = data
        self._pos = 0
        self._fail_after = fail_after
        self._failed = False
        self.closed = False

    def read(self, size: int | None = None) -> str:
        if self._fail_after is not None and not self._failed and self._pos >= self._fail_after:
            self._failed = True
            raise EOFError("Simulated truncated stream")
        if size is None or size < 0:
            chunk = self._data[self._pos :]
            self._pos = len(self._data)
            return chunk
        chunk = self._data[self._pos : self._pos + size]
        self._pos += len(chunk)
        return chunk

    def readline(self, *args, **kwargs) -> str:
        return self.read(*args, **kwargs)

    def readable(self) -> bool:
        return True

    def close(self) -> None:
        self.closed = True


class TestRetryingStream(unittest.TestCase):
    def test_retry_replays_from_offset(self):
        data = "abcdef"
        attempts = {"count": 0}

        def opener():
            attempts["count"] += 1
            if attempts["count"] == 1:
                return FlakyTextStream(data, fail_after=3)
            return FlakyTextStream(data, fail_after=None)

        stream = open_text_stream_with_retries(opener, retries=2)
        first = stream.read(3)
        rest = stream.read()
        self.assertEqual(first + rest, data)
        self.assertGreaterEqual(attempts["count"], 2)


if __name__ == "__main__":
    unittest.main()
