from __future__ import annotations

import csv
import gzip
import io
from contextlib import ExitStack
from typing import BinaryIO, Callable, Iterator, TextIO

GZIP_MAGIC = b"\x1f\x8b"


class ManagedTextIO:
    def __init__(self, text_stream: TextIO, stack: ExitStack) -> None:
        self._text_stream = text_stream
        self._stack = stack

    def close(self) -> None:
        try:
            self._text_stream.close()
        finally:
            self._stack.close()

    def __enter__(self) -> "ManagedTextIO":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def __iter__(self):
        return iter(self._text_stream)

    def readline(self, *args, **kwargs):
        return self._text_stream.readline(*args, **kwargs)

    def read(self, *args, **kwargs):
        return self._text_stream.read(*args, **kwargs)

    def readable(self):
        return self._text_stream.readable()

    @property
    def closed(self) -> bool:
        return self._text_stream.closed

    def __getattr__(self, name):
        return getattr(self._text_stream, name)


def open_text_stream(binary_stream: BinaryIO, encoding: str) -> ManagedTextIO:
    stack = ExitStack()
    stack.callback(_safe_close, binary_stream)

    buffered = io.BufferedReader(binary_stream)
    stack.callback(_safe_close, buffered)

    peek = buffered.peek(2)[:2]
    if peek == GZIP_MAGIC:
        decompressor = gzip.GzipFile(fileobj=buffered)
        stack.callback(_safe_close, decompressor)
        text = io.TextIOWrapper(decompressor, encoding=encoding)
    else:
        text = io.TextIOWrapper(buffered, encoding=encoding)

    return ManagedTextIO(text, stack)


class RetryingTextIO:
    def __init__(
        self,
        opener: Callable[[], ManagedTextIO],
        retries: int,
    ) -> None:
        self._opener = opener
        self._remaining = max(0, retries)
        self._stream = self._opener()
        self._chars_read = 0

    def close(self) -> None:
        self._stream.close()

    def __enter__(self) -> "RetryingTextIO":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def __iter__(self):
        while True:
            line = self.readline()
            if line == "":
                return
            yield line

    def readline(self, *args, **kwargs):
        while True:
            try:
                line = self._stream.readline(*args, **kwargs)
                self._chars_read += len(line)
                return line
            except _RETRY_ERRORS:
                if not self._retry():
                    raise

    def read(self, *args, **kwargs):
        while True:
            try:
                data = self._stream.read(*args, **kwargs)
                self._chars_read += len(data)
                return data
            except _RETRY_ERRORS:
                if not self._retry():
                    raise

    def readable(self):
        return self._stream.readable()

    @property
    def closed(self) -> bool:
        return self._stream.closed

    def __getattr__(self, name):
        return getattr(self._stream, name)

    def _retry(self) -> bool:
        if self._remaining <= 0:
            return False
        self._remaining -= 1
        self._stream.close()
        self._stream = self._opener()
        self._skip_chars(self._chars_read)
        return True

    def _skip_chars(self, count: int) -> None:
        remaining = count
        while remaining > 0:
            chunk = self._stream.read(min(8192, remaining))
            if chunk == "":
                raise EOFError("Stream ended before retry offset could be reached")
            remaining -= len(chunk)


def open_text_stream_with_retries(
    opener: Callable[[], ManagedTextIO],
    retries: int,
) -> RetryingTextIO:
    return RetryingTextIO(opener, retries=retries)


def iter_lines(uri: str, *, encoding: str = "utf-8") -> Iterator[str]:
    from .api import open_text

    with open_text(uri, encoding=encoding) as handle:
        for line in handle:
            yield line


def iter_tsv_dicts(
    uri: str, *, delimiter: str = "\t", encoding: str = "utf-8"
) -> Iterator[dict[str, str]]:
    from .api import open_text

    with open_text(uri, encoding=encoding) as handle:
        reader = csv.DictReader(handle, delimiter=delimiter)
        for row in reader:
            yield row


def _safe_close(obj) -> None:
    try:
        obj.close()
    except Exception:
        pass


_RETRY_ERRORS = (gzip.BadGzipFile, EOFError, OSError)
