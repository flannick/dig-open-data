from __future__ import annotations

import csv
import gzip
import io
from contextlib import ExitStack
from typing import BinaryIO, Iterator, TextIO

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
