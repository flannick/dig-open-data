from __future__ import annotations

import urllib.parse
from typing import BinaryIO, Dict

from .backends import Backend, LocalBackend, S3HttpBackend
import os
import tempfile

from .streams import open_text_stream, open_text_stream_with_retries

_BACKENDS: Dict[str, Backend] = {}


def register_backend(backend: Backend) -> None:
    for scheme in backend.schemes:
        _BACKENDS[scheme] = backend


def resolve_uri(uri: str) -> str:
    parsed = urllib.parse.urlparse(uri)
    scheme = parsed.scheme
    if scheme == "registry":
        return f"s3://{parsed.netloc}{parsed.path}"
    if scheme == "file":
        return parsed.path
    return uri


def open_text(
    uri: str,
    *,
    encoding: str = "utf-8",
    retries: int = 3,
    download: bool = False,
):
    resolved = resolve_uri(uri)
    backend = _select_backend(resolved)

    if download and _is_remote_uri(resolved):
        return _open_text_downloaded(
            backend,
            resolved,
            encoding=encoding,
            retries=retries,
        )

    def opener():
        binary = backend.open_binary(resolved)
        return open_text_stream(binary, encoding)

    if retries <= 0:
        return opener()

    return open_text_stream_with_retries(opener, retries=retries)


def exists(uri: str) -> bool:
    resolved = resolve_uri(uri)
    backend = _select_backend(resolved)
    return backend.exists(resolved)


def _select_backend(uri: str) -> Backend:
    parsed = urllib.parse.urlparse(uri)
    scheme = parsed.scheme
    if scheme == "":
        scheme = ""
    backend = _BACKENDS.get(scheme)
    if backend is None:
        raise ValueError(f"No backend registered for scheme '{scheme}' in URI: {uri}")
    return backend


register_backend(LocalBackend())
register_backend(S3HttpBackend())


def _open_text_downloaded(
    backend: Backend,
    uri: str,
    *,
    encoding: str,
    retries: int,
):
    tmp_path = _download_with_retries(backend, uri, retries=retries)
    handle = open_text_stream(open(tmp_path, "rb"), encoding)
    return _CleanupTextIO(handle, tmp_path)


def _download_with_retries(backend: Backend, uri: str, *, retries: int) -> str:
    attempts = max(0, retries)
    last_error: Exception | None = None
    for _ in range(attempts + 1):
        try:
            with backend.open_binary(uri) as response:
                content_length = _get_content_length(response)
                fd, path = tempfile.mkstemp(prefix="dig-open-data-", suffix=".tmp")
                os.close(fd)
                bytes_read = 0
                with open(path, "wb") as out:
                    while True:
                        chunk = response.read(1024 * 1024)
                        if not chunk:
                            break
                        out.write(chunk)
                        bytes_read += len(chunk)
                if content_length is not None and bytes_read < content_length:
                    os.remove(path)
                    raise OSError(
                        f"Downloaded {bytes_read} bytes, expected {content_length}"
                    )
                return path
        except Exception as exc:
            last_error = exc
            continue
    if last_error:
        raise last_error
    raise RuntimeError("Failed to download resource")


def _get_content_length(response) -> int | None:
    length = getattr(response, "length", None)
    if isinstance(length, int) and length >= 0:
        return length
    header = getattr(response, "headers", None)
    if header is not None:
        value = header.get("Content-Length")
        if value:
            try:
                return int(value)
            except ValueError:
                return None
    return None


def _is_remote_uri(uri: str) -> bool:
    parsed = urllib.parse.urlparse(uri)
    return parsed.scheme not in ("", "file")


class _CleanupTextIO:
    def __init__(self, inner, path: str) -> None:
        self._inner = inner
        self._path = path

    def close(self) -> None:
        try:
            self._inner.close()
        finally:
            try:
                os.remove(self._path)
            except OSError:
                pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()

    def __getattr__(self, name):
        return getattr(self._inner, name)
