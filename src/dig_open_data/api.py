from __future__ import annotations

import urllib.parse
from typing import BinaryIO, Dict

from .backends import Backend, LocalBackend, S3HttpBackend
from .cache import CacheConfig, CacheStore, cache_config_from_env
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
    cache: CacheConfig | None = None,
    cache_refresh: bool = False,
):
    resolved = resolve_uri(uri)
    backend = _select_backend(resolved)

    cache_config = cache or cache_config_from_env()
    if _cache_force_env():
        cache_refresh = True

    if cache_config is not None and _is_remote_uri(resolved):
        return _open_text_cached(
            backend,
            resolved,
            encoding=encoding,
            retries=retries,
            cache_config=cache_config,
            cache_refresh=cache_refresh,
        )

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


def _open_text_cached(
    backend: Backend,
    uri: str,
    *,
    encoding: str,
    retries: int,
    cache_config: CacheConfig,
    cache_refresh: bool,
):
    cache_store = CacheStore(cache_config)
    cached_entry = cache_store.get(uri)
    if cached_entry is not None and not cache_refresh:
        if _cache_entry_valid(backend, uri, cached_entry):
            handle = open_text_stream(open(cached_entry["path"], "rb"), encoding)
            return handle
        cache_store.delete(uri)

    tmp_path, size, metadata = _download_to_temp(
        backend, uri, retries=retries, cache_dir=cache_store._objects_dir
    )
    cached_path = cache_store.put(uri, tmp_path, size, metadata=metadata)
    handle = open_text_stream(open(cached_path, "rb"), encoding)
    return handle


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


def _download_to_temp(
    backend: Backend,
    uri: str,
    *,
    retries: int,
    cache_dir: str | None = None,
) -> tuple[str, int, dict]:
    attempts = max(0, retries)
    last_error: Exception | None = None
    for _ in range(attempts + 1):
        try:
            with backend.open_binary(uri) as response:
                content_length = _get_content_length(response)
                if cache_dir:
                    fd, path = tempfile.mkstemp(
                        prefix="dig-open-data-", suffix=".partial", dir=cache_dir
                    )
                else:
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
                metadata = _get_response_metadata(response)
                if content_length is not None:
                    metadata["content_length"] = content_length
                return path, bytes_read, metadata
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


def _get_response_metadata(response) -> dict:
    headers = getattr(response, "headers", None)
    if headers is None:
        return {}
    etag = headers.get("ETag") or headers.get("Etag")
    last_modified = headers.get("Last-Modified")
    metadata = {}
    if etag:
        metadata["etag"] = etag.strip("\"")
    if last_modified:
        metadata["last_modified"] = last_modified
    return metadata


def _cache_entry_valid(backend: Backend, uri: str, entry: dict) -> bool:
    meta = _remote_metadata(backend, uri)
    if not meta:
        return True
    etag = entry.get("etag")
    if etag and meta.get("etag") and etag != meta.get("etag"):
        return False
    last_modified = entry.get("last_modified")
    if last_modified and meta.get("last_modified") and last_modified != meta.get("last_modified"):
        return False
    content_length = entry.get("content_length")
    if content_length and meta.get("content_length") and content_length != meta.get("content_length"):
        return False
    return True


def _remote_metadata(backend: Backend, uri: str) -> dict:
    if hasattr(backend, "head_metadata"):
        try:
            return backend.head_metadata(uri)
        except Exception:
            return {}
    return {}


def _cache_force_env() -> bool:
    value = os.environ.get("DIG_OPEN_DATA_CACHE_FORCE", "")
    return value.lower() in {"1", "true", "yes"}


def _is_remote_uri(uri: str) -> bool:
    parsed = urllib.parse.urlparse(uri)
    return parsed.scheme not in ("", "file")


class _CleanupTextIO:
    def __init__(self, inner, path: str | None) -> None:
        self._inner = inner
        self._path = path

    def close(self) -> None:
        try:
            self._inner.close()
        finally:
            if self._path:
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
