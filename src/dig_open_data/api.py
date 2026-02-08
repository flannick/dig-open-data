from __future__ import annotations

import urllib.parse
from typing import BinaryIO, Dict

from .backends import Backend, LocalBackend, S3HttpBackend
from .streams import open_text_stream

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


def open_text(uri: str, *, encoding: str = "utf-8"):
    resolved = resolve_uri(uri)
    backend = _select_backend(resolved)
    binary = backend.open_binary(resolved)
    return open_text_stream(binary, encoding)


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
