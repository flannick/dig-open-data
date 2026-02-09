from __future__ import annotations

import os
import os
import time
import urllib.parse
import urllib.request
from typing import BinaryIO, Protocol


class Backend(Protocol):
    schemes: set[str]

    def open_binary(self, uri: str) -> BinaryIO:
        ...

    def exists(self, uri: str) -> bool:
        ...

    def head_metadata(self, uri: str) -> dict:
        return {}

    def resolve_uri(self, uri: str) -> str:
        return uri


class LocalBackend:
    schemes = {"", "file"}

    def open_binary(self, uri: str) -> BinaryIO:
        path = self._uri_to_path(uri)
        return open(path, "rb")

    def exists(self, uri: str) -> bool:
        path = self._uri_to_path(uri)
        return os.path.exists(path)

    def resolve_uri(self, uri: str) -> str:
        return self._uri_to_path(uri)

    def head_metadata(self, uri: str) -> dict:
        path = self._uri_to_path(uri)
        if not os.path.exists(path):
            return {}
        stat = os.stat(path)
        return {
            "content_length": stat.st_size,
            "last_modified": str(int(stat.st_mtime)),
        }

    @staticmethod
    def _uri_to_path(uri: str) -> str:
        parsed = urllib.parse.urlparse(uri)
        if parsed.scheme in ("", "file"):
            path = parsed.path if parsed.scheme == "file" else uri
            path = urllib.parse.unquote(path)
            return os.path.expanduser(path)
        return os.path.expanduser(uri)


class S3HttpBackend:
    schemes = {"s3"}

    def __init__(self, *, retries: int = 2, backoff: float = 0.5) -> None:
        self._retries = max(0, retries)
        self._backoff = max(0.0, backoff)

    def open_binary(self, uri: str) -> BinaryIO:
        urls = s3_uri_to_https_urls(uri)
        last_error: Exception | None = None
        for url in urls:
            for attempt in range(self._retries + 1):
                request = urllib.request.Request(
                    url,
                    method="GET",
                    headers={
                        "User-Agent": "dig-open-data/0.1",
                        "Accept-Encoding": "identity",
                    },
                )
                try:
                    return urllib.request.urlopen(request, timeout=60)
                except urllib.error.HTTPError as exc:
                    if exc.code == 404:
                        raise FileNotFoundError(
                            f"S3 object not found: {uri} (resolved {url})"
                        ) from exc
                    last_error = exc
                    _maybe_debug(uri, url, exc)
                    if attempt < self._retries and exc.code in {429, 500, 502, 503, 504}:
                        time.sleep(self._backoff * (2**attempt))
                        continue
                    break
                except urllib.error.URLError as exc:
                    last_error = exc
                    _maybe_debug(uri, url, exc)
                    if attempt < self._retries:
                        time.sleep(self._backoff * (2**attempt))
                        continue
                    break
        if isinstance(last_error, urllib.error.HTTPError):
            raise RuntimeError(
                f"S3 request failed: {uri} status={last_error.code} reason={last_error.reason}"
            ) from last_error
        raise RuntimeError(f"S3 request failed: {uri}") from last_error

    def exists(self, uri: str) -> bool:
        urls = s3_uri_to_https_urls(uri)
        last_error: Exception | None = None
        for url in urls:
            request = urllib.request.Request(
                url,
                method="HEAD",
                headers={
                    "User-Agent": "dig-open-data/0.1",
                    "Accept-Encoding": "identity",
                },
            )
            try:
                with urllib.request.urlopen(request, timeout=30) as response:
                    return response.status == 200
            except urllib.error.HTTPError as exc:
                if exc.code == 404:
                    return False
                last_error = exc
                _maybe_debug(uri, url, exc)
                continue
            except urllib.error.URLError as exc:
                last_error = exc
                _maybe_debug(uri, url, exc)
                continue
        if isinstance(last_error, urllib.error.HTTPError):
            raise RuntimeError(
                f"S3 request failed: {uri} status={last_error.code} reason={last_error.reason}"
            ) from last_error
        raise RuntimeError(f"S3 request failed: {uri}") from last_error

    def head_metadata(self, uri: str) -> dict:
        urls = s3_uri_to_https_urls(uri)
        last_error: Exception | None = None
        for url in urls:
            request = urllib.request.Request(
                url,
                method="HEAD",
                headers={
                    "User-Agent": "dig-open-data/0.1",
                    "Accept-Encoding": "identity",
                },
            )
            try:
                with urllib.request.urlopen(request, timeout=30) as response:
                    return _headers_to_metadata(response)
            except urllib.error.HTTPError as exc:
                last_error = exc
                continue
            except urllib.error.URLError as exc:
                last_error = exc
                continue
        if last_error:
            raise last_error
        return {}


def s3_uri_to_https_url(uri: str) -> str:
    return s3_uri_to_https_urls(uri)[0]


def s3_uri_to_https_urls(uri: str) -> list[str]:
    parsed = urllib.parse.urlparse(uri)
    if parsed.scheme != "s3":
        raise ValueError(f"Expected s3:// URI, got: {uri}")
    bucket = parsed.netloc
    key = parsed.path.lstrip("/")
    quoted_key = urllib.parse.quote(key, safe="/")
    if quoted_key:
        return [
            f"https://{bucket}.s3.amazonaws.com/{quoted_key}",
            f"https://s3.amazonaws.com/{bucket}/{quoted_key}",
            f"https://{bucket}.s3.us-east-1.amazonaws.com/{quoted_key}",
            f"https://s3.us-east-1.amazonaws.com/{bucket}/{quoted_key}",
        ]
    return [
        f"https://{bucket}.s3.amazonaws.com/",
        f"https://s3.amazonaws.com/{bucket}/",
        f"https://{bucket}.s3.us-east-1.amazonaws.com/",
        f"https://s3.us-east-1.amazonaws.com/{bucket}/",
    ]


def _headers_to_metadata(response) -> dict:
    headers = getattr(response, "headers", None)
    if headers is None:
        return {}
    etag = headers.get("ETag") or headers.get("Etag")
    last_modified = headers.get("Last-Modified")
    length = headers.get("Content-Length")
    metadata = {}
    if etag:
        metadata["etag"] = etag.strip("\"")
    if last_modified:
        metadata["last_modified"] = last_modified
    if length:
        try:
            metadata["content_length"] = int(length)
        except ValueError:
            pass
    return metadata


def _maybe_debug(uri: str, url: str, exc: Exception) -> None:
    if os.environ.get("DIG_OPEN_DATA_S3_DEBUG") != "1":
        return
    try:
        print(f"[dig-open-data] S3 error for {uri} -> {url}: {exc}", file=os.sys.stderr)
    except Exception:
        pass
