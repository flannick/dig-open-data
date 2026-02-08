from __future__ import annotations

import os
import urllib.parse
import urllib.request
from typing import BinaryIO, Protocol


class Backend(Protocol):
    schemes: set[str]

    def open_binary(self, uri: str) -> BinaryIO:
        ...

    def exists(self, uri: str) -> bool:
        ...

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

    def open_binary(self, uri: str) -> BinaryIO:
        url = s3_uri_to_https_url(uri)
        request = urllib.request.Request(url, method="GET", headers={"User-Agent": "dig-open-data/0.1"})
        try:
            return urllib.request.urlopen(request, timeout=60)
        except urllib.error.HTTPError as exc:
            if exc.code == 404:
                raise FileNotFoundError(f"S3 object not found: {uri} (resolved {url})") from exc
            raise RuntimeError(
                f"S3 request failed: {uri} (resolved {url}) status={exc.code}"
            ) from exc
        except urllib.error.URLError as exc:
            raise RuntimeError(f"S3 request failed: {uri} (resolved {url})") from exc

    def exists(self, uri: str) -> bool:
        url = s3_uri_to_https_url(uri)
        request = urllib.request.Request(url, method="HEAD", headers={"User-Agent": "dig-open-data/0.1"})
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                return response.status == 200
        except urllib.error.HTTPError as exc:
            if exc.code == 404:
                return False
            raise RuntimeError(
                f"S3 request failed: {uri} (resolved {url}) status={exc.code}"
            ) from exc
        except urllib.error.URLError as exc:
            raise RuntimeError(f"S3 request failed: {uri} (resolved {url})") from exc


def s3_uri_to_https_url(uri: str) -> str:
    parsed = urllib.parse.urlparse(uri)
    if parsed.scheme != "s3":
        raise ValueError(f"Expected s3:// URI, got: {uri}")
    bucket = parsed.netloc
    key = parsed.path.lstrip("/")
    quoted_key = urllib.parse.quote(key, safe="/")
    if quoted_key:
        return f"https://{bucket}.s3.amazonaws.com/{quoted_key}"
    return f"https://{bucket}.s3.amazonaws.com/"
