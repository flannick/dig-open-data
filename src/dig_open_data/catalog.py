from __future__ import annotations

import os
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from typing import Iterable

from .api import open_text

DEFAULT_BUCKET = "dig-open-bottom-line-analysis"
DEFAULT_PREFIX = "bottom-line/"
DEFAULT_SUFFIX = ".sumstats.tsv.gz"
DOC_FILENAMES = (
    "README",
    "README.md",
    "README.txt",
    "docs.md",
    "documentation.md",
    "manifest.json",
    "metadata.json",
)


@dataclass(frozen=True)
class ListObjectsResult:
    keys: list[str]
    common_prefixes: list[str]
    is_truncated: bool
    next_token: str | None


@dataclass(frozen=True)
class FileEntry:
    ancestry: str | None
    trait: str | None
    filename: str
    key: str


def list_objects(
    *,
    bucket: str = DEFAULT_BUCKET,
    prefix: str = "",
    delimiter: str | None = None,
    max_keys: int = 1000,
) -> ListObjectsResult:
    return _list_objects_page(
        bucket=bucket,
        prefix=prefix,
        delimiter=delimiter,
        max_keys=max_keys,
        continuation_token=None,
    )


def list_all_objects(
    *,
    bucket: str = DEFAULT_BUCKET,
    prefix: str = "",
    delimiter: str | None = None,
    max_keys: int = 1000,
) -> ListObjectsResult:
    keys: list[str] = []
    prefixes: list[str] = []
    token: str | None = None
    while True:
        page = _list_objects_page(
            bucket=bucket,
            prefix=prefix,
            delimiter=delimiter,
            max_keys=max_keys,
            continuation_token=token,
        )
        keys.extend(page.keys)
        prefixes.extend(page.common_prefixes)
        if not page.is_truncated:
            return ListObjectsResult(
                keys=keys,
                common_prefixes=prefixes,
                is_truncated=False,
                next_token=None,
            )
        token = page.next_token


def list_datasets(
    *,
    bucket: str = DEFAULT_BUCKET,
    prefix: str = "",
    max_keys: int = 1000,
    limit: int | None = None,
) -> list[str]:
    result = list_all_objects(
        bucket=bucket, prefix=prefix, delimiter="/", max_keys=max_keys
    )
    datasets = sorted(set(result.common_prefixes))
    if limit is None:
        return datasets
    return datasets[: max(0, limit)]


def list_ancestries(
    *,
    bucket: str = DEFAULT_BUCKET,
    prefix: str = DEFAULT_PREFIX,
    max_keys: int = 1000,
) -> list[str]:
    result = list_all_objects(
        bucket=bucket, prefix=_ensure_prefix(prefix), delimiter="/", max_keys=max_keys
    )
    ancestries = [p.rstrip("/").split("/")[-1] for p in result.common_prefixes]
    return sorted(set(ancestries))


def list_dataset_files(
    *,
    bucket: str = DEFAULT_BUCKET,
    prefix: str = DEFAULT_PREFIX,
    max_keys: int = 1000,
    limit: int | None = None,
    ancestry: str | None = None,
    contains: str | None = None,
) -> list[str]:
    effective_prefix = _ensure_prefix(prefix)
    if ancestry:
        effective_prefix = _join_prefix(effective_prefix, ancestry)
    result = list_all_objects(
        bucket=bucket, prefix=effective_prefix, delimiter=None, max_keys=max_keys
    )
    files = sorted(result.keys)
    if contains:
        files = [key for key in files if contains in key]
    if limit is None:
        return files
    return files[: max(0, limit)]


def list_files_with_metadata(
    *,
    bucket: str = DEFAULT_BUCKET,
    prefix: str = DEFAULT_PREFIX,
    ancestry: str | None = None,
    max_keys: int = 1000,
    limit: int | None = None,
    contains: str | None = None,
) -> list[FileEntry]:
    keys = list_dataset_files(
        bucket=bucket,
        prefix=prefix,
        max_keys=max_keys,
        limit=limit,
        ancestry=ancestry,
        contains=contains,
    )
    base_prefix = _ensure_prefix(prefix)
    entries: list[FileEntry] = []
    for key in keys:
        entry = _key_to_file_entry(
            key,
            base_prefix=base_prefix,
            default_prefix=DEFAULT_PREFIX,
            ancestry_override=ancestry,
        )
        entries.append(entry)
    entries = sorted(entries, key=lambda e: (e.ancestry or "", e.key))
    if limit is None:
        return entries
    return entries[: max(0, limit)]


def list_traits(
    *,
    bucket: str = DEFAULT_BUCKET,
    prefix: str = DEFAULT_PREFIX,
    ancestry: str | None = None,
    max_keys: int = 1000,
    limit: int | None = None,
    contains: str | None = None,
) -> list[str]:
    entries = list_files_with_metadata(
        bucket=bucket,
        prefix=prefix,
        ancestry=ancestry,
        max_keys=max_keys,
        limit=None,
        contains=contains,
    )
    traits = sorted({entry.trait for entry in entries if entry.trait})
    if limit is None:
        return traits
    return traits[: max(0, limit)]


def build_key(
    ancestry: str,
    trait: str,
    *,
    prefix: str = DEFAULT_PREFIX,
    suffix: str = DEFAULT_SUFFIX,
) -> str:
    clean_trait = trait
    if clean_trait.endswith(suffix):
        clean_trait = clean_trait[: -len(suffix)]
    filename = f"{clean_trait}{suffix}"
    return _join_prefix(_ensure_prefix(prefix), ancestry) + filename


def open_trait(
    ancestry: str,
    trait: str,
    *,
    bucket: str = DEFAULT_BUCKET,
    prefix: str = DEFAULT_PREFIX,
    suffix: str = DEFAULT_SUFFIX,
    encoding: str = "utf-8",
):
    key = build_key(ancestry, trait, prefix=prefix, suffix=suffix)
    uri = f"s3://{bucket}/{key}"
    return open_text(uri, encoding=encoding)


def get_documentation(
    dataset_prefix: str,
    *,
    bucket: str = DEFAULT_BUCKET,
    recursive: bool = False,
    doc_filenames: Iterable[str] = DOC_FILENAMES,
) -> dict[str, str]:
    delimiter = None if recursive else "/"
    keys = list_all_objects(
        bucket=bucket,
        prefix=dataset_prefix,
        delimiter=delimiter,
    ).keys
    doc_names = {name.lower() for name in doc_filenames}
    matches = [key for key in keys if os.path.basename(key).lower() in doc_names]

    docs: dict[str, str] = {}
    for key in matches:
        uri = f"s3://{bucket}/{key}"
        with open_text(uri) as handle:
            docs[key] = handle.read()
    return docs


def list_datasets_with_docs(
    *,
    bucket: str = DEFAULT_BUCKET,
    prefix: str = "",
    recursive: bool = False,
    doc_filenames: Iterable[str] = DOC_FILENAMES,
) -> list[tuple[str, dict[str, str]]]:
    datasets = list_datasets(bucket=bucket, prefix=prefix)
    results: list[tuple[str, dict[str, str]]] = []
    for dataset in datasets:
        docs = get_documentation(
            dataset,
            bucket=bucket,
            recursive=recursive,
            doc_filenames=doc_filenames,
        )
        results.append((dataset, docs))
    return results


def _list_objects_page(
    *,
    bucket: str,
    prefix: str,
    delimiter: str | None,
    max_keys: int,
    continuation_token: str | None,
) -> ListObjectsResult:
    query = {
        "list-type": "2",
        "max-keys": str(max_keys),
    }
    if prefix:
        query["prefix"] = prefix
    if delimiter:
        query["delimiter"] = delimiter
    if continuation_token:
        query["continuation-token"] = continuation_token

    url = f"https://{bucket}.s3.amazonaws.com?{urllib.parse.urlencode(query)}"
    request = urllib.request.Request(url, method="GET", headers={"User-Agent": "dig-open-data/0.1"})
    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            body = response.read()
    except urllib.error.HTTPError as exc:
        raise RuntimeError(
            f"S3 list request failed: bucket={bucket} prefix={prefix!r} status={exc.code}"
        ) from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(
            f"S3 list request failed: bucket={bucket} prefix={prefix!r}"
        ) from exc

    return _parse_list_objects(body)


def _parse_list_objects(xml_bytes: bytes) -> ListObjectsResult:
    root = ET.fromstring(xml_bytes)

    def findtext(path: str) -> str | None:
        return root.findtext(path)

    is_truncated_text = findtext(".//{*}IsTruncated")
    is_truncated = is_truncated_text == "true"
    next_token = findtext(".//{*}NextContinuationToken")

    keys: list[str] = []
    for entry in root.findall(".//{*}Contents"):
        key = entry.findtext("{*}Key")
        if key:
            keys.append(key)

    common_prefixes: list[str] = []
    for entry in root.findall(".//{*}CommonPrefixes"):
        prefix = entry.findtext("{*}Prefix")
        if prefix:
            common_prefixes.append(prefix)

    return ListObjectsResult(
        keys=keys,
        common_prefixes=common_prefixes,
        is_truncated=is_truncated,
        next_token=next_token,
    )


def _ensure_prefix(prefix: str) -> str:
    if not prefix:
        return prefix
    return prefix if prefix.endswith("/") else f"{prefix}/"


def _join_prefix(base: str, suffix: str) -> str:
    base = _ensure_prefix(base)
    suffix = suffix.strip("/")
    return f"{base}{suffix}/"


def _extract_ancestry_from_key(key: str, base_prefix: str) -> str | None:
    if not key.startswith(base_prefix):
        return None
    remainder = key[len(base_prefix) :]
    if "/" not in remainder:
        return None
    return remainder.split("/", 1)[0]


def _extract_ancestry_from_prefix(prefix: str, base_prefix: str) -> str | None:
    base = _ensure_prefix(base_prefix)
    pref = _ensure_prefix(prefix)
    if not pref.startswith(base):
        return None
    remainder = pref[len(base) :].strip("/")
    if not remainder:
        return None
    return remainder.split("/", 1)[0]


def _key_to_file_entry(
    key: str,
    *,
    base_prefix: str,
    default_prefix: str,
    ancestry_override: str | None,
) -> FileEntry:
    ancestry = None
    if ancestry_override is not None:
        ancestry = ancestry_override
    else:
        ancestry = _extract_ancestry_from_key(key, default_prefix)
        if ancestry is None:
            ancestry = _extract_ancestry_from_key(key, base_prefix)
        if ancestry is None:
            ancestry = _extract_ancestry_from_prefix(base_prefix, default_prefix)

    filename = os.path.basename(key)
    trait = None
    if filename.endswith(DEFAULT_SUFFIX):
        trait = filename[: -len(DEFAULT_SUFFIX)]
    return FileEntry(ancestry=ancestry, trait=trait, filename=filename, key=key)
