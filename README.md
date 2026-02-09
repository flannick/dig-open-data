# DIG Open Data GWAS Access Module

Small, dependency-light helpers to open GWAS inputs as **streaming text** from local disk or the public DIG Open Data S3 registry.

## Features

- Streaming text I/O (never loads full files into memory)
- Transparent gzip detection by magic number
- Pluggable storage backends (local disk + public S3 by default)
- No AWS credentials required for the DIG Open Data bucket

## Install

This module uses only the Python standard library. If you want to package it:

```bash
python -m pip install -e .
```

If your environment blocks network access during build isolation, use:

```bash
python -m pip install -e . --no-build-isolation
```

## R Package (digOpenData)

An R interface lives under `r/digOpenData/` and follows Bioconductor-style conventions.

Quick usage (from inside the R package directory):

```r
library(digOpenData)

list_ancestries()
list_traits(ancestry = "EU")

con <- open_trait("EU", "AlbInT2D")
lines <- read_lines("s3://dig-open-bottom-line-analysis/bottom-line/EU/AlbInT2D.sumstats.tsv.gz")
```

## Quick Start

```python
from dig_open_data import open_trait

with open_trait("EU", "AlbInT2D") as f:
    header = f.readline()
    for line in f:
        # parse line
        pass
```

## Public API

- `open_text(uri: str, *, encoding: str = "utf-8", retries: int = 3, download: bool = False) -> TextIO`
- `exists(uri: str) -> bool`
- `resolve_uri(uri: str) -> str`
- `register_backend(backend) -> None`
- `iter_lines(uri: str, *, encoding: str = "utf-8") -> Iterator[str]`
- `iter_tsv_dicts(uri: str, *, delimiter: str = "\t", encoding: str = "utf-8") -> Iterator[dict[str, str]]`
- `list_ancestries(*, bucket: str = DEFAULT_BUCKET, prefix: str = DEFAULT_PREFIX, max_keys: int = 1000) -> list[str]`
- `list_traits(*, bucket: str = DEFAULT_BUCKET, prefix: str = DEFAULT_PREFIX, ancestry: str | None = None, max_keys: int = 1000, limit: int | None = None, contains: str | None = None) -> list[str]`
- `list_files(*, bucket: str = DEFAULT_BUCKET, prefix: str = DEFAULT_PREFIX, max_keys: int = 1000, limit: int | None = None, ancestry: str | None = None, contains: str | None = None) -> list[str]`
- `list_dataset_files(*, bucket: str = DEFAULT_BUCKET, prefix: str = DEFAULT_PREFIX, max_keys: int = 1000, limit: int | None = None, ancestry: str | None = None, contains: str | None = None) -> list[str]`
- `list_files_with_metadata(*, bucket: str = DEFAULT_BUCKET, prefix: str = DEFAULT_PREFIX, ancestry: str | None = None, max_keys: int = 1000, limit: int | None = None, contains: str | None = None) -> list[FileEntry]`
- `build_key(ancestry: str, trait: str, *, prefix: str = DEFAULT_PREFIX, suffix: str = DEFAULT_SUFFIX) -> str`
- `open_trait(ancestry: str, trait: str, *, bucket: str = DEFAULT_BUCKET, prefix: str = DEFAULT_PREFIX, suffix: str = DEFAULT_SUFFIX, encoding: str = "utf-8") -> TextIO`
- `get_documentation(dataset_prefix: str, *, bucket: str = DEFAULT_BUCKET, recursive: bool = False, doc_filenames: Iterable[str] = DOC_FILENAMES) -> dict[str, str]`
- `list_datasets_with_docs(*, bucket: str = DEFAULT_BUCKET, prefix: str = "", recursive: bool = False, doc_filenames: Iterable[str] = DOC_FILENAMES) -> list[tuple[str, dict[str, str]]]`

## Input Formats

This module is format-agnostic. It simply returns a streaming text handle. Downstream tools are expected to parse the file (TSV, CSV, etc.).

## Dataset Discovery Utilities

You can list available files by ancestry and trait name, and retrieve documentation files stored alongside datasets.

```python
from dig_open_data import list_ancestries, list_traits, list_files_with_metadata

ancestries = list_ancestries()
traits = list_traits(ancestry="EU")
entries = list_files_with_metadata(ancestry="EU")

for entry in entries:
    print(entry.ancestry, entry.trait, entry.key)
```

Notes:
- `list_dataset_files()` uses S3 ListObjectsV2 to return full object keys.
- `get_documentation()` looks for common doc filenames (README, manifest, metadata). Set `recursive=True` to search deeper paths.

## Tests

From `analysis/dig_open_data_module`:

```bash
PYTHONPATH=src/dig_open_data/src ../.venv/bin/python -m unittest discover -s src/dig_open_data/tests
```

## CLI

You can run the CLI in two ways.

### Option 1: Install (recommended for repeated use)

Install in editable mode, then use the `dig-open-data` command:

```bash
python -m pip install -e .
```

List files under the default DIG prefix (`bottom-line/`):

```bash
dig-open-data list
dig-open-data list --limit 10
dig-open-data list --ancestry EUR --json
dig-open-data list --contains T2D
dig-open-data list --max-keys 500
```

List available ancestries:

```bash
dig-open-data ancestries
dig-open-data ancestries --json
dig-open-data ancestries --max-keys 500
```

List available trait names:

```bash
dig-open-data traits
dig-open-data traits --ancestry EU --contains T2D
dig-open-data traits --json
```

Stream a file to stdout:

```bash
dig-open-data stream --ancestry EU --trait AlbInT2D | head
dig-open-data stream --file bottom-line/EU/AlbInT2D.sumstats.tsv.gz | head
```

Include ancestry + trait metadata in listings:

```bash
dig-open-data list --with-ancestry
dig-open-data list --with-ancestry --ancestry EUR --limit 20
```

Fetch documentation for a dataset prefix:

```bash
dig-open-data docs dataset1/
dig-open-data docs dataset1/ --recursive --json
dig-open-data docs dataset1/ --names README.md manifest.json
```

### Option 2: No install (PYTHONPATH)

From the repo root:

```bash
PYTHONPATH=src ../.venv/bin/python -m dig_open_data.cli list
PYTHONPATH=src ../.venv/bin/python -m dig_open_data.cli list --with-ancestry
PYTHONPATH=src ../.venv/bin/python -m dig_open_data.cli ancestries
PYTHONPATH=src ../.venv/bin/python -m dig_open_data.cli traits --ancestry EU
PYTHONPATH=src ../.venv/bin/python -m dig_open_data.cli stream --ancestry EU --trait AlbInT2D | head
PYTHONPATH=src ../.venv/bin/python -m dig_open_data.cli docs dataset1/ --recursive
```

### Listing Best Practices

- S3 ListObjectsV2 returns up to 1,000 keys per request, so large listings are paginated; this CLI paginates automatically but you should still use `--limit` to keep outputs manageable.
- Use `--ancestry` and `--contains` to scope listings to a smaller portion of the bucket.
- Dataset and ancestry discovery uses the S3 `delimiter="/"` behavior to return `CommonPrefixes` that act like subdirectories; this is why ancestry and dataset listings return prefixes rather than raw files.

## Design Notes

- Gzip detection is based on the first two bytes (0x1f, 0x8b), not filename.
- S3 access uses unsigned HTTPS requests via `urllib.request` to avoid extra dependencies.
- Backends are intentionally minimal; add new schemes by registering a backend implementing the small protocol.
- `open_text(..., retries=N)` retries on truncated gzip streams by reopening and skipping already-read characters. Use `download=True` to stage remote files locally before reading.

## Caching (Optional)

Caching is **opt‑in**. If you do nothing, behavior is unchanged (streaming reads).

### Option 1: Explicit cache config (recommended)

```python
from dig_open_data import CacheConfig, open_trait

cache = CacheConfig(dir="/data/dig_cache", max_bytes=10 * 1024**3, ttl_days=None)
with open_trait("EU", "AlbInT2D", cache=cache) as f:
    ...
```

### Option 2: Environment variables (fallback)

Set at least `DIG_OPEN_DATA_CACHE_DIR` to enable caching. Other variables are optional.

```bash
export DIG_OPEN_DATA_CACHE_DIR=/data/dig_cache
export DIG_OPEN_DATA_CACHE_MAX_BYTES=10737418240  # 10GB
export DIG_OPEN_DATA_CACHE_TTL_DAYS=30
```

Env defaults:
- If `DIG_OPEN_DATA_CACHE_DIR` is **not** set, caching is **disabled**.
- If `DIG_OPEN_DATA_CACHE_DIR` is set:
  - `DIG_OPEN_DATA_CACHE_MAX_BYTES` defaults to 10GB if unset.
  - `DIG_OPEN_DATA_CACHE_TTL_DAYS` defaults to no TTL if unset.
  
If the environment variables are set, caching is enabled automatically even if your script doesn’t change.

### Eviction

The cache uses least‑recently‑used (LRU) eviction based on last access time. When the cache exceeds `max_bytes`, the oldest entries are removed first.
