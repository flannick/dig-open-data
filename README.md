# DIG Open Data GWAS Access Module

Small, dependency-light helpers to open GWAS inputs as **streaming text** from local disk or the public DIG Open Data S3 registry. It is designed to be a drop-in replacement for ORIOLE's `_open_text` function without changing parsing logic.

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

## Quick Start

```python
from dig_open_data import open_text

with open_text("s3://dig-open-bottom-line-analysis/path/aligned_bmi.tsv.gz") as f:
    header = f.readline()
    for line in f:
        # parse line
        pass
```

Local files work the same way:

```python
from dig_open_data import open_text

with open_text("/path/to/local.tsv.gz") as f:
    for line in f:
        ...
```

## Public API

- `open_text(uri: str, *, encoding: str = "utf-8") -> TextIO`
- `exists(uri: str) -> bool`
- `resolve_uri(uri: str) -> str`
- `register_backend(backend) -> None`
- `iter_lines(uri: str, *, encoding: str = "utf-8") -> Iterator[str]`
- `iter_tsv_dicts(uri: str, *, delimiter: str = "\t", encoding: str = "utf-8") -> Iterator[dict[str, str]]`
- `list_objects(*, bucket: str = DEFAULT_BUCKET, prefix: str = "", delimiter: str | None = None, max_keys: int = 1000) -> ListObjectsResult`
- `list_all_objects(*, bucket: str = DEFAULT_BUCKET, prefix: str = "", delimiter: str | None = None, max_keys: int = 1000) -> ListObjectsResult`
- `list_datasets(*, bucket: str = DEFAULT_BUCKET, prefix: str = "", max_keys: int = 1000, limit: int | None = None) -> list[str]`
- `list_ancestries(*, bucket: str = DEFAULT_BUCKET, prefix: str = DEFAULT_PREFIX, max_keys: int = 1000) -> list[str]`
- `list_datasets_with_ancestry(*, bucket: str = DEFAULT_BUCKET, prefix: str = DEFAULT_PREFIX, ancestry: str | None = None, max_keys: int = 1000, limit: int | None = None) -> list[DatasetEntry]`
- `list_dataset_files(*, bucket: str = DEFAULT_BUCKET, prefix: str = DEFAULT_PREFIX, max_keys: int = 1000, limit: int | None = None) -> list[str]`
- `get_documentation(dataset_prefix: str, *, bucket: str = DEFAULT_BUCKET, recursive: bool = False, doc_filenames: Iterable[str] = DOC_FILENAMES) -> dict[str, str]`
- `list_datasets_with_docs(*, bucket: str = DEFAULT_BUCKET, prefix: str = "", recursive: bool = False, doc_filenames: Iterable[str] = DOC_FILENAMES) -> list[tuple[str, dict[str, str]]]`

## URI Formats

- Local path (no scheme): `/path/to/file.tsv.gz`
- File URI: `file:///path/to/file.tsv.gz`
- Direct S3: `s3://dig-open-bottom-line-analysis/path/file.tsv.gz`
- Registry alias: `registry://dig-open-bottom-line-analysis/path/file.tsv.gz`

`registry://` resolves to `s3://` and is provided for convenience.

## Input Formats

This module is format-agnostic. It simply returns a streaming text handle. Downstream tools (like ORIOLE) are expected to parse the file (TSV, CSV, etc.).

## Dataset Discovery Utilities

You can list available datasets in the DIG Open Data public bucket and retrieve documentation files stored alongside datasets.

```python
from dig_open_data import list_datasets, get_documentation

datasets = list_datasets()
for dataset in datasets:
    docs = get_documentation(dataset)
    if docs:
        print(dataset, list(docs.keys()))
```

Notes:
- `list_datasets()` uses S3 ListObjectsV2 with `delimiter="/"` to return top-level prefixes.
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
dig-open-data list --prefix path/to/subset/ --json
dig-open-data list --ancestry EUR --json
dig-open-data list --max-keys 500
```

List available ancestries:

```bash
dig-open-data ancestries
dig-open-data ancestries --json
dig-open-data ancestries --max-keys 500
```

Include ancestry metadata in listings:

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
PYTHONPATH=src ../.venv/bin/python -m dig_open_data.cli docs dataset1/ --recursive
```

### Listing Best Practices

- S3 ListObjectsV2 returns up to 1,000 keys per request, so large listings are paginated; this CLI paginates automatically but you should still use `--limit` to keep outputs manageable.
- Use `--prefix` (or `--ancestry`) to scope listings to a smaller portion of the bucket.
- Dataset and ancestry discovery uses the S3 `delimiter="/"` behavior to return `CommonPrefixes` that act like subdirectories; this is why ancestry and dataset listings return prefixes rather than raw files.

## Design Notes

- Gzip detection is based on the first two bytes (0x1f, 0x8b), not filename.
- S3 access uses unsigned HTTPS requests via `urllib.request` to avoid extra dependencies.
- Backends are intentionally minimal; add new schemes by registering a backend implementing the small protocol.
