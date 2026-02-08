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

## URI Formats

- Local path (no scheme): `/path/to/file.tsv.gz`
- File URI: `file:///path/to/file.tsv.gz`
- Direct S3: `s3://dig-open-bottom-line-analysis/path/file.tsv.gz`
- Registry alias: `registry://dig-open-bottom-line-analysis/path/file.tsv.gz`

`registry://` resolves to `s3://` and is provided for convenience.

## Input Formats

This module is format-agnostic. It simply returns a streaming text handle. Downstream tools (like ORIOLE) are expected to parse the file (TSV, CSV, etc.).

## Tests

From `analysis/dig_open_data_module`:

```bash
PYTHONPATH=src/dig_open_data/src ../.venv/bin/python -m unittest discover -s src/dig_open_data/tests
```

## CLI

This repository does not include a CLI. If you want one, add a thin wrapper that calls `open_text` and streams to stdout.

## Design Notes

- Gzip detection is based on the first two bytes (0x1f, 0x8b), not filename.
- S3 access uses unsigned HTTPS requests via `urllib.request` to avoid extra dependencies.
- Backends are intentionally minimal; add new schemes by registering a backend implementing the small protocol.
