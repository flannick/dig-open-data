from __future__ import annotations

import argparse
import json
import sys

from .catalog import (
    DOC_FILENAMES,
    DEFAULT_BUCKET,
    DEFAULT_PREFIX,
    get_documentation,
    list_dataset_files,
    list_ancestries,
    list_files_with_metadata,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="dig-open-data",
        description="Utilities for listing DIG Open Data datasets and documentation.",
    )
    parser.add_argument(
        "--bucket",
        default=DEFAULT_BUCKET,
        help=f"S3 bucket to query (default: {DEFAULT_BUCKET})",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    list_parser = subparsers.add_parser(
        "list", help="List files under a prefix (optionally with ancestry)"
    )
    list_parser.add_argument(
        "--prefix",
        default=DEFAULT_PREFIX,
        help=f"Prefix to scope listing (default: {DEFAULT_PREFIX})",
    )
    list_parser.add_argument(
        "--json",
        action="store_true",
        help="Emit JSON array instead of plain text",
    )
    list_parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limit number of results returned",
    )
    list_parser.add_argument(
        "--ancestry",
        default=None,
        help="Limit dataset listing to a specific ancestry",
    )
    list_parser.add_argument(
        "--with-ancestry",
        action="store_true",
        help="Include ancestry metadata in output",
    )
    list_parser.add_argument(
        "--contains",
        default=None,
        help="Filter results to keys containing this substring",
    )
    list_parser.add_argument(
        "--max-keys",
        type=int,
        default=1000,
        help="Max keys per S3 request (pagination still applied)",
    )

    ancestry_parser = subparsers.add_parser(
        "ancestries", help="List available ancestries"
    )
    ancestry_parser.add_argument(
        "--prefix",
        default=DEFAULT_PREFIX,
        help=f"Prefix to scope listing (default: {DEFAULT_PREFIX})",
    )
    ancestry_parser.add_argument(
        "--json",
        action="store_true",
        help="Emit JSON array instead of plain text",
    )
    ancestry_parser.add_argument(
        "--max-keys",
        type=int,
        default=1000,
        help="Max keys per S3 request (pagination still applied)",
    )

    docs_parser = subparsers.add_parser("docs", help="Fetch documentation for a dataset")
    docs_parser.add_argument("dataset", help="Dataset prefix (e.g., dataset1/)")
    docs_parser.add_argument(
        "--recursive",
        action="store_true",
        help="Search recursively for documentation files",
    )
    docs_parser.add_argument(
        "--names",
        nargs="+",
        default=list(DOC_FILENAMES),
        help="Documentation filenames to match (default: common README/manifest names)",
    )
    docs_parser.add_argument(
        "--json",
        action="store_true",
        help="Emit JSON mapping of key to content",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.command == "list":
        if args.with_ancestry:
            entries = list_files_with_metadata(
                bucket=args.bucket,
                prefix=args.prefix,
                ancestry=args.ancestry,
                limit=args.limit,
                max_keys=args.max_keys,
                contains=args.contains,
            )
            if args.json:
                payload = [
                    {"ancestry": entry.ancestry, "key": entry.key}
                    for entry in entries
                ]
                print(json.dumps(payload, indent=2))
            else:
                if not entries:
                    print("No files found.", file=sys.stderr)
                    return 1
                for entry in entries:
                    ancestry = entry.ancestry if entry.ancestry is not None else "-"
                    print(f"{ancestry}\t{entry.key}")
            return 0

        results = list_dataset_files(
            bucket=args.bucket,
            prefix=args.prefix,
            limit=args.limit,
            ancestry=args.ancestry,
            max_keys=args.max_keys,
            contains=args.contains,
        )
        if args.json:
            print(json.dumps(results, indent=2))
        else:
            if not results:
                print("No files found.", file=sys.stderr)
                return 1
            for item in results:
                print(item)
        return 0

    if args.command == "ancestries":
        ancestries = list_ancestries(
            bucket=args.bucket, prefix=args.prefix, max_keys=args.max_keys
        )
        if args.json:
            print(json.dumps(ancestries, indent=2))
        else:
            if not ancestries:
                print("No ancestries found.", file=sys.stderr)
                return 1
            for ancestry in ancestries:
                print(ancestry)
        return 0

    if args.command == "docs":
        docs = get_documentation(
            args.dataset,
            bucket=args.bucket,
            recursive=args.recursive,
            doc_filenames=args.names,
        )
        if args.json:
            print(json.dumps(docs, indent=2))
            return 0

        if not docs:
            print("No documentation files found.", file=sys.stderr)
            return 1

        for key, content in docs.items():
            header = f"# {key}"
            print(header)
            print("-" * len(header))
            print(content.rstrip())
            print()
        return 0

    parser.error("Unknown command")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
