from __future__ import annotations

import json
import os
import tempfile
import time
from dataclasses import dataclass
from hashlib import sha256
from typing import Iterable


@dataclass(frozen=True)
class CacheConfig:
    dir: str
    max_bytes: int = 10 * 1024**3
    ttl_days: int | None = None

    @property
    def ttl_seconds(self) -> int | None:
        if self.ttl_days is None:
            return None
        return int(self.ttl_days * 24 * 60 * 60)


class CacheStore:
    def __init__(self, config: CacheConfig) -> None:
        self._config = config
        self._dir = os.path.abspath(config.dir)
        self._objects_dir = os.path.join(self._dir, "objects")
        self._index_path = os.path.join(self._dir, "index.jsonl")
        os.makedirs(self._objects_dir, exist_ok=True)
        self._cleanup_partials()

    def get(self, key: str) -> dict | None:
        entry = self._load_index().get(key)
        if entry is None:
            return None
        path = entry.get("path")
        if not path or not os.path.exists(path):
            self._delete_entry(key, entry)
            return None
        if self._expired(entry):
            self._delete_entry(key, entry)
            return None
        self._touch(key, entry)
        return entry

    def put(self, key: str, source_path: str, size: int, metadata: dict | None = None) -> str:
        digest = sha256(key.encode("utf-8")).hexdigest()
        dest_path = os.path.join(self._objects_dir, digest)
        os.replace(source_path, dest_path)
        now = int(time.time())
        entry = {
            "path": dest_path,
            "size": size,
            "created_at": now,
            "last_access": now,
        }
        if metadata:
            entry.update(metadata)
        index = self._load_index()
        index[key] = entry
        self._write_index(index)
        self._evict_if_needed(index)
        return dest_path

    def _touch(self, key: str, entry: dict) -> None:
        entry["last_access"] = int(time.time())
        index = self._load_index()
        index[key] = entry
        self._write_index(index)

    def _expired(self, entry: dict) -> bool:
        ttl = self._config.ttl_seconds
        if ttl is None:
            return False
        last_access = entry.get("last_access", 0)
        return (int(time.time()) - int(last_access)) > ttl

    def _evict_if_needed(self, index: dict) -> None:
        max_bytes = max(0, int(self._config.max_bytes))
        total = sum(int(v.get("size", 0)) for v in index.values())
        if total <= max_bytes:
            return
        entries = sorted(
            index.items(),
            key=lambda kv: int(kv[1].get("last_access", 0)),
        )
        for key, entry in entries:
            self._delete_entry(key, entry)
            total = sum(int(v.get("size", 0)) for v in index.values())
            if total <= max_bytes:
                break

    def _delete_entry(self, key: str, entry: dict) -> None:
        path = entry.get("path")
        if path and os.path.exists(path):
            try:
                os.remove(path)
            except OSError:
                pass
        index = self._load_index()
        if key in index:
            del index[key]
            self._write_index(index)

    def delete(self, key: str) -> None:
        entry = self._load_index().get(key)
        if entry is not None:
            self._delete_entry(key, entry)

    def _load_index(self) -> dict:
        if not os.path.exists(self._index_path):
            return {}
        index: dict = {}
        with open(self._index_path, "r", encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                record = json.loads(line)
                key = record.get("key")
                if key:
                    index[key] = record.get("entry", {})
        return index

    def _write_index(self, index: dict) -> None:
        fd, path = tempfile.mkstemp(prefix="dig-open-data-index-", suffix=".jsonl")
        os.close(fd)
        with open(path, "w", encoding="utf-8") as handle:
            for key, entry in index.items():
                handle.write(json.dumps({"key": key, "entry": entry}) + "\n")
        os.replace(path, self._index_path)

    def _cleanup_partials(self) -> None:
        try:
            for name in os.listdir(self._objects_dir):
                if name.endswith(".partial"):
                    try:
                        os.remove(os.path.join(self._objects_dir, name))
                    except OSError:
                        pass
        except OSError:
            pass

def cache_config_from_env() -> CacheConfig | None:
    cache_dir = os.environ.get("DIG_OPEN_DATA_CACHE_DIR")
    if not cache_dir:
        return None
    max_bytes = _parse_int_env("DIG_OPEN_DATA_CACHE_MAX_BYTES", 10 * 1024**3)
    ttl_days = _parse_int_env("DIG_OPEN_DATA_CACHE_TTL_DAYS", None)
    return CacheConfig(dir=cache_dir, max_bytes=max_bytes, ttl_days=ttl_days)


def _parse_int_env(name: str, default: int | None) -> int | None:
    value = os.environ.get(name)
    if value is None or value == "":
        return default
    try:
        return int(value)
    except ValueError:
        return default
