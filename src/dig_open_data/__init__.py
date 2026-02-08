from .api import exists, open_text, register_backend, resolve_uri
from .streams import iter_lines, iter_tsv_dicts

__all__ = [
    "open_text",
    "exists",
    "resolve_uri",
    "register_backend",
    "iter_lines",
    "iter_tsv_dicts",
]
