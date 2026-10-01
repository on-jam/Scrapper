"""Small, expiring per-domain cookie store."""
from __future__ import annotations

import pickle
import time
from pathlib import Path
from typing import Any

CACHE = Path(__file__).resolve().parent / "data" / "sessions"
MAX_AGE_SECONDS = 86400 * 7


def load_cookies(session: Any, domain: str) -> None:
    path = CACHE / f"{domain}.pkl"
    if not path.exists() or time.time() - path.stat().st_mtime >= MAX_AGE_SECONDS:
        return
    try:
        session.cookies.update(pickle.loads(path.read_bytes()))
    except Exception:
        pass


def save_cookies(session: Any, domain: str) -> None:
    CACHE.mkdir(parents=True, exist_ok=True)
    try:
        (CACHE / f"{domain}.pkl").write_bytes(pickle.dumps(dict(session.cookies)))
    except Exception:
        pass
