"""Small, expiring per-domain cookie store."""
from __future__ import annotations

import pickle
import time
import os
import tempfile
from pathlib import Path
from typing import Any

CACHE = Path(__file__).resolve().parent / "data" / "sessions"
MAX_AGE_SECONDS = 86400 * 7


def load_cookies(session: Any, domain: str) -> None:
    path = CACHE / f"{domain}.pkl"
    try:
        if not path.exists() or time.time() - path.stat().st_mtime >= MAX_AGE_SECONDS:
            return
        session.cookies.update(pickle.loads(path.read_bytes()))
    except Exception:
        pass


def save_cookies(session: Any, domain: str) -> None:
    temporary = None
    try:
        CACHE.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(dir=CACHE, delete=False) as stream:
            temporary = Path(stream.name)
            stream.write(pickle.dumps(dict(session.cookies)))
        os.replace(temporary, CACHE / f"{domain}.pkl")
    except Exception:
        pass
    finally:
        if temporary is not None:
            try:
                temporary.unlink(missing_ok=True)
            except OSError:
                pass
