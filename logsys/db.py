# logsys/db.py

import sqlite3
from typing import Iterable, List, Set, Tuple

def open_db_and_tune(db: sqlite3.Connection) -> None:
    cur = db.cursor()
    cur.execute("PRAGMA journal_mode = WAL;")
    cur.execute("PRAGMA synchronous = NORMAL;")
    cur.execute("PRAGMA temp_store = MEMORY;")
    cur.execute("PRAGMA mmap_size = 268435456;")  # 256 MiB
    db.commit()

def upsert_module_bulk(db: sqlite3.Connection, mods: Set[str]) -> None:
    if not mods:
        return
    cur = db.cursor()
    cur.executemany(
        """
        INSERT INTO MODULE(mod, description, created_at, updated_at)
        VALUES(?, '', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        ON CONFLICT(mod) DO UPDATE SET updated_at = CURRENT_TIMESTAMP
        """,
        [(m,) for m in mods],
    )

def upsert_smod_bulk(db: sqlite3.Connection, mod_smods: Set[Tuple[str, str]]) -> None:
    if not mod_smods:
        return
    cur = db.cursor()
    cur.executemany(
        """
        INSERT INTO SUBMODULE(smod, mod, description, created_at, updated_at)
        VALUES(?, ?, '', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        ON CONFLICT(smod) DO UPDATE SET mod = excluded.mod, updated_at = CURRENT_TIMESTAMP
        """,
        [(smod, mod) for mod, smod in mod_smods],
    )

def bump_template_stats_bulk(db: sqlite3.Connection, tids: List[int]) -> None:
    if not tids:
        return
    cur = db.cursor()
    cur.executemany(
        """
        UPDATE REGEX_TEMPLATE
           SET match_count = COALESCE(match_count, 0) + 1,
               last_seen   = CURRENT_TIMESTAMP
         WHERE template_id = ?
        """,
        [(tid,) for tid in tids],
    )
