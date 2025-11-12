# logsys/db.py
"""
提供 Database 类以兼容 logsys.main 的导入，同时保留批量 upsert 与调优工具函数。
"""

from __future__ import annotations
import os
import sqlite3
from typing import Iterable, List, Set, Tuple, Optional

__all__ = [
    "Database",
    "open_db_and_tune",
    "upsert_module_bulk",
    "upsert_smod_bulk",
    "bump_template_stats_bulk",
]

class Database:
    """
    轻量级 SQLite 包装，兼容 `from .db import Database` 的旧用法。

    常用属性与方法：
    - conn: sqlite3.Connection 实例
    - init_db(schema_path: Optional[str]): 执行建表脚本（默认寻找仓库根目录的 schema.sql）
    - execute_script(path): 同上，别名
    - close(): 关闭连接
    """
    def __init__(self, db_path: str, row_factory: bool = True) -> None:
        os.makedirs(os.path.dirname(os.path.abspath(db_path)), exist_ok=True)
        self.conn = sqlite3.connect(db_path)
        if row_factory:
            self.conn.row_factory = sqlite3.Row
        open_db_and_tune(self.conn)

    def init_db(self, schema_path: Optional[str] = None) -> None:
        if schema_path is None:
            # 默认从仓库根目录的 schema.sql 读取
            cwd = os.getcwd()
            default_path = os.path.join(cwd, "schema.sql")
            schema_path = default_path
        self.execute_script(schema_path)

    def execute_script(self, path: str) -> None:
        with open(path, "r", encoding="utf-8") as f:
            sql = f.read()
        cur = self.conn.cursor()
        cur.executescript(sql)
        self.conn.commit()

    # 兼容某些调用写法
    def cursor(self):
        return self.conn.cursor()

    def close(self) -> None:
        try:
            self.conn.close()
        except Exception:
            pass


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
