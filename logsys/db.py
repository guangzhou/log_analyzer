# logsys/db.py
"""
Database 兼容层：
- 保留 Database 类以兼容 from .db import Database
- init_db 与 execute_script 均可接受 文件路径 或 原始 SQL 文本
- 内置 SQLite PRAGMA 调优
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
    def __init__(self, db_path: str, row_factory: bool = True) -> None:
        db_dir = os.path.dirname(os.path.abspath(db_path)) or "."
        os.makedirs(db_dir, exist_ok=True)
        self.conn = sqlite3.connect(db_path)
        if row_factory:
            self.conn.row_factory = sqlite3.Row
        open_db_and_tune(self.conn)

    def init_db(self, schema: Optional[str] = None, treat_as_sql: Optional[bool] = None) -> None:
        """
        初始化数据库结构。参数 schema 可以是：
        - None: 默认读取当前工作目录的 schema.sql 文件
        - 路径字符串: 指向 .sql 文件
        - 原始 SQL 文本: 以 'CREATE TABLE' 等关键字为特征
        参数 treat_as_sql=True 时强制按 SQL 文本执行；False 时强制按文件路径处理。
        """
        if schema is None:
            default_path = os.path.join(os.getcwd(), "schema.sql")
            self.execute_script(default_path, treat_as_sql=False)
            return
        self.execute_script(schema, treat_as_sql=treat_as_sql)

    def execute_script(self, path_or_sql: str, treat_as_sql: Optional[bool] = None) -> None:
        """
        执行脚本：支持文件路径或直接 SQL 文本。
        决策规则：
        - treat_as_sql 显式指定时遵从指定
        - 否则若存在同名文件则按路径读取
        - 否则按 SQL 文本直接执行
        """
        sql: Optional[str] = None
        if treat_as_sql is True:
            sql = path_or_sql
        elif treat_as_sql is False:
            if not os.path.isfile(path_or_sql):
                raise FileNotFoundError(f"schema file not found: {path_or_sql}")
        else:
            # 自动判定
            if os.path.isfile(path_or_sql):
                pass  # 走文件分支
            else:
                # 非文件：视为 SQL 文本
                sql = path_or_sql

        cur = self.conn.cursor()
        if sql is None:
            with open(path_or_sql, "r", encoding="utf-8") as f:
                sql = f.read()
        cur.executescript(sql)
        self.conn.commit()

    # 兼容用法
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
