# -*- coding: utf-8 -*-
import sqlite3, threading
from contextlib import contextmanager
from typing import Iterable, List
class Database:
    def __init__(self, path: str):
        self.path = path
        self._lock = threading.Lock()
    @contextmanager
    def connect(self):
        con = sqlite3.connect(self.path)
        con.execute('PRAGMA journal_mode=WAL;')
        con.execute('PRAGMA synchronous=NORMAL;')
        con.row_factory = sqlite3.Row
        try:
            yield con
            con.commit()
        except:
            con.rollback()
            raise
        finally:
            con.close()
    def executescript(self, script: str):
        with self._lock:
            with self.connect() as con:
                con.executescript(script)
    def execute(self, sql: str, params: Iterable = ()): 
        with self._lock:
            with self.connect() as con:
                cur = con.execute(sql, tuple(params))
                return cur.lastrowid
    def executemany(self, sql: str, rows: List[Iterable]):
        with self._lock:
            with self.connect() as con:
                con.executemany(sql, rows)
    def query(self, sql: str, params: Iterable = ()): 
        with self._lock:
            with self.connect() as con:
                cur = con.execute(sql, tuple(params))
                return cur.fetchall()
