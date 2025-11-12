# -*- coding: utf-8 -*-
from .db import Database
def top_keys(db: Database, run_id:int, limit=20):
    sql='''\nSELECT template_id, mod, smod, classification, level, thread_id, line_count FROM LOG_MATCH_SUMMARY WHERE run_id=? ORDER BY line_count DESC LIMIT ?\n'''
    return db.query(sql, (run_id, limit))

