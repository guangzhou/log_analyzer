# -*- coding: utf-8 -*-
import argparse, os
from .config import load_config
from .db import Database
from .ddl_sql import ALL_TABLE_DDL
from .pass1 import run_pass1
from .pass2 import run_pass2
from .summary_agg import merge_to_summary
def init_db(db: Database):
    for ddl in ALL_TABLE_DDL:
        db.execute_script(ddl)
def main():
    p=argparse.ArgumentParser(description='日志规则演进 与 统计管线')
    p.add_argument('--config', required=True)
    sub=p.add_subparsers(dest='cmd', required=True)
    sub.add_parser('init-db')
    s1=sub.add_parser('pass1'); s1.add_argument('--file', required=True)
    s2=sub.add_parser('pass2'); s2.add_argument('--file', required=True)
    sub.add_parser('merge-summary')
    a=p.parse_args(); cfg=load_config(a.config)
    os.makedirs(cfg['app']['unmatched_dir'], exist_ok=True)
    db=Database(cfg['app']['db_path'])
    if a.cmd=='init-db': init_db(db); print('数据库初始化完成。'); return
    if a.cmd=='pass1': run_pass1(cfg, db, a.file); print('Pass1 完成。'); return
    if a.cmd=='pass2': run_pass2(cfg, db, a.file); print('Pass2 完成。'); return
    if a.cmd=='merge-summary': merge_to_summary(db); print('SUMMARY 合并完成。'); return
if __name__=='__main__': main()
