# -*- coding: utf-8 -*-
from typing import List, Tuple
from datetime import datetime
import json
from .ingest import read_gz_lines
from .preprocess import normalize_lines
from .parser import parse_line
from .key_extract import extract_key_text, normalize_key_text
from .dedup import sort_and_dedup
from .matcher import TemplateMatcher
from .buffer_mgr import BufferManager
from .llm_adapter import call_llm
from .template_mgr import TemplateManager
from .db import Database
def _start(db: Database, file_id:int, cfg_json:str) -> int:
    now=datetime.utcnow().isoformat()
    return db.execute('INSERT INTO RUN_SESSION(file_id, pass_type, config_json, started_at, status) VALUES(?,?,?,?,?)', (file_id,'PASS1',cfg_json,now,'运行中'))
def _end(db: Database, run_id:int, totals:dict):
    now=datetime.utcnow().isoformat()
    db.execute('UPDATE RUN_SESSION SET ended_at=?, total_lines=?, preprocessed_lines=?, matched_lines=?, unmatched_lines=?, status=? WHERE run_id=?', (now, totals.get('total',0), totals.get('pre',0), totals.get('matched',0), totals.get('unmatched',0), '成功', run_id))
def upsert_module(db: Database, mod:str):
    if not mod: return
    now=datetime.utcnow().isoformat()
    db.execute('INSERT OR IGNORE INTO MODULE(mod, description, created_at, updated_at) VALUES(?,?,?,?)', (mod, None, now, now))
def upsert_smod(db: Database, mod:str, smod:str):
    if not smod: return
    now=datetime.utcnow().isoformat()
    if mod: upsert_module(db, mod)
    db.execute('INSERT OR IGNORE INTO SUBMODULE(smod, mod, description, created_at, updated_at) VALUES(?,?,?,?,?)', (smod, mod, None, now, now))
def run_pass1(cfg:dict, db:Database, gz_path:str):
    file_id = db.execute('INSERT INTO FILE_REGISTRY(path, status, ingested_at) VALUES(?,?,?)', (gz_path, '新', datetime.utcnow().isoformat()))
    run_id = _start(db, file_id, json.dumps(cfg, ensure_ascii=False))
    totals={'total':0,'pre':0,'matched':0,'unmatched':0}
    parsed_batch: List[Tuple[str,str,dict]] = []
    for line in normalize_lines(read_gz_lines(gz_path)):
        totals['total']+=1
        parsed = parse_line(line)
        if not parsed: continue
        totals['pre']+=1
        upsert_module(db, parsed.get('mod'))
        upsert_smod(db, parsed.get('mod'), parsed.get('smod'))
        key_text = extract_key_text(parsed['raw'])
        norm = normalize_key_text(key_text)
        parsed_batch.append((norm, key_text, parsed))
    pairs = sort_and_dedup([(t[0],t[1]) for t in parsed_batch])
    matcher=TemplateMatcher(db); matcher.load_templates()
    buf=BufferManager(cfg, db); tpl=TemplateManager(db)
    for norm, raw_key in pairs:
        parsed = next((p[2] for p in parsed_batch if p[1]==raw_key), None)
        if not parsed: continue
        hit = matcher.match_text(raw_key)
        if hit:
            totals['matched']+=1
            ts = parsed.get('timestamp') or datetime.utcnow().isoformat()
            db.execute('UPDATE REGEX_TEMPLATE SET match_count=match_count+1, last_seen=? WHERE template_id=?', (datetime.utcnow().isoformat(), hit['template_id']))
            tpl.observe(hit['template_id'], parsed.get('mod') or '', parsed.get('smod') or '', ts)
        else:
            totals['unmatched']+=1
            bid = buf.append_unmatched(run_id, parsed, raw_key, parsed['raw'])
            db.execute('INSERT INTO UNMATCHED_LOG(run_id,mod,smod,level,thread_id,timestamp,key_text,raw_log,buffered,buffer_id,reason) VALUES(?,?,?,?,?,?,?,?,?,?,?)', (run_id, parsed.get('mod'), parsed.get('smod'), parsed.get('level'), parsed.get('thread_id'), parsed.get('timestamp'), raw_key, parsed['raw'], 1, bid, 'no_match'))
    if buf.ready():
        items = buf.drain()
        if items:
            task_id = db.execute('INSERT INTO LLM_TASK(buffer_id, model, prompt_version, started_at, status, input_count) VALUES(?,?,?,?,?,?)', (items[0]['buffer_id'], cfg['llm']['model'], cfg['llm']['prompt_version'], datetime.utcnow().isoformat(), '运行中', len(items)))
            sample_texts = [s['key_text'] for s in items]
            llm_items = call_llm(cfg, sample_texts)
            if llm_items:
                tpl.upsert_from_llm(items[0]['buffer_id'], task_id, llm_items)
                db.execute('UPDATE LLM_TASK SET status=?, finished_at=?, output_json=? WHERE llm_task_id=?', ('成功', datetime.utcnow().isoformat(), json.dumps(llm_items, ensure_ascii=False), task_id))
            else:
                db.execute('UPDATE LLM_TASK SET status=?, finished_at=?, error=? WHERE llm_task_id=?', ('失败', datetime.utcnow().isoformat(), 'LLM 返回空或失败', task_id))
            buf.clear(items[0]['buffer_id'])
    _end(db, run_id, totals)
