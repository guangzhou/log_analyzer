# -*- coding: utf-8 -*-
from datetime import datetime
import json, os
from typing import Dict, Tuple
from .ingest import read_gz_lines
from .preprocess import normalize_lines
from .parser import parse_line
from .key_extract import extract_key_text
from .matcher import TemplateMatcher
from .utils import iso, floor_bucket, inc
from .db import Database
def run_pass2(cfg:dict, db:Database, gz_path:str):
    file_id=db.execute('INSERT INTO FILE_REGISTRY(path, status, ingested_at) VALUES(?,?,?)', (gz_path,'新', datetime.utcnow().isoformat()))
    run_id=db.execute('INSERT INTO RUN_SESSION(file_id, pass_type, config_json, started_at, status) VALUES(?,?,?,?,?)', (file_id,'PASS2', json.dumps(cfg, ensure_ascii=False), datetime.utcnow().isoformat(),'运行中'))
    matcher=TemplateMatcher(db); matcher.load_templates()
    gran=cfg['app'].get('time_bucket','5min')
    batch_threshold=int(cfg['app'].get('pass2_batch_rows',10000))
    summary={}; buckets={}; total=pre=matched=unmatched=0
    os.makedirs(cfg['app']['unmatched_dir'], exist_ok=True)
    out_um=os.path.join(cfg['app']['unmatched_dir'], f'unmatched_{int(datetime.utcnow().timestamp())}.log')
    um_f=open(out_um,'w',encoding='utf-8')
    def flush():
        for k,v in list(summary.items()):
            mod,smod,tpl_id,cls,lvl,th=k
            db.execute('''
            INSERT INTO LOG_MATCH_SUMMARY(run_id, template_id, mod, smod, classification, level, thread_id, first_ts, last_ts, line_count)
            VALUES(?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(run_id, template_id, mod, smod, classification, level, thread_id) DO UPDATE SET
              first_ts=min(first_ts, excluded.first_ts),
              last_ts=max(last_ts, excluded.last_ts),
              line_count=line_count + excluded.line_count
            ''', (run_id, tpl_id, mod, smod, cls, lvl, th, v['first_ts'], v['last_ts'], v['count']))
        summary.clear()
        for k,cnt in list(buckets.items()):
            mod,smod,tpl_id,cls,lvl,th,g,b=k
            db.execute('''
            INSERT INTO KEY_TIME_BUCKET(run_id, template_id, mod, smod, classification, level, thread_id, bucket_granularity, bucket_start, count_in_bucket)
            VALUES(?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(run_id, template_id, mod, smod, classification, level, thread_id, bucket_granularity, bucket_start) DO UPDATE SET
              count_in_bucket = count_in_bucket + excluded.count_in_bucket
            ''', (run_id, tpl_id, mod, smod, cls, lvl, th, g, b, cnt))
        buckets.clear()
    for line in normalize_lines(read_gz_lines(gz_path)):
        total+=1
        parsed=parse_line(line)
        if not parsed: continue
        pre+=1
        key_text=extract_key_text(parsed['raw'])
        hit=matcher.match_text(key_text)
        if hit:
            matched+=1
            tpl_id=hit['template_id']; cls=''
            try:
                import json as _j
                sem=_j.loads(hit['semantic_info']) if hit['semantic_info'] else {}
                cls=sem.get('分类','')
            except Exception:
                cls=''
            mod=parsed.get('mod') or ''
            smod=parsed.get('smod') or ''
            lvl=parsed.get('level') or ''
            th=parsed.get('thread_id') or ''
            ts=iso(parsed.get('timestamp'))
            sk=(mod,smod,tpl_id,cls,lvl,th)
            if sk not in summary:
                summary[sk]={'first_ts':ts,'last_ts':ts,'count':1}
            else:
                if ts<summary[sk]['first_ts']: summary[sk]['first_ts']=ts
                if ts>summary[sk]['last_ts']: summary[sk]['last_ts']=ts
                summary[sk]['count']+=1
            b=floor_bucket(ts,gran)
            bk=(mod,smod,tpl_id,cls,lvl,th,gran,b)
            inc(buckets,bk,1)
            if sum(v['count'] for v in summary.values())>=batch_threshold:
                flush()
        else:
            unmatched+=1
            um_f.write(line+'\n')
            db.execute('INSERT INTO UNMATCHED_LOG(run_id,mod,smod,level,thread_id,timestamp,key_text,raw_log,buffered,reason) VALUES(?,?,?,?,?,?,?,?,?,?)', (run_id, parsed.get('mod'), parsed.get('smod'), parsed.get('level'), parsed.get('thread_id'), parsed.get('timestamp'), key_text, parsed['raw'], 0, 'no_match_pass2'))
    flush(); um_f.close()
    db.execute('UPDATE RUN_SESSION SET ended_at=?, total_lines=?, preprocessed_lines=?, matched_lines=?, unmatched_lines=?, status=? WHERE run_id=?', (datetime.utcnow().isoformat(), total, pre, matched, unmatched, '成功', run_id))
