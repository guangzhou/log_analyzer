# -*- coding: utf-8 -*-
import json
from datetime import datetime
from typing import List, Dict
from .db import Database
class TemplateManager:
    def __init__(self, db: Database): self.db=db
    def _find_by_pattern(self, pat:str):
        r=self.db.query('SELECT template_id, version FROM REGEX_TEMPLATE WHERE pattern=?', (pat,))
        return r[0] if r else None
    def upsert_from_llm(self, buffer_id:int, llm_task_id:int, items:List[Dict]):
        now=datetime.utcnow().isoformat(); ids=[]
        for it in items:
            pat=(it.get('匹配规则','') or '').strip()
            if not pat: continue
            sample=it.get('典型日志','')
            sem={'分类': it.get('分类',''), '推荐方案': it.get('推荐方案','')}
            ex=self._find_by_pattern(pat)
            if ex:
                self.db.execute('INSERT INTO TEMPLATE_HISTORY(template_id,pattern,sample_log,version,created_at,source,note) VALUES(?,?,?,?,?,?,?)', (ex['template_id'],pat,sample,ex['version'],now,'merge','LLM 合并'))
                self.db.execute('UPDATE REGEX_TEMPLATE SET last_seen=?, semantic_info=? WHERE template_id=?', (now, json.dumps(sem, ensure_ascii=False), ex['template_id']))
                tpl_id=ex['template_id']
            else:
                tpl_id=self.db.execute('INSERT INTO REGEX_TEMPLATE(pattern,sample_log,normalized_sample,match_count,first_seen,last_seen,version,is_active,semantic_info) VALUES(?,?,?,?,?,?,?,?,?)', (pat,sample,None,0,now,now,1,1,json.dumps(sem, ensure_ascii=False)))
            ids.append(tpl_id)
        return ids
    def observe(self, template_id:int, mod:str, smod:str, ts_iso:str):
        self.db.execute('''
            INSERT INTO TEMPLATE_APPLICABILITY(template_id, mod, smod, observed_count, first_seen_in_ctx, last_seen_in_ctx, source, last_updated)
            VALUES(?,?,?,?,?,?,?,?)
            ON CONFLICT(template_id, mod, smod, source) DO UPDATE SET
              observed_count = observed_count + 1,
              last_seen_in_ctx = excluded.last_seen_in_ctx,
              last_updated = excluded.last_updated
        ''', (template_id, mod or '', smod or '', 1, ts_iso, ts_iso, 'observed', ts_iso))
