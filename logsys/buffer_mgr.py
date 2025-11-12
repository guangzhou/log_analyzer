# -*- coding: utf-8 -*-
from datetime import datetime
from typing import List, Dict
from .db import Database
class BufferManager:
    def __init__(self, cfg: dict, db: Database):
        self.cfg=cfg; self.db=db; self.threshold=int(cfg['llm']['n_samples_per_task'])
    def ensure_global(self) -> int:
        r=self.db.query('SELECT buffer_id FROM BUFFER_GROUP WHERE scope=? AND status=?', ('global','收集中'))
        if r: return r[0]['buffer_id']
        now=datetime.utcnow().isoformat()
        return self.db.execute('INSERT INTO BUFFER_GROUP(scope,mod,smod,size_threshold,current_size,created_at,status) VALUES(?,?,?,?,?,?,?)', ('global',None,None,self.threshold,0,now,'收集中'))
    def append_unmatched(self, run_id:int, parsed:Dict, key_text:str, raw:str) -> int:
        bid=self.ensure_global()
        self.db.execute('INSERT INTO BUFFER_ITEM(buffer_id,run_id,timestamp,mod,smod,level,thread_id,key_text,raw_log) VALUES(?,?,?,?,?,?,?,?,?)', (bid,run_id,parsed.get('timestamp'),parsed.get('mod'),parsed.get('smod'),parsed.get('level'),parsed.get('thread_id'),key_text,raw))
        self.db.execute('UPDATE BUFFER_GROUP SET current_size=current_size+1 WHERE buffer_id=?', (bid,))
        return bid
    def ready(self)->bool:
        r=self.db.query('SELECT current_size,size_threshold FROM BUFFER_GROUP WHERE scope=? AND status=?', ('global','收集中'))
        return bool(r) and r[0]['current_size']>=r[0]['size_threshold']
    def drain(self):
        r=self.db.query('SELECT buffer_id FROM BUFFER_GROUP WHERE scope=? AND status=?', ('global','收集中'))
        if not r: return []
        bid=r[0]['buffer_id']
        items=self.db.query('SELECT * FROM BUFFER_ITEM WHERE buffer_id=? ORDER BY item_id ASC', (bid,))
        self.db.execute('UPDATE BUFFER_GROUP SET status=? WHERE buffer_id=?', ('已提交',bid))
        return items
    def clear(self, bid:int):
        self.db.execute('DELETE FROM BUFFER_ITEM WHERE buffer_id=?', (bid,))
        self.db.execute('UPDATE BUFFER_GROUP SET current_size=0, status=? WHERE buffer_id=?', ('已清理',bid))
