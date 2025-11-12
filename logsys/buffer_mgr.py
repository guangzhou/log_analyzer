# -*- coding: utf-8 -*-
from typing import List, Dict
from datetime import datetime
from db import Database

class BufferManager:
    """
    简单全局缓冲 默认 scope=global
    可拓展为按 mod smod 维度分桶
    """

    def __init__(self, cfg: dict, db: Database):
        self.cfg = cfg
        self.db = db
        self.threshold = int(cfg["llm"]["n_samples_per_task"])

    def ensure_global_buffer(self) -> int:
        rows = self.db.query("SELECT buffer_id, current_size, size_threshold, status FROM BUFFER_GROUP WHERE scope=? AND status=?", ("global", "收集中"))
        if rows:
            return rows[0]["buffer_id"]
        # 新建
        now = datetime.utcnow().isoformat()
        sql = "INSERT INTO BUFFER_GROUP(scope, mod, smod, size_threshold, current_size, created_at, status) VALUES(?,?,?,?,?,?,?)"
        bid = self.db.execute(sql, ("global", None, None, self.threshold, 0, now, "收集中"))
        return bid

    def add_unmatched(self, run_id: int, parsed: Dict, key_text: str, raw: str) -> int:
        buffer_id = self.ensure_global_buffer()
        now = datetime.utcnow().isoformat()
        self.db.execute(
            "INSERT INTO BUFFER_ITEM(buffer_id, run_id, timestamp, mod, smod, level, thread_id, key_text, raw_log) VALUES(?,?,?,?,?,?,?,?,?)",
            (buffer_id, run_id, parsed.get("timestamp"), parsed.get("mod"), parsed.get("smod"), parsed.get("level"), parsed.get("thread_id"), key_text, raw)
        )
        # 更新计数
        self.db.execute("UPDATE BUFFER_GROUP SET current_size=current_size+1 WHERE buffer_id=?", (buffer_id,))
        return buffer_id

    def should_trigger(self) -> bool:
        rows = self.db.query("SELECT current_size, size_threshold FROM BUFFER_GROUP WHERE scope=? AND status=?", ("global", "收集中"))
        if not rows:
            return False
        cur, th = rows[0]["current_size"], rows[0]["size_threshold"]
        return cur >= th

    def drain_samples(self) -> List[Dict]:
        """取出 threshold 条 并将 group 标记为 已提交"""
        rows = self.db.query("SELECT buffer_id, current_size, size_threshold FROM BUFFER_GROUP WHERE scope=? AND status=?", ("global", "收集中"))
        if not rows:
            return []
        gid = rows[0]["buffer_id"]
        samples = self.db.query("SELECT * FROM BUFFER_ITEM WHERE buffer_id=? ORDER BY item_id ASC LIMIT ?", (gid, self.threshold))
        # 标记提交
        self.db.execute("UPDATE BUFFER_GROUP SET status=? WHERE buffer_id=?", ("已提交", gid))
        return samples

    def clear_buffer(self, buffer_id: int):
        self.db.execute("DELETE FROM BUFFER_ITEM WHERE buffer_id=?", (buffer_id,))
        self.db.execute("UPDATE BUFFER_GROUP SET current_size=0, status=? WHERE buffer_id=?", ("已清理", buffer_id))
