# -*- coding: utf-8 -*-
import re
from typing import List, Dict, Optional
# from db import Database
from .db import Database

class TemplateMatcher:
    """正则模板匹配器"""

    def __init__(self, db: Database):
        self.db = db
        self._compiled = []  # list of dict pattern re template_id semantic_info

    def load_templates(self):
        rows = self.db.query("SELECT template_id, pattern, semantic_info FROM REGEX_TEMPLATE WHERE is_active=1")
        self._compiled = []
        for r in rows:
            try:
                cre = re.compile(r["pattern"])
                self._compiled.append({"id": r["template_id"], "re": cre, "sem": r["semantic_info"]})
            except re.error:
                # 模式不合法 跳过或告警
                continue

    def match_text(self, key_text: str) -> Optional[Dict]:
        for it in self._compiled:
            if it["re"].search(key_text):
                return {"template_id": it["id"], "semantic_info": it["sem"]}
        return None
