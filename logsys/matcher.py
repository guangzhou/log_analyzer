# -*- coding: utf-8 -*-
import re
from typing import Optional
from .db import Database
class TemplateMatcher:
    def __init__(self, db: Database):
        self.db=db; self._compiled=[]
    def load_templates(self):
        rows=self.db.query('SELECT template_id, pattern, semantic_info FROM REGEX_TEMPLATE WHERE is_active=1')
        comp=[]
        for r in rows:
            try:
                comp.append({'template_id': r['template_id'], 're': re.compile(r['pattern']), 'semantic_info': r['semantic_info']})
            except re.error:
                pass
        self._compiled=comp
    def match_text(self, key_text: str) -> Optional[dict]:
        for it in self._compiled:
            if it['re'].search(key_text):
                return it
        return None
