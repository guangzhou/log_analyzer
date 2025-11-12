# logsys/patterns.py

import re
import sqlite3
from typing import Dict, List, Optional, Set

WORD_RE = re.compile(r"[A-Za-z0-9_]{3,}")

class CompiledPattern:
    __slots__ = ("template_id", "pattern", "regex", "tokens")
    def __init__(self, template_id: int, pattern: str) -> None:
        self.template_id = template_id
        self.pattern = pattern
        self.regex = re.compile(pattern)
        # 朴素 token 提取 可替换为更强的关键短语生成
        self.tokens = set(WORD_RE.findall(pattern))

def load_compiled_patterns(db: sqlite3.Connection) -> List[CompiledPattern]:
    cur = db.cursor()
    cur.execute("SELECT template_id, pattern FROM REGEX_TEMPLATE WHERE is_active = 1")
    out: List[CompiledPattern] = []
    for tid, ptn in cur.fetchall():
        try:
            out.append(CompiledPattern(int(tid), str(ptn)))
        except re.error:
            # 可记录到审计表
            pass
    return out

def build_keyword_index(patterns: List[CompiledPattern]) -> Dict[str, Set[int]]:
    index: Dict[str, Set[int]] = {}
    for cp in patterns:
        for tk in cp.tokens:
            index.setdefault(tk, set()).add(cp.template_id)
    return index

def preselect_candidates(text: str, index: Dict[str, Set[int]]) -> Set[int]:
    cands: Set[int] = set()
    for tk in set(WORD_RE.findall(text)):
        s = index.get(tk)
        if s:
            cands |= s
    return cands

def try_match_patterns(text: str, candidates: Set[int], patterns: List[CompiledPattern]) -> Optional[int]:
    if not patterns:
        return None
    if not candidates:
        # 候选为空直接返回 None 将长尾交给 LLM 缓冲
        return None
    cand_map = {cp.template_id: cp for cp in patterns if cp.template_id in candidates}
    for tid, cp in cand_map.items():
        if cp.regex.search(text):
            return tid
    return None
