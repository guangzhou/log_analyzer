# logsys/pass1.py
"""
兼容版 run_pass1：签名为 run_pass1(cfg, db, gz_path)
- 与 logsys.main 中的调用一致
- db 既可为 Database 实例 也可为 sqlite3.Connection
- 自包含 read_gz_lines 与 normalize_lines，避免 preprocess 依赖
"""

from __future__ import annotations
import re
import gzip
import sqlite3
from typing import Dict, Iterable, Iterator, List, Optional, Set, Tuple, Any
from .db import (
    open_db_and_tune,
    upsert_module_bulk,
    upsert_smod_bulk,
    bump_template_stats_bulk,
)
from .patterns import (
    load_compiled_patterns,
    build_keyword_index,
    preselect_candidates,
    try_match_patterns,
)

# 可选指标埋点
try:
    from prometheus_client import Counter  # type: ignore
    PASS1_UNIQUE = Counter("pass1_unique_norms", "Pass1 唯一 normalized 关键文本数量")
    PASS1_MATCHED = Counter("pass1_matched_norms", "Pass1 命中模板的唯一样本数")
    PASS1_BUFFERED = Counter("pass1_buffered_norms", "Pass1 未命中进入缓冲的唯一样本数")
except Exception:  # noqa: BLE001
    PASS1_UNIQUE = PASS1_MATCHED = PASS1_BUFFERED = None  # type: ignore

# === 预处理：读 gz 与合并非时间戳开头行 ======================================

_TS_PREFIX = re.compile(r"^\[\d{8}_\d{6}\]")

def read_gz_lines(gz_path: str, encoding: str = "utf-8") -> Iterator[str]:
    """流式读取 .gz 文本文件，逐行返回。"""
    with gzip.open(gz_path, "rt", encoding=encoding, errors="replace") as f:
        for line in f:
            yield line.rstrip("\n")

def normalize_lines(lines: Iterable[str]) -> Iterator[str]:
    """
    规整：把非时间戳开头的行，去掉回车后加空格拼到上一行，保证“一行一日志”。
    """
    buf: Optional[str] = None
    for raw in lines:
        s = raw.strip()
        if not s:
            continue
        if _TS_PREFIX.match(s):
            if buf is not None:
                yield buf
            buf = s
        else:
            if buf is None:
                buf = s
            else:
                buf += " " + s
    if buf is not None:
        yield buf

# === 轻量抽取与标准化 =======================================================

MOD_RE  = re.compile(r"\[MOD:([^\]]+)\]")
SMOD_RE = re.compile(r"\[SMOD:([^\]]+)\]")

NUM_RE  = re.compile(r"\b\d+(?:\.\d+)?\b")
HEX_RE  = re.compile(r"\b0x[0-9a-fA-F]+\b")
PATH_RE = re.compile(r"(?:/[A-Za-z0-9_\-\.]+)+")

class Sample:
    __slots__ = ("key_text", "mod", "smod")
    def __init__(self, key_text: str, mod: Optional[str], smod: Optional[str]) -> None:
        self.key_text = key_text
        self.mod = mod
        self.smod = smod

def fast_extract_mod_smod(raw_line: str) -> Tuple[Optional[str], Optional[str]]:
    m1 = MOD_RE.search(raw_line)
    m2 = SMOD_RE.search(raw_line)
    return (m1.group(1) if m1 else None, m2.group(1) if m2 else None)

def extract_key_text(line: str) -> str:
    """从规整行中剥去前缀中括号段, 返回关键文本。"""
    last = -1
    idx = line.find("] ")
    while idx != -1:
        last = idx
        idx = line.find("] ", last + 2)
    if last == -1:
        return line.strip()
    return line[last + 2 :].strip()

def normalize_key_text(s: str) -> str:
    s = NUM_RE.sub("<NUM>", s)
    s = HEX_RE.sub("<HEX>", s)
    s = PATH_RE.sub("<PATH>", s)
    return " ".join(s.split())

# === 入缓冲 ================================================================

def buffer_unmatched_for_llm(conn: sqlite3.Connection, samples: List['Sample'], threshold: int) -> None:
    """
    最小入缓冲实现：把未命中样本批量写入 BUFFER_ITEM。
    触发 LLM 的阈值与任务编排由下游定时任务处理。
    """
    cur = conn.cursor()
    cur.execute(
        "SELECT buffer_id, current_size, size_threshold FROM BUFFER_GROUP WHERE status = '收集中' ORDER BY buffer_id DESC LIMIT 1"
    )
    row = cur.fetchone()
    if row:
        buffer_id, current_size, size_thr = row
    else:
        cur.execute(
            "INSERT INTO BUFFER_GROUP(scope, mod, smod, size_threshold, current_size, created_at, status) "
            "VALUES('全局', NULL, NULL, ?, 0, CURRENT_TIMESTAMP, '收集中')",
            (threshold,),
        )
        buffer_id = cur.lastrowid
        current_size = 0

    to_ins = []
    for s in samples:
        to_ins.append((buffer_id, None, None, None, s.key_text, s.key_text))

    cur.executemany(
        "INSERT INTO BUFFER_ITEM(buffer_id, run_id, timestamp, level, key_text, raw_log) VALUES(?, ?, ?, ?, ?, ?)",
        to_ins,
    )
    current_size += len(to_ins)
    cur.execute("UPDATE BUFFER_GROUP SET current_size = ? WHERE buffer_id = ?", (current_size, buffer_id))
    conn.commit()

# === Pass1 主流程 ===========================================================

def _ensure_conn(db_or_database: Any) -> sqlite3.Connection:
    """
    兼容 Database 或 sqlite3.Connection。
    """
    if isinstance(db_or_database, sqlite3.Connection):
        return db_or_database
    # Database 包装类：取其 conn
    if hasattr(db_or_database, "conn"):
        return db_or_database.conn  # type: ignore[return-value]
    raise TypeError("db 参数必须是 sqlite3.Connection 或含 conn 属性的 Database 实例")

def run_pass1(cfg: dict, db: Any, gz_path: str) -> None:
    """
    与 logsys.main 的调用签名一致：run_pass1(cfg, db, file)
    第一遍 规则演进：
      1 批量 upsert MODULE 与 SUBMODULE
      2 对唯一 normalized 关键文本做规则匹配 命中更新模板计数 未命中入缓冲
    不做逐行统计 不做时间分布。
    """
    conn = _ensure_conn(db)
    open_db_and_tune(conn)

    uniq_cap = int(cfg.get("pass1_unique_cap", 200_000))
    buffer_threshold = int(cfg.get("buffer_threshold", 100))

    mods: Set[str] = set()
    mod_smods: Set[Tuple[str, str]] = set()
    uniq: Dict[str, Sample] = {}

    # 轻量抽取与唯一集合构建
    for line in normalize_lines(read_gz_lines(gz_path)):
        mod, smod = fast_extract_mod_smod(line)
        if mod:
            mods.add(mod)
            if smod:
                mod_smods.add((mod, smod))
        key = extract_key_text(line)
        norm = normalize_key_text(key)
        if norm not in uniq:
            if len(uniq) >= uniq_cap:
                uniq.pop(next(iter(uniq)))
            uniq[norm] = Sample(key_text=key, mod=mod, smod=smod)

    if PASS1_UNIQUE:
        PASS1_UNIQUE.inc(len(uniq))

    # 批量 upsert 基础表
    if mods:
        upsert_module_bulk(conn, mods)
    if mod_smods:
        upsert_smod_bulk(conn, mod_smods)

    # 模板匹配 只针对唯一集合
    patterns = load_compiled_patterns(conn)
    index    = build_keyword_index(patterns)

    matched_ids: List[int] = []
    unmatched_samples: List[Sample] = []

    for norm, s in uniq.items():
        cands = preselect_candidates(norm, index)
        tid = try_match_patterns(norm, cands, patterns)
        if tid is not None:
            matched_ids.append(tid)
        else:
            unmatched_samples.append(s)

    if matched_ids:
        bump_template_stats_bulk(conn, matched_ids)
        if PASS1_MATCHED:
            PASS1_MATCHED.inc(len(matched_ids))

    if unmatched_samples:
        buffer_unmatched_for_llm(conn, unmatched_samples, buffer_threshold)
        if PASS1_BUFFERED:
            PASS1_BUFFERED.inc(len(unmatched_samples))

    conn.commit()
