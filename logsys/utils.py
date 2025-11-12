# -*- coding: utf-8 -*-
from datetime import datetime
def iso(ts: str) -> str:
    return ts if ts else datetime.utcnow().isoformat()
def floor_bucket(ts_iso: str, granularity: str) -> str:
    try:
        dt = datetime.fromisoformat(ts_iso)
    except Exception:
        dt = datetime.utcnow()
    if granularity == 'hour':
        return dt.replace(minute=0, second=0, microsecond=0).isoformat()
    if granularity == '5min':
        m = dt.minute - dt.minute % 5
        return dt.replace(minute=m, second=0, microsecond=0).isoformat()
    return dt.replace(second=0, microsecond=0).isoformat()
def inc(d: dict, key, delta=1):
    d[key] = d.get(key, 0) + delta
