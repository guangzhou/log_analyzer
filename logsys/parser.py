# -*- coding: utf-8 -*-
import re
from typing import Optional, Dict
BR = re.compile(r'\[([^\]]+)\]')
PMOD = re.compile(r'\bMOD:(?P<mod>[^\]]+)')
PSMOD = re.compile(r'\bSMOD:(?P<smod>[^\]]+)')
PLEV = re.compile(r'^\[([^\]]+)\]\[([^\]]+)\]\[([EWID])\]')
def parse_line(line: str) -> Optional[Dict]:
    parts = BR.findall(line)
    if not parts:
        return None
    ts = parts[0] if '_' in parts[0] and len(parts[0])==15 else None
    level=None; mod=None; smod=None
    for p in parts:
        m = PMOD.search(p)
        if m: mod = m.group('mod')
        s = PSMOD.search(p)
        if s: smod = s.group('smod')
    ml = PLEV.match(line)
    if ml: level = ml.group(3)
    return {'timestamp': ts, 'level': level, 'thread_id': None, 'mod': mod, 'smod': smod, 'raw': line}
