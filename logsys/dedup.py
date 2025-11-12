# -*- coding: utf-8 -*-
from typing import List, Tuple
def sort_and_dedup(items: List[Tuple[str,str]]) -> List[Tuple[str,str]]:
    items = sorted(items, key=lambda x: x[0])
    seen=set(); out=[]
    for n,r in items:
        if n in seen: continue
        seen.add(n); out.append((n,r))
    return out
