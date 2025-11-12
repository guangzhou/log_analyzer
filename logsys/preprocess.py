# -*- coding: utf-8 -*-
import re
from typing import Iterator
_TS = re.compile(r'^\[\d{8}_\d{6}\]')
def normalize_lines(lines: Iterator[str]) -> Iterator[str]:
    buf=None
    for line in lines:
        if _TS.match(line):
            if buf is not None:
                yield buf
            buf=line
        else:
            if buf is None:
                continue
            s=line.strip()
            if s:
                buf=f'{buf} {s}'
    if buf is not None:
        yield buf
