# -*- coding: utf-8 -*-
import gzip
from typing import Iterator
def read_gz_lines(path: str) -> Iterator[str]:
    with gzip.open(path, 'rt', encoding='utf-8', errors='ignore') as f:
        for line in f:
            yield line.rstrip('\n')
