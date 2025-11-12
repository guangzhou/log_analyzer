# -*- coding: utf-8 -*-
import re
LEAD = re.compile(r'^(?:\[[^\]]*\])+\s*')
NUM = re.compile(r'\b\d+\b')
HEX = re.compile(r'\b0x[0-9a-fA-F]+\b')
LONGID = re.compile(r'\b\d{10,}\b')
def extract_key_text(line: str) -> str:
    return LEAD.sub('', line).strip()
def normalize_key_text(text: str) -> str:
    t = LONGID.sub('<ID>', text)
    t = HEX.sub('<HEX>', t)
    t = NUM.sub('<NUM>', t)
    return t
