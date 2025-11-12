# -*- coding: utf-8 -*-
import os, json
PROMPT_TMPL = '占位: 按规范输出 JSON 数组即可'
def call_llm(cfg: dict, samples):
    if not cfg.get('llm',{}).get('enabled', False):
        return []
    try:
        import requests
    except Exception:
        return []
    api_key=os.getenv(cfg['llm'].get('api_key_env','OPENAI_API_KEY'),'')
    if not api_key: return []
    return []
