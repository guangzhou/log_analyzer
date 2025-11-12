# -*- coding: utf-8 -*-
import os, yaml
def load_config(path: str) -> dict:
    with open(path, 'r', encoding='utf-8') as f:
        cfg = yaml.safe_load(f)
    assert 'app' in cfg and 'db_path' in cfg['app'], '配置缺少 app.db_path'
    assert 'llm' in cfg and 'n_samples_per_task' in cfg['llm'], '配置缺少 llm.n_samples_per_task'
    if cfg['llm'].get('enabled'):
        key_env = cfg['llm'].get('api_key_env', '')
        if key_env and os.getenv(key_env) is None:
            print(f'警告: 环境变量 {key_env} 未设置。若需要调用 LLM, 请先导出密钥。')
    return cfg
