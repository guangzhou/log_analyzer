
# -*- coding: utf-8 -*-
import os, json, re
from typing import List, Dict, Optional

def _read_text(path: str) -> str:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return f.read()
    except Exception:
        return ""

def _render_prompt(tmpl: str, samples: List[str]) -> str:
    joined = "\n".join(f"- {s}" for s in samples)
    return tmpl.replace("{SAMPLES}", joined)

def _extract_json_array(text: str) -> Optional[List[Dict]]:
    """从任意文本中提取第一个顶层 JSON 数组并解析。"""
    if not text:
        return None
    # 先尝试直接解析
    try:
        j = json.loads(text)
        if isinstance(j, list):
            return j
    except Exception:
        pass
    # 退化：提取第一个顶层 [ ... ] 片段
    start = text.find("[")
    while start != -1:
        depth = 0
        for i in range(start, len(text)):
            if text[i] == "[":
                depth += 1
            elif text[i] == "]":
                depth -= 1
                if depth == 0:
                    chunk = text[start:i+1]
                    try:
                        j = json.loads(chunk)
                        if isinstance(j, list):
                            return j
                    except Exception:
                        break
        start = text.find("[", start + 1)
    return None

def call_llm(cfg: dict, samples: List[str]) -> List[Dict]:
    """
    根据配置调用 LLM 并返回聚类后的条目列表。
    配置：
      llm.enabled: bool
      llm.api_base: OpenAI 兼容 API
      llm.model: 模型名
      llm.api_key_env: 环境变量名
      llm.timeout_sec: 超时
      llm.prompt_path: 提示词模板路径（包含 {SAMPLES} 占位符）
    """
    if not cfg.get("llm", {}).get("enabled", False):
        return []

    # 惰性导入第三方，未安装也不影响主流程
    try:
        import requests
    except Exception:
        return []

    api_key = os.getenv(cfg["llm"].get("api_key_env", "OPENAI_API_KEY"), "")
    if not api_key:
        return []

    tmpl_path = cfg["llm"].get("prompt_path") or ""
    tmpl = _read_text(tmpl_path).strip()
    if not tmpl:
        # 内置兜底模板
        tmpl = "你是日志模板生成器。仅输出 JSON 数组。每个对象包含 分类 推荐方案 匹配规则 典型日志 mod smod 出现次数。输入：{SAMPLES}"

    # 采样与去重
    uniq = []
    seen = set()
    for s in samples:
        s = (s or "").strip()
        if not s: continue
        if s in seen: continue
        seen.add(s); uniq.append(s)
        if len(uniq) >= int(cfg["llm"].get("max_prompt_samples", 50)):
            break

    prompt = _render_prompt(tmpl, uniq)

    api_base = cfg["llm"].get("api_base", "https://api.openai.com/v1")
    model = cfg["llm"].get("model", "gpt-4o-mini")
    timeout = int(cfg["llm"].get("timeout_sec", 60))

    url = f"{api_base}/chat/completions"
    headers = {"Authorization": f"Bearer {api_key}"}
    data = {
        "model": model,
        "messages": [
            {"role": "system", "content": "你是资深日志规则抽取专家。"},
            {"role": "user", "content": prompt}
        ],
        "temperature": 0.2
    }

    try:
        resp = requests.post(url, headers=headers, json=data, timeout=timeout)
        resp.raise_for_status()
        txt = resp.json()["choices"][0]["message"]["content"]
        arr = _extract_json_array(txt)
        return arr or []
    except Exception:
        return []
