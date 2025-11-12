# LLM 模板补丁包

## 放置方式
将本压缩包中的文件拷贝到你的仓库根目录：
- prompts/cluster_template_zh_v1.txt
- logsys/llm_adapter.py
- config_llm.yaml（示例）
- requirements-llm.txt（当启用 LLM 时再安装）

## 使用步骤
1. 安装依赖（仅在 llm.enabled=true 时需要）
   ```bash
   pip install -r requirements-llm.txt
   ```
2. 设置密钥
   ```bash
   export OPENAI_API_KEY=你的APIKey
   ```
3. 运行第一遍，触发缓冲到阈值后会自动调用 LLM 生成正则：
   ```bash
   python -m logsys.main --config config_llm.yaml pass1 --file your_log.gz
   ```

> 你也可以把 `prompt_path` 指向任意自定义模板文件，以适配不同语言或输出字段要求。
