# Patch Notes

本补丁包含三处文件覆盖更新：

- `logsys/pass1.py`: 第一遍只做模块去重批量入库与唯一关键文本集合匹配；移除逐行 upsert 与逐行统计。
- `logsys/db.py`: 新增 `open_db_and_tune`、`upsert_module_bulk`、`upsert_smod_bulk`、`bump_template_stats_bulk`，统一批量写库与 SQLite 调优。
- `logsys/patterns.py`: 增加关键词预筛与编译缓存，只对候选模板做精匹配，降低 CPU。

使用方式：
1. 解压后覆盖到你的仓库根目录，保持 `logsys/` 目录结构。
2. 运行：
   ```bash
   python -m logsys.main --config config.yaml init-db
   python -m logsys.main --config config.yaml pass1 --file sample.gz
   python -m logsys.main --config config.yaml merge-summary
   python -m logsys.main --config config.yaml pass2 --file sample.gz
   ```
3. 可在 `config.yaml` 中新增配置项（可选）：
   ```yaml
   pass1_unique_cap: 200000
   buffer_threshold: 100
   ```
