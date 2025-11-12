# 智驾日志两遍处理管线

## 快速开始
```bash
python -m logsys.main --config config.yaml init-db
python -m logsys.main --config config.yaml pass1 --file sample.gz
python -m logsys.main --config config.yaml merge-summary
python -m logsys.main --config config.yaml pass2 --file sample.gz
```

## 总流程
```mermaid
flowchart TD
  Z1[zcat 读取原始日志] --> Z2[预处理 合并非时间戳行]
  Z2 --> Z3[字段解析 提取关键内容]
  Z3 --> Z4[关键内容归一化 排序去重]
  Z4 --> Z5{匹配 REGEX_TEMPLATE}
  Z5 -- 命中 --> Z6[更新 match_count 与适用上下文]
  Z6 --> Z8[定期合入 SUMMARY 星号]
  Z5 -- 未命中 --> Z7[写入缓冲 触发 LLM 聚类生成新模板]
  Z7 --> Z8[合并入规则库]
```

## 第二遍统计
```mermaid
flowchart TD
  B1[读取第一遍的规整日志] --> B2[解析 时间戳 等级 线程号 模块 子模块 关键内容]
  B2 --> B3{按最新模板 pattern 匹配 支持本地与汇总优先策略}
  B3 -- 命中 --> B4[在内存聚合结果]
  B4 --> B5[批量写入 LOG_MATCH_SUMMARY]
  B5 --> B6[按时间桶写 KEY_TIME_BUCKET]
  B3 -- 未命中 --> B7[写异常文件 UNMATCHED_LOG 并告警]
  B7 --> B8[自动回流至第一遍 缓冲 聚类 与规则补齐]
```
