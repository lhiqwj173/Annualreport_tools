# 批量 AI 退市分析器

## 模块概述

本模块是一个独立于 `delist-analysis` skill 的批处理脚本，通过 LLM API 调用实现大规模退市股票的自动化分析。

## 开发进度

### 已完成功能 ✅

1. **LLM 客户端** (`llm_client.py`)
   - 兼容 OpenAI 格式 API（支持 Cherry Studio / DeepSeek 等）
   - 模型自动发现：启动时通过 `/models` 端点获取可用模型列表
   - **模型自动切换**：当一个模型失败时自动尝试下一个可用模型
   - JSON 修复：处理 Markdown 代码块、`<think>` 标签等格式问题
   - 重试机制：指数退避重试

2. **批量分析器** (`batch_ai_analyzer.py`)
   - Agent-Lite 循环（最多 8 轮推理）
   - Validation-Correction Loop：校验失败时回填错误信息让 LLM 重试
   - `updated_state` 机制：每轮 LLM 输出累积到 `current_state`
   - PDF 关键词切片：只保留包含关键词的段落，节省 Token
   - 断点续传：通过 `progress.json` 记录处理进度
   - 严格时间约束：所有公告搜索限制在退市日期之前

3. **配置文件** (`config.example.json`)
   - 支持配置 API Key、Base URL、Model 等

### 待优化项 ⚠️

1. **LLM 服务稳定性**
   - 测试期间 Cherry Studio 后端不稳定，所有模型间歇性返回 500 错误
   - 模型自动切换功能正常工作，但需要稳定的后端服务才能完成完整测试

2. **Prompt 优化**
   - LLM 在读取 PDF 后有时会重复请求同一个文档，而不是提取数据并 SUBMIT
   - 可能需要进一步强化 `updated_state` 的输出要求

3. **错误处理**
   - 当所有模型都失败时，可以考虑增加更长的等待间隔后重试

## 文件结构

```
batch_analyzer/
├── README.md              # 本文件
├── llm_client.py          # LLM API 客户端
├── batch_ai_analyzer.py   # 批量分析主程序
└── config.example.json    # 配置文件示例
```

## 使用方法

```bash
# 1. 复制配置文件并填写 API 信息
cp config.example.json config.json

# 2. 运行批量分析
python batch_ai_analyzer.py \
  --input delist_st_status.csv \
  --output delist_analysis_output.csv \
  --config config.json \
  --limit 5  # 可选，限制处理数量
```

## 依赖关系

- 依赖 `cninfo_tools.py`（位于 `.agent/skills/delist-analysis/scripts/`）
- 需要 Python 3.8+
- 需要 `requests`, `pdfplumber` 等库

## 已知问题

1. **Cherry Studio 500 错误**
   - 日期: 2026-01-18
   - 现象: 所有可用模型间歇性返回 500 Internal Server Error
   - 状态: 服务端问题，需等待服务恢复

## 更新日志

### 2026-01-18
- 初始开发完成
- 实现模型自动发现和切换功能
- 增强 JSON 解析处理 think 标签
- 重构 Prompt 引入 `updated_state` 机制
