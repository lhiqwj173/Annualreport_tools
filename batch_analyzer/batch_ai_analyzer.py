#!/usr/bin/env python
# -*- coding: UTF-8 -*-
"""
批量退市分析器 (Batch AI Analyzer)

功能：
1. 读取 CSV 中的股票代码和退市日期
2. 自动获取公告列表 (严格限制在退市日期前)
3. 使用 LLM 进行"观察-分析-决策"循环，提取首次退市通知日、置换方案等关键信息
4. 严格校验数据完整性，支持断点续传
5. Validation-Correction Loop: 校验失败时回填错误信息让 LLM 重试
"""

import argparse
import csv
import json
import os
import re
import sys
import time
from pathlib import Path
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional

# 添加当前目录和 skill 脚本目录到 sys.path
current_dir = Path(__file__).resolve().parent
project_root = current_dir.parent
skill_scripts_dir = project_root / ".agent" / "skills" / "delist-analysis" / "scripts"

sys.path.insert(0, str(current_dir))
sys.path.insert(0, str(skill_scripts_dir))

from llm_client import LLMClient, load_config
import cninfo_tools

# 常量定义
MAX_TURNS = 8           # 最大推理轮次 (增加以允许更多搜索)
MAX_DOC_LENGTH = 6000   # PDF 文本最大长度
TEMP_DIR = Path("temp")
PROGRESS_FILE = "progress.json"

# ========== SKILL.md 规则客制化 Prompt ==========
SYSTEM_PROMPT = """你是一个严谨的退市股票分析师。目标：构建 Point-in-Time (PIT) 历史数据库，用于量化回测。

# 核心业务规则

## 1. 首次退市通知日 (PIT 关键)
定义：投资者**首次确定**知道股票将要退市的日期。
- MERGE (吸收合并): 取**董事会通过换股吸收合并预案**的公告日，而非最终摘牌日。
- VOLUNTARY (主动退市): 取**股东大会决议通过**的公告日。
- FORCE_* (强制退市): 取**交易所决定终止上市**的公告日。
- RECODE (更名换码): 取**证券简称和代码变更公告**发布日。

**错误示例**：使用"筹划停牌"日期（只是筹划，未确认）或"终止上市暨摘牌"日期（太晚）。

## 2. 退市类型判断
| 类型 | 特征关键词 |
|------|-----------|
| MERGE | 换股吸收合并、发行股份购买资产并吸收合并 |
| RECODE | 证券简称和证券代码变更、变更证券代码 |
| VOLUNTARY | 主动终止上市、股东大会决议方式终止 |
| TENDER | 要约收购、收购完成后终止上市 |
| FORCE_FIN | 连续亏损、净资产为负、财务类退市 |
| FORCE_TRADE | 股价低于面值、交易类退市 |
| FORCE_FRAUD | 重大违法、财务造假 |
| FORCE_NORM | 未按期披露、规范类退市 |

## 3. 置换字段要求 (MERGE/RECODE 必填)
- 置换标的code: 6位数字 (如 "600150")
- 置换标的名称: 公司简称
- 置换比例: 格式 "1:X.XXXX" (RECODE 固定为 "1:1")
- 置换完成日期: YYYY-MM-DD

**警告**: MERGE/RECODE 类型缺少这些字段会导致回测持仓价值归零！

## 4. 日期逻辑
首次退市通知日 < 退市日期

# 工作流

你需要通过多轮交互完成分析。**每轮你必须更新 state 字段，无论你选择什么 action。**

每轮你可以选择：

1. **READ_DOC**: 阅读某个公告的详细内容
   - 参数: {"id": "公告ID"}
   
2. **SEARCH_MORE**: 搜索更多公告 (自动限制在退市日期之前)
   - 参数: {"keyword": "关键词"}
   
3. **SUBMIT**: 信息完整，提交最终结果
   - 不需要额外参数，使用当前 state
   
4. **SKIP**: 确认无法找到必要信息 (历史数据缺失)
   - 参数: {"reason": "原因说明"}

# 响应格式 (每轮都必须按此格式输出)

```json
{
  "thought": "当前分析思路，缺什么信息，为什么选择这个 action...",
  "updated_state": {
    "退市类型": "MERGE | RECODE | ...",
    "退市原因": "简要描述",
    "首次退市通知日": "YYYY-MM-DD 或 null",
    "置换标的code": "6位代码 或 NaN",
    "置换标的名称": "公司名 或 NaN",
    "置换比例": "1:X.XXXX 或 NaN",
    "置换完成日期": "YYYY-MM-DD 或 NaN"
  },
  "action": "READ_DOC | SEARCH_MORE | SUBMIT | SKIP",
  "action_params": { ... }
}
```

# 重要提示
- **每次读完 PDF 后，必须立即在 updated_state 中填写你从中提取到的信息！**
- 如果你从 PDF 中看到了换股比例（如 1:0.1339），立即填入 updated_state 并 SUBMIT。
- MERGE 类型必须有：首次退市通知日、置换标的code、置换比例、置换完成日期。
- 不要重复阅读同一个公告。
"""


class BatchAnalyzer:
    """批量分析器"""
    
    def __init__(self, input_csv: str, output_csv: str, config_path: str = None):
        self.input_csv = input_csv
        self.output_csv = output_csv
        self.config_path = config_path
        self.client = LLMClient(load_config(config_path))
        self.cninfo = cninfo_tools.CNINFOClient()
        self.progress = self._load_progress()
        
        TEMP_DIR.mkdir(exist_ok=True)
        
    def _load_progress(self) -> Dict[str, str]:
        if os.path.exists(PROGRESS_FILE):
            try:
                with open(PROGRESS_FILE, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except:
                return {}
        return {}

    def _save_progress(self, code: str, status: str):
        self.progress[code] = status
        with open(PROGRESS_FILE, 'w', encoding='utf-8') as f:
            json.dump(self.progress, f, ensure_ascii=False, indent=2)

    def run(self, limit: int = None):
        """执行批量分析"""
        print(f"Starting batch analysis from {self.input_csv}...")
        
        todos = []
        with open(self.input_csv, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            for row in reader:
                todos.append(row)
        
        print(f"Loaded {len(todos)} stocks to process.")
        
        success_count = 0
        skip_count = 0
        fail_count = 0
        
        for i, row in enumerate(todos):
            if limit and i >= limit:
                break
                
            code = row.get("code") or row.get("股票代码") or row.get("Code")
            delist_date = row.get("退市日期") or row.get("DelistDate")
            name = row.get("名称") or row.get("Name") or ""
            
            if code:
                code = str(code).zfill(6)
            
            if not code or not delist_date:
                print(f"[{i+1}] Skipping invalid row: {row}")
                continue
                
            if code in self.progress and self.progress[code] == "DONE":
                print(f"[{i+1}] Skipping {code} (Already DONE)")
                continue

            print(f"\n[{i+1}/{len(todos)}] Processing {code} {name} (Delist: {delist_date})...")
            
            try:
                result = self.analyze_stock(code, delist_date, name)
                
                if result:
                    cninfo_tools.append_result_to_csv(self.output_csv, result)
                    self._save_progress(code, "DONE")
                    print(f"  ✅ Success")
                    success_count += 1
                elif result is None:
                    self._save_progress(code, "SKIPPED")
                    print(f"  ⏭️ Skipped")
                    skip_count += 1
                else:
                    self._save_progress(code, "FAILED")
                    print(f"  ❌ Failed")
                    fail_count += 1
                    
            except Exception as e:
                print(f"  ❌ Error: {e}")
                self._save_progress(code, f"ERROR: {str(e)[:100]}")
                fail_count += 1
            
        print(f"\n========== Summary ==========")
        print(f"Success: {success_count}")
        print(f"Skipped: {skip_count}")
        print(f"Failed:  {fail_count}")

    def analyze_stock(self, code: str, delist_date: str, name: str = "") -> Optional[Dict[str, Any]]:
        """分析单个股票"""
        
        # Step 1: 初始搜索
        print("  -> Fetching announcements...")
        try:
            date_end = datetime.strptime(delist_date, "%Y-%m-%d")
            date_start = date_end - timedelta(days=540)
            date_range = f"{date_start.strftime('%Y-%m-%d')}~{delist_date}"
        except ValueError:
            print(f"  Invalid date: {delist_date}")
            return False

        announcements = self.cninfo.list_announcements(
            stock_code=code,
            limit=50,
            date_range=date_range
        )
        
        if not announcements:
            print("  No announcements found.")
            return None  # SKIP
            
        # 保存公告列表到 temp（用于 download-pdf 验证）
        cache_file = TEMP_DIR / f"{code}_announcements.json"
        with open(cache_file, 'w', encoding='utf-8') as f:
            json.dump({"announcements": announcements}, f, ensure_ascii=False, indent=2)

        # Step 2: Agent Loop
        ann_summary = [{"id": a["id"], "date": a["date"], "title": a["title"]} for a in announcements]
        current_state = {"code": code, "名称": name, "退市日期": delist_date}
        last_doc_content = ""
        last_action_result = ""
        
        for turn in range(MAX_TURNS):
            print(f"  -> Turn {turn + 1}/{MAX_TURNS}")
            
            # 构造 User Prompt
            user_prompt = self._build_user_prompt(code, delist_date, ann_summary, current_state, last_doc_content, last_action_result)
            
            # 调用 LLM
            try:
                response = self.client.chat([
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": user_prompt}
                ])
            except Exception as e:
                print(f"    LLM Error: {e}")
                return False
            
            # 解析响应
            thought = response.get("thought", "")
            action = response.get("action", "")
            params = response.get("action_params", {})
            
            print(f"    Thought: {thought[:80]}...")
            print(f"    Action: {action}")
            
            # 从响应中获取 updated_state 并更新 current_state
            updated_state = response.get("updated_state", {})
            if updated_state:
                for key, value in updated_state.items():
                    if value is not None and value != "null" and value != "":
                        current_state[key] = value
            
            # 清除上一轮的文档内容 (Memory Compression)
            last_doc_content = ""
            last_action_result = ""
            
            # 执行 Action
            if action == "SUBMIT":
                # 使用累积的 current_state
                submit_data = {**current_state}
                submit_data["code"] = code  # 确保 code 正确
                
                # 校验
                validation = cninfo_tools.validate_result(submit_data)
                if validation["valid"]:
                    return submit_data
                else:
                    # Validation-Correction Loop: 回填错误
                    errors = validation.get("errors", [])
                    error_msg = "; ".join([e.get("message", str(e)) for e in errors])
                    print(f"    Validation Failed: {error_msg[:100]}")
                    last_action_result = f"VALIDATION_ERROR: {error_msg}"
                    # 继续循环让 LLM 修正
                    continue
                    
            elif action == "SKIP":
                reason = params.get("reason", "Unknown")
                print(f"    Skip Reason: {reason}")
                return None
                    
            elif action == "READ_DOC":
                ann_id = str(params.get("id", ""))
                target = next((a for a in announcements if str(a["id"]) == ann_id), None)
                
                if target and target.get("url"):
                    pdf_path = TEMP_DIR / f"{code}_{ann_id}.pdf"
                    print(f"    Downloading: {target['title'][:40]}...")
                    
                    if self.cninfo.download_pdf(target["url"], str(pdf_path)):
                        text = cninfo_tools.extract_text_from_pdf(str(pdf_path), max_pages=5)
                        # 关键词切片
                        sliced = self._slice_text_by_keywords(text)
                        last_doc_content = f"--- {target['title']} ---\n{sliced}"
                        current_state["来源公告"] = target["title"]
                        current_state["公告URL"] = target["url"]
                    else:
                        last_action_result = "ERROR: PDF download failed."
                else:
                    last_action_result = f"ERROR: Announcement ID '{ann_id}' not found."
                        
            elif action == "SEARCH_MORE":
                keyword = params.get("keyword", "")
                if keyword:
                    print(f"    Searching: {keyword}")
                    more = self.cninfo.list_announcements(code, keyword=keyword, limit=20, date_range=date_range)
                    new_anns = [a for a in more if not any(e["id"] == a["id"] for e in announcements)]
                    announcements.extend(new_anns)
                    ann_summary.extend([{"id": a["id"], "date": a["date"], "title": a["title"]} for a in new_anns])
                    last_action_result = f"SEARCH_RESULT: Found {len(new_anns)} new announcements."
                else:
                    last_action_result = "ERROR: Missing keyword."
            else:
                last_action_result = f"ERROR: Unknown action '{action}'."
        
        print(f"  -> Max turns reached without valid result.")
        return False

    def _build_user_prompt(self, code, delist_date, ann_summary, state, doc_content, last_result):
        """构建 User Prompt"""
        return f"""# 当前任务
股票代码: {code}
退市日期 (参考): {delist_date}

# 已提取信息 (State)
{json.dumps(state, ensure_ascii=False, indent=2)}

# 上一步结果
{last_result if last_result else "(无)"}

# 当前文档内容
{doc_content if doc_content else "(无 - 请选择要阅读的公告)"}

# 公告列表 (ID - Date - Title)
{json.dumps(ann_summary[:30], ensure_ascii=False, indent=2)}
{"... 更多公告省略" if len(ann_summary) > 30 else ""}

请分析并决定下一步 action。"""

    def _slice_text_by_keywords(self, text: str, context_size: int = 500) -> str:
        """根据关键词切片文本，只保留相关段落"""
        keywords = ["置换", "比例", "换股", "合并", "预案", "方案", "终止上市", "退市", "摘牌", "决议", "通过"]
        
        if len(text) <= MAX_DOC_LENGTH:
            return text
            
        # 找到所有关键词位置
        positions = []
        for kw in keywords:
            for match in re.finditer(kw, text):
                positions.append(match.start())
        
        if not positions:
            # 没找到关键词，返回开头部分
            return text[:MAX_DOC_LENGTH]
        
        # 合并重叠的切片
        positions = sorted(set(positions))
        slices = []
        current_start = None
        current_end = None
        
        for pos in positions:
            start = max(0, pos - context_size)
            end = min(len(text), pos + context_size)
            
            if current_start is None:
                current_start, current_end = start, end
            elif start <= current_end:
                current_end = max(current_end, end)
            else:
                slices.append(text[current_start:current_end])
                current_start, current_end = start, end
        
        if current_start is not None:
            slices.append(text[current_start:current_end])
        
        result = "\n...\n".join(slices)
        return result[:MAX_DOC_LENGTH]


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Batch AI Delist Analyzer")
    parser.add_argument("--input", "-i", required=True, help="Input CSV (must contain 'code' and '退市日期')")
    parser.add_argument("--output", "-o", required=True, help="Output CSV")
    parser.add_argument("--config", "-c", help="Config file path")
    parser.add_argument("--limit", "-l", type=int, help="Limit number of stocks to process")
    
    args = parser.parse_args()
    
    analyzer = BatchAnalyzer(args.input, args.output, args.config)
    analyzer.run(args.limit)
