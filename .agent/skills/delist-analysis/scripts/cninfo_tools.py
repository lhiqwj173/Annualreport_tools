#!/usr/bin/env python
# -*- coding: UTF-8 -*-
"""
巨潮资讯工具集

提供以下命令：
1. list-announcements - 获取股票公告列表
2. download-pdf - 下载公告PDF
3. extract-text - 从PDF提取文本
4. append-result - 追加结果到CSV

供 Agent 调用，执行数据获取和存储任务。
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Optional
from zoneinfo import ZoneInfo

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


class CNINFOClient:
    """巨潮资讯API客户端"""

    STOCK_QUERY_URL = "http://www.cninfo.com.cn/new/hisAnnouncement/query"
    STOCK_INFO_URL = "http://www.cninfo.com.cn/new/information/topSearch/query"

    HEADERS = {
        "Accept": "*/*",
        "Accept-Encoding": "gzip, deflate",
        "Accept-Language": "zh-CN,zh;q=0.9",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "Host": "www.cninfo.com.cn",
        "Origin": "http://www.cninfo.com.cn",
        "Referer": "http://www.cninfo.com.cn/new/disclosure/stock",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "X-Requested-With": "XMLHttpRequest"
    }

    def __init__(self, timeout: int = 30, max_retries: int = 3):
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update(self.HEADERS)
        retries = Retry(total=max_retries, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
        self.session.mount('http://', HTTPAdapter(max_retries=retries))
        self.session.mount('https://', HTTPAdapter(max_retries=retries))

    def _get_org_id(self, stock_code: str) -> Optional[str]:
        """获取股票的orgId"""
        # 先尝试构造
        if stock_code.startswith('6'):
            constructed = f"gssh0{stock_code}"
        elif stock_code.startswith('0') or stock_code.startswith('3'):
            constructed = f"gssz0{stock_code}"
        elif stock_code.startswith('8') or stock_code.startswith('4'):
            constructed = f"gsbj0{stock_code}"
        else:
            constructed = f"gssz0{stock_code}"

        # 通过API查询验证
        try:
            response = self.session.post(
                self.STOCK_INFO_URL,
                data={"keyWord": stock_code},
                timeout=self.timeout
            )
            response.raise_for_status()
            result = response.json()
            if isinstance(result, list):
                for item in result:
                    if item.get("code") == stock_code:
                        return item.get("orgId", constructed)
        except Exception:
            pass

        return constructed

    def list_announcements(
        self,
        stock_code: str,
        keyword: str = "",
        sort: str = "desc",
        limit: int = 30
    ) -> List[Dict[str, Any]]:
        """
        获取股票公告列表
        
        Args:
            stock_code: 股票代码
            keyword: 搜索关键词（可选）
            sort: 排序方式 asc/desc
            limit: 返回数量限制
            
        Returns:
            公告列表 [{date, title, id, url}, ...]
        """
        org_id = self._get_org_id(stock_code)
        all_announcements = []
        page_num = 1
        page_size = 30

        while len(all_announcements) < limit:
            data = {
                "pageNum": page_num,
                "pageSize": page_size,
                "column": "szse",
                "tabName": "fulltext",
                "stock": f"{stock_code},{org_id}",
                "searchkey": keyword,
                "category": "",
                "seDate": "",
                "sortName": "time",
                "sortType": sort,
                "isHLtitle": "false"
            }

            try:
                response = self.session.post(
                    self.STOCK_QUERY_URL, data=data, timeout=self.timeout
                )
                response.raise_for_status()
                response_data = response.json()
            except Exception as e:
                print(f"Error fetching announcements: {e}", file=sys.stderr)
                break

            announcements = response_data.get("announcements", [])
            if not announcements:
                break

            for ann in announcements:
                if len(all_announcements) >= limit:
                    break
                    
                ann_time = ann.get("announcementTime", 0)
                if ann_time:
                    tz = ZoneInfo("Asia/Shanghai")
                    dt = datetime.fromtimestamp(ann_time / 1000, tz=tz)
                    date_str = dt.strftime("%Y-%m-%d")
                else:
                    date_str = ""

                adjunct_url = ann.get("adjunctUrl", "")
                full_url = f"http://static.cninfo.com.cn/{adjunct_url}" if adjunct_url else ""

                all_announcements.append({
                    "date": date_str,
                    "title": ann.get("announcementTitle", ""),
                    "id": str(ann.get("announcementId", "")),
                    "url": full_url,
                    "secName": ann.get("secName", "")
                })

            if not response_data.get("hasMore", False):
                break

            page_num += 1

        return all_announcements[:limit]

    def download_pdf(self, url: str, output_path: str) -> bool:
        """
        下载PDF文件
        
        Args:
            url: PDF的URL
            output_path: 保存路径
            
        Returns:
            是否成功
        """
        try:
            # PDF下载需要使用不同的headers
            pdf_headers = {
                "Accept": "application/pdf,*/*",
                "Accept-Encoding": "gzip, deflate",
                "Accept-Language": "zh-CN,zh;q=0.9",
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Referer": "http://www.cninfo.com.cn/new/disclosure/stock",
            }
            
            response = requests.get(url, headers=pdf_headers, timeout=self.timeout, stream=True, allow_redirects=True)
            response.raise_for_status()

            with open(output_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)

            return os.path.getsize(output_path) > 0
        except Exception as e:
            print(f"Error downloading PDF: {e}", file=sys.stderr)
            return False


def extract_text_from_pdf(pdf_path: str, max_pages: int = 10) -> str:
    """
    从PDF提取文本
    
    Args:
        pdf_path: PDF文件路径
        max_pages: 最大提取页数
        
    Returns:
        提取的文本内容
    """
    try:
        import pdfplumber
        text_parts = []
        with pdfplumber.open(pdf_path) as pdf:
            for i, page in enumerate(pdf.pages[:max_pages]):
                page_text = page.extract_text()
                if page_text:
                    text_parts.append(f"--- Page {i+1} ---\n{page_text}")
        return "\n\n".join(text_parts)
    except ImportError:
        print("Error: pdfplumber not installed. Run: pip install pdfplumber", file=sys.stderr)
        return ""
    except Exception as e:
        print(f"Error extracting text: {e}", file=sys.stderr)
        return ""


def append_result_to_csv(csv_path: str, data: Dict[str, Any]) -> bool:
    """
    追加结果到CSV文件
    
    Args:
        csv_path: CSV文件路径
        data: 要追加的数据字典
        
    Returns:
        是否成功
    """
    headers = [
        "code", "名称", "退市日期", "退市原因", "退市类型",
        "首次退市通知日", "停牌开始日", "置换标的code", "置换标的名称", "置换比例",
        "置换完成日期", "来源公告", "公告URL"
    ]

    file_exists = os.path.exists(csv_path)

    try:
        with open(csv_path, 'a', newline='', encoding='utf-8-sig') as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            if not file_exists:
                writer.writeheader()
            
            # 确保所有字段都有值
            row = {h: data.get(h, "NaN") for h in headers}
            writer.writerow(row)
        return True
    except Exception as e:
        print(f"Error appending to CSV: {e}", file=sys.stderr)
        return False


# 退市类型定义
DELIST_TYPES = {
    "MERGE": "吸收合并退市",
    "VOLUNTARY": "主动退市",
    "TENDER": "要约收购退市",
    "FORCE_FIN": "强制退市_财务",
    "FORCE_TRADE": "强制退市_交易",
    "FORCE_FRAUD": "强制退市_违法",
    "FORCE_NORM": "强制退市_规范",
    "OTHER": "其他"
}

# 需要置换字段的类型
TYPES_REQUIRE_SWAP = {"MERGE"}
# 可能需要置换字段的类型（股票要约）
TYPES_MAYBE_SWAP = {"TENDER"}
# 不需要置换字段的类型
TYPES_NO_SWAP = {"VOLUNTARY", "FORCE_FIN", "FORCE_TRADE", "FORCE_FRAUD", "FORCE_NORM", "OTHER"}

# 置换相关字段
SWAP_FIELDS = ["置换标的code", "置换标的名称", "置换比例", "置换完成日期"]
# 通用必填字段 - 所有退市类型都必须有这些字段
REQUIRED_FIELDS = [
    "code", "名称", "退市日期", "退市原因", "退市类型", 
    "首次退市通知日",  # PIT关键：投资者首次获知
    "停牌开始日",       # PIT关键：最后卖出机会
    "来源公告", "公告URL"
]


def validate_result(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    校验提取结果
    
    Args:
        data: 提取的数据字典
        
    Returns:
        校验结果 {"valid": bool, "errors": [...], "warnings": [...]}
    """
    errors = []
    warnings = []
    
    # 1. 检查通用必填字段
    for field in REQUIRED_FIELDS:
        value = data.get(field, "")
        if not value or value == "NaN":
            errors.append({
                "type": "MISSING_REQUIRED",
                "field": field,
                "message": f"必填字段 '{field}' 缺失或为空"
            })
    
    # 2. 检查 code 格式 (增强版：必须是6位数字字符串)
    code = data.get("code", "")
    if code and code != "NaN":
        # 检查类型必须是字符串
        if not isinstance(code, str):
            errors.append({
                "type": "INVALID_FORMAT",
                "field": "code",
                "message": f"股票代码必须是字符串类型，当前为 {type(code).__name__}: {code}。"
                           f"请使用引号包裹，如 \"000001\" 而非 1"
            })
        # 检查长度和格式
        elif not (len(code) == 6 and code.isdigit()):
            errors.append({
                "type": "INVALID_FORMAT",
                "field": "code",
                "message": f"股票代码格式错误: '{code}'，应为6位数字字符串（如 '000001'）"
            })
    
    # 3. 检查日期格式
    date_fields = ["退市日期", "首次退市通知日", "停牌开始日", "置换完成日期"]
    for field in date_fields:
        value = data.get(field, "")
        if value and value != "NaN":
            try:
                datetime.strptime(value, "%Y-%m-%d")
            except ValueError:
                errors.append({
                    "type": "INVALID_FORMAT",
                    "field": field,
                    "message": f"日期格式错误: '{value}'，应为 YYYY-MM-DD"
                })
    
    # 4. 检查日期逻辑
    first_notice = data.get("首次退市通知日", "")
    suspend_date = data.get("停牌开始日", "")
    delist_date = data.get("退市日期", "")
    
    # 4.1 首次通知日 < 退市日期
    if first_notice and delist_date and first_notice != "NaN" and delist_date != "NaN":
        try:
            d1 = datetime.strptime(first_notice, "%Y-%m-%d")
            d2 = datetime.strptime(delist_date, "%Y-%m-%d")
            if d1 >= d2:
                errors.append({
                    "type": "LOGIC_ERROR",
                    "field": "首次退市通知日",
                    "message": f"首次退市通知日({first_notice})应早于退市日期({delist_date})"
                })
        except ValueError:
            pass
    
    # 4.2 首次通知日 <= 停牌开始日 <= 退市日期
    if suspend_date and suspend_date != "NaN":
        try:
            d_suspend = datetime.strptime(suspend_date, "%Y-%m-%d")
            if first_notice and first_notice != "NaN":
                d_notice = datetime.strptime(first_notice, "%Y-%m-%d")
                if d_suspend < d_notice:
                    errors.append({
                        "type": "LOGIC_ERROR",
                        "field": "停牌开始日",
                        "message": f"停牌开始日({suspend_date})应晚于或等于首次退市通知日({first_notice})"
                    })
                    
                # 4.3 合理性检验：首次通知日与停牌日间隔应足够长
                days_gap = (d_suspend - d_notice).days
                if days_gap < 7:  # 间隔小于7天为警告
                    warnings.append({
                        "type": "SHORT_INTERVAL",
                        "message": f"首次退市通知日({first_notice})与停牌开始日({suspend_date})仅相隔{days_gap}天，"
                                   f"投资者反应时间很短。请确认首次通知日是否正确，可能需要搜索更早的公告（如'筹划重组'、'筹划重大事项'等）"
                    })
                    
            if delist_date and delist_date != "NaN":
                d_delist = datetime.strptime(delist_date, "%Y-%m-%d")
                if d_suspend > d_delist:
                    errors.append({
                        "type": "LOGIC_ERROR",
                        "field": "停牌开始日",
                        "message": f"停牌开始日({suspend_date})应早于或等于退市日期({delist_date})"
                    })
        except ValueError:
            pass
    
    # 5. 检查退市类型
    delist_type = data.get("退市类型", "")
    if delist_type and delist_type != "NaN":
        if delist_type not in DELIST_TYPES:
            errors.append({
                "type": "UNKNOWN_TYPE",
                "field": "退市类型",
                "message": f"未知的退市类型: '{delist_type}'，有效值: {list(DELIST_TYPES.keys())}"
            })
    
    # 6. 检查分类字段一致性
    if delist_type in TYPES_REQUIRE_SWAP:
        # MERGE 类型必须有置换信息
        for field in SWAP_FIELDS:
            value = data.get(field, "NaN")
            if not value or value == "NaN":
                errors.append({
                    "type": "FIELD_CONFLICT",
                    "field": field,
                    "message": f"退市类型为 {delist_type}，字段 '{field}' 必须有值（当前为 NaN）"
                })
        
        # 检查置换比例格式
        ratio = data.get("置换比例", "")
        if ratio and ratio != "NaN":
            import re
            if not re.match(r'^\d+:\d+\.?\d*$', ratio):
                errors.append({
                    "type": "INVALID_FORMAT",
                    "field": "置换比例",
                    "message": f"置换比例格式错误: '{ratio}'，应为 '1:X.XXXX' 格式"
                })
        
        # 检查置换标的code格式 (增强版)
        target_code = data.get("置换标的code", "")
        if target_code and target_code != "NaN":
            if not isinstance(target_code, str):
                errors.append({
                    "type": "INVALID_FORMAT",
                    "field": "置换标的code",
                    "message": f"置换标的code必须是字符串类型，当前为 {type(target_code).__name__}: {target_code}"
                })
            elif not (len(target_code) == 6 and target_code.isdigit()):
                errors.append({
                    "type": "INVALID_FORMAT",
                    "field": "置换标的code",
                    "message": f"置换标的代码格式错误: '{target_code}'，应为6位数字"
                })
    
    elif delist_type in TYPES_NO_SWAP:
        # 非合并类型，置换字段必须为 NaN
        for field in SWAP_FIELDS:
            value = data.get(field, "NaN")
            if value and value != "NaN":
                errors.append({
                    "type": "FIELD_CONFLICT",
                    "field": field,
                    "message": f"退市类型为 {delist_type}，字段 '{field}' 应为 NaN（当前为 '{value}'）"
                })
    
    elif delist_type in TYPES_MAYBE_SWAP:
        # TENDER 类型可能有也可能没有置换
        swap_values = [data.get(f, "NaN") for f in SWAP_FIELDS]
        has_any = any(v and v != "NaN" for v in swap_values)
        has_all = all(v and v != "NaN" for v in swap_values)
        
        if has_any and not has_all:
            warnings.append({
                "type": "PARTIAL_SWAP",
                "message": "要约收购退市的置换字段不完整，请确认是现金要约还是股票要约"
            })
    
    # 7. 检查 URL 格式
    url = data.get("公告URL", "")
    if url and url != "NaN":
        if not url.startswith("http"):
            errors.append({
                "type": "INVALID_FORMAT",
                "field": "公告URL",
                "message": f"URL格式错误: '{url}'，应以 http 开头"
            })
    
    return {
        "valid": len(errors) == 0,
        "errors": errors,
        "warnings": warnings,
        "data": data
    }


# 风险信号关键词
RISK_KEYWORDS = {
    "CRITICAL": [  # 🔴 紧急
        "股东大会决议方式主动终止",
        "股东大会通过.*吸收合并",
        "终止上市的决定",
        "终止上市暨摘牌",
        "股票终止上市的公告",
        "停牌公告.*终止上市",
    ],
    "HIGH": [  # 🟠 高风险
        "换股吸收合并.*预案",
        "吸收合并.*预案",
        "主动终止上市.*预案",
        "收到.*事先告知书",
        "董事会.*通过.*合并",
    ],
    "MEDIUM": [  # 🟡 中风险
        "触发退市条件",
        "可能终止上市的风险提示",
        "连续亏损",
        "净资产为负",
        "收到终止上市.*事先告知书",
    ],
    "LOW": [  # 🟢 低风险
        "筹划重大资产重组",
        "筹划重大事项",
        "重大资产重组停牌",
    ]
}


def scan_delist_risk(announcements: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    扫描公告列表，检测退市风险信号
    
    Args:
        announcements: 公告列表
        
    Returns:
        风险扫描结果 {"risk_level": str, "signals": [...]}
    """
    import re
    
    signals = []
    highest_level = None
    level_priority = {"CRITICAL": 0, "HIGH": 1, "MEDIUM": 2, "LOW": 3}
    
    for ann in announcements:
        title = ann.get("title", "")
        date = ann.get("date", "")
        
        for level, keywords in RISK_KEYWORDS.items():
            for keyword in keywords:
                if re.search(keyword, title):
                    signals.append({
                        "level": level,
                        "date": date,
                        "title": title,
                        "keyword": keyword,
                        "url": ann.get("url", "")
                    })
                    
                    # 更新最高风险等级
                    if highest_level is None or level_priority[level] < level_priority[highest_level]:
                        highest_level = level
                    break  # 一个公告只匹配一个等级
    
    # 按风险等级和日期排序
    signals.sort(key=lambda x: (level_priority.get(x["level"], 99), x["date"]))
    
    return {
        "risk_level": highest_level or "NONE",
        "signal_count": len(signals),
        "signals": signals
    }


def main():
    parser = argparse.ArgumentParser(description="巨潮资讯工具集")
    subparsers = parser.add_subparsers(dest="command", help="可用命令")

    # list-announcements
    list_parser = subparsers.add_parser("list-announcements", help="获取股票公告列表")
    list_parser.add_argument("stock_code", help="股票代码")
    list_parser.add_argument("--keyword", "-k", default="", help="搜索关键词")
    list_parser.add_argument("--sort", "-s", choices=["asc", "desc"], default="desc", help="排序方式")
    list_parser.add_argument("--limit", "-l", type=int, default=30, help="返回数量限制")

    # download-pdf
    dl_parser = subparsers.add_parser("download-pdf", help="下载公告PDF")
    dl_parser.add_argument("url", help="PDF的URL")
    dl_parser.add_argument("--output", "-o", required=True, help="保存路径")

    # extract-text
    ext_parser = subparsers.add_parser("extract-text", help="从PDF提取文本")
    ext_parser.add_argument("pdf_path", help="PDF文件路径")
    ext_parser.add_argument("--max-pages", "-m", type=int, default=10, help="最大提取页数")

    # append-result
    app_parser = subparsers.add_parser("append-result", help="追加结果到CSV")
    app_parser.add_argument("--csv", "-c", required=True, help="CSV文件路径")
    app_group = app_parser.add_mutually_exclusive_group(required=True)
    app_group.add_argument("--data", "-d", help="JSON格式的数据")
    app_group.add_argument("--file", "-f", help="包含JSON数据的文件路径")

    # validate
    val_parser = subparsers.add_parser("validate", help="校验提取结果")
    val_group = val_parser.add_mutually_exclusive_group(required=True)
    val_group.add_argument("--data", "-d", help="JSON格式的数据")
    val_group.add_argument("--file", "-f", help="包含JSON数据的文件路径")

    # scan-risk
    scan_parser = subparsers.add_parser("scan-risk", help="扫描股票退市风险")
    scan_parser.add_argument("stock_code", help="股票代码")
    scan_parser.add_argument("--days", "-d", type=int, default=30, help="扫描最近N天的公告")

    # filter-delist: 筛选退市相关公告
    filter_parser = subparsers.add_parser("filter-delist", help="筛选退市相关公告")
    filter_parser.add_argument("stock_code", help="股票代码")
    filter_parser.add_argument("--limit", "-l", type=int, default=200, help="查询公告数量上限")

    args = parser.parse_args()

    if args.command == "list-announcements":
        client = CNINFOClient()
        results = client.list_announcements(
            args.stock_code,
            keyword=args.keyword,
            sort=args.sort,
            limit=args.limit
        )
        print(json.dumps(results, ensure_ascii=False, indent=2))

    elif args.command == "download-pdf":
        client = CNINFOClient()
        success = client.download_pdf(args.url, args.output)
        if success:
            print(json.dumps({"success": True, "path": args.output}))
        else:
            print(json.dumps({"success": False, "error": "Download failed"}))
            sys.exit(1)

    elif args.command == "extract-text":
        text = extract_text_from_pdf(args.pdf_path, args.max_pages)
        if text:
            print(text)
        else:
            print("Failed to extract text", file=sys.stderr)
            sys.exit(1)

    elif args.command == "append-result":
        try:
            if args.file:
                with open(args.file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
            else:
                data = json.loads(args.data)
        except json.JSONDecodeError as e:
            print(f"Invalid JSON: {e}", file=sys.stderr)
            sys.exit(1)
        except FileNotFoundError:
            print(f"File not found: {args.file}", file=sys.stderr)
            sys.exit(1)

        if isinstance(data, list):
            success = True
            for item in data:
                if not append_result_to_csv(args.csv, item):
                    success = False
        else:
            success = append_result_to_csv(args.csv, data)
            
        if success:
            print(json.dumps({"success": True}))
        else:
            sys.exit(1)

    elif args.command == "validate":
        try:
            if args.file:
                with open(args.file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
            else:
                data = json.loads(args.data)
        except json.JSONDecodeError as e:
            print(f"Invalid JSON: {e}", file=sys.stderr)
            sys.exit(1)
        except FileNotFoundError:
            print(f"File not found: {args.file}", file=sys.stderr)
            sys.exit(1)

        if isinstance(data, list):
            results = []
            all_valid = True
            for item in data:
                res = validate_result(item)
                results.append(res)
                if not res["valid"]:
                    all_valid = False
            print(json.dumps(results, ensure_ascii=False, indent=2))
            if not all_valid:
                sys.exit(1)
        else:
            result = validate_result(data)
            print(json.dumps(result, ensure_ascii=False, indent=2))
            if not result["valid"]:
                sys.exit(1)

    elif args.command == "scan-risk":
        client = CNINFOClient()
        # 获取最近的公告
        announcements = client.list_announcements(
            args.stock_code,
            keyword="",
            sort="desc",
            limit=args.days * 3  # 假设每天最多3个公告
        )
        
        # 过滤最近N天的公告
        from datetime import datetime, timedelta
        cutoff_date = (datetime.now() - timedelta(days=args.days)).strftime("%Y-%m-%d")
        recent = [a for a in announcements if a.get("date", "") >= cutoff_date]
        
        # 扫描风险
        result = scan_delist_risk(recent)
        result["stock_code"] = args.stock_code
        result["scan_days"] = args.days
        result["announcement_count"] = len(recent)
        
        print(json.dumps(result, ensure_ascii=False, indent=2))
        
        # 如果有紧急或高风险，返回非0退出码
        if result["risk_level"] in ["CRITICAL", "HIGH"]:
            sys.exit(1)

    elif args.command == "filter-delist":
        # 筛选退市相关公告
        client = CNINFOClient()
        announcements = client.list_announcements(
            args.stock_code,
            keyword="",
            sort="desc",
            limit=args.limit
        )
        
        # 退市相关关键词
        delist_keywords = [
            "吸收合并", "换股", "终止上市", "摘牌", "退市",
            "停牌", "预案", "要约收购", "主动退市",
            "触发退市", "退市整理", "股东大会.*决议"
        ]
        
        import re
        filtered = []
        for ann in announcements:
            title = ann.get("title", "")
            for kw in delist_keywords:
                if re.search(kw, title):
                    ann["matched_keyword"] = kw
                    filtered.append(ann)
                    break
        
        # 输出结果
        result = {
            "stock_code": args.stock_code,
            "total_announcements": len(announcements),
            "filtered_count": len(filtered),
            "announcements": filtered
        }
        print(json.dumps(result, ensure_ascii=False, indent=2))

    else:
        parser.print_help()


if __name__ == "__main__":
    main()
