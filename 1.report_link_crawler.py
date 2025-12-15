#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
@Project ：PycharmProjects
@File    ：巨潮资讯定期报告爬虫
@IDE     ：PyCharm
@Author  ：lingxiaotian
@Date    ：2023/5/20 12:38
@LastEditTime: 2025/12/15
'''

from __future__ import annotations

import logging
import re
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Optional, Dict, Any

import openpyxl
import requests

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)


@dataclass(frozen=True)
class CrawlerConfig:
    """爬虫配置类。"""
    start_date: str  # 开始日期 YYYY-MM-DD
    end_date: str  # 结束日期 YYYY-MM-DD
    exclude_keywords: List[str]  # 排除关键词列表
    category: str = "category_ndbg_szsh"  # 报告类型
    trade: str = ""  # 行业过滤
    plate: str = "sz;sh"  # 板块控制（不含北交所bj）
    max_retries: int = 3  # 最大重试次数
    retry_delay: int = 5  # 重试延迟（秒）
    timeout: int = 10  # 请求超时（秒）
    output_dir: str = "."  # 输出目录
    save_interval: int = 500  # 增量保存间隔（条数）
    page_delay: float = 0.3  # 页面间延迟（秒）


class CNINFOClient:
    """巨潮资讯API客户端。"""

    BASE_URL = "http://www.cninfo.com.cn/new/hisAnnouncement/query"

    HEADERS = {
        "Accept": "*/*",
        "Accept-Encoding": "gzip, deflate",
        "Accept-Language": "zh-CN,zh;q=0.9",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "Host": "www.cninfo.com.cn",
        "Origin": "http://www.cninfo.com.cn",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "X-Requested-With": "XMLHttpRequest"
    }

    def __init__(self, config: CrawlerConfig) -> None:
        self.config = config
        self.session = requests.Session()
        self.session.headers.update(self.HEADERS)

    def _build_request_data(self, page_num: int, date_range: str) -> Dict[str, Any]:
        return {
            "pageNum": page_num,
            "pageSize": 30,
            "column": "szse",
            "tabName": "fulltext",
            "plate": self.config.plate,
            "searchkey": "",
            "secid": "",
            "category": self.config.category,
            "trade": self.config.trade,
            "seDate": date_range,
            "sortName": "code",
            "sortType": "asc",
            "isHLtitle": "false"
        }

    def fetch_page(self, page_num: int, date_range: str) -> Dict[str, Any]:
        """获取单页数据。失败时抛出异常。"""
        data = self._build_request_data(page_num, date_range)

        for attempt in range(1, self.config.max_retries + 1):
            try:
                response = self.session.post(
                    self.BASE_URL, data=data, timeout=self.config.timeout
                )
                response.raise_for_status()
                return response.json()
            except (requests.exceptions.RequestException, ValueError) as e:
                logging.warning(f"请求失败 (尝试 {attempt}/{self.config.max_retries}): {e}")
                if attempt < self.config.max_retries:
                    time.sleep(self.config.retry_delay)

        raise RuntimeError(f"获取数据失败（已重试{self.config.max_retries}次）: {date_range} 第{page_num}页")

    def fetch_all_pages(self, date_range: str) -> List[Dict[str, Any]]:
        """获取指定日期范围的所有页面数据。"""
        all_results = []
        page_num = 1
        expected_total = None

        while True:
            page_data = self.fetch_page(page_num, date_range)

            if page_num == 1:
                expected_total = page_data.get("totalAnnouncement", 0)
                if expected_total == 0:
                    return all_results

            announcements = page_data.get("announcements")
            if not announcements:
                break

            all_results.extend(announcements)
            print(f"\r日期 {date_range}: 第{page_num}页, 已获取 {len(all_results)}/{expected_total} 条", end='', flush=True)

            if not page_data.get("hasMore", False):
                break

            page_num += 1
            time.sleep(self.config.page_delay)

        print()

        # 严格校验数据完整性
        if expected_total is not None and len(all_results) != expected_total:
            raise AssertionError(
                f"数据完整性校验失败: {date_range} API声称{expected_total}条, 实际获取{len(all_results)}条"
            )

        return all_results


class DateRangeGenerator:
    """日期范围生成器。"""

    @staticmethod
    def generate_daily_ranges(start_date: str, end_date: str) -> List[str]:
        """生成日期范围列表（按天）。"""
        ranges = []
        current = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(end_date, "%Y-%m-%d")
        today = datetime.now().date()

        while current.date() <= end.date() and current.date() <= today:
            date_str = current.strftime("%Y-%m-%d")
            ranges.append(f"{date_str}~{date_str}")
            current += timedelta(days=1)

        return ranges


class ReportCrawler:
    """定期报告爬虫主类。"""

    def __init__(self, config: CrawlerConfig) -> None:
        self.config = config
        self.client = CNINFOClient(config)

    def _clean_title(self, title: str) -> str:
        title = title.strip()
        title = re.sub(r"<.*?>", "", title)
        title = title.replace("：", "")
        return f"《{title}》"

    def _should_exclude(self, title: str) -> bool:
        return any(kw in title for kw in self.config.exclude_keywords)

    def _parse_announcement_time(self, timestamp_ms: int) -> str:
        return datetime.fromtimestamp(timestamp_ms / 1000).strftime("%Y-%m-%d")

    def _identify_report_type(self, title: str) -> str:
        if "摘要" in title:
            return "摘要"
        if any(kw in title for kw in ["更正", "修订", "补充", "更新"]):
            return "修订"
        return "正式"

    def _identify_period_type(self, title: str) -> str:
        if "半年" in title or "中期" in title:
            return "半年报"
        if "第一季" in title or "一季" in title:
            return "一季报"
        if "第三季" in title or "三季" in title:
            return "三季报"
        if "年度报告" in title or "年报" in title:
            return "年报"
        return "未知"

    def _parse_announcement(self, item: Dict[str, Any]) -> Optional[Dict[str, str]]:
        """解析单条公告数据。"""
        try:
            title = self._clean_title(item["announcementTitle"])

            if self._should_exclude(title):
                return None

            # 提取公告日期 - 避免前视偏差的核心字段
            announcement_time = item.get("announcementTime")
            if announcement_time is None:
                raise KeyError("缺少announcementTime字段")
            announcement_date = self._parse_announcement_time(announcement_time)

            # 提取报告期年份
            year_match = re.search(r"(\d{4})年", title)
            if not year_match:
                logging.debug(f"标题中无年份信息，跳过: {title}")
                return None
            report_year = year_match.group(1)

            return {
                "company_code": item["secCode"],
                "company_name": item["secName"],
                "title": title,
                "report_year": report_year,
                "announcement_date": announcement_date,
                "period_type": self._identify_period_type(title),
                "report_type": self._identify_report_type(title),
                "announcement_id": str(item.get("announcementId", "")),
                "url": f"http://static.cninfo.com.cn/{item['adjunctUrl']}"
            }
        except KeyError as e:
            raise RuntimeError(f"解析公告数据失败，缺少必要字段: {e}")

    def _save_to_excel(self, data: List[Dict[str, str]], output_path: str) -> None:
        """保存数据到Excel。"""
        workbook = openpyxl.Workbook()
        ws = workbook.active
        ws.title = "定期报告"

        ws.append([
            "公司代码", "公司简称", "标题", "报告期年份",
            "公告日期", "报告期类型", "报告类型", "公告ID", "下载链接"
        ])

        for item in data:
            ws.append([
                item["company_code"], item["company_name"], item["title"],
                item["report_year"], item["announcement_date"], item["period_type"],
                item["report_type"], item["announcement_id"], item["url"]
            ])

        workbook.save(output_path)
        logging.info(f"Excel保存成功: {output_path}")

    def run(self) -> None:
        """执行爬取任务。"""
        logging.info("=" * 60)
        logging.info("巨潮资讯定期报告爬虫启动")
        logging.info(f"日期范围: {self.config.start_date} ~ {self.config.end_date}")
        logging.info(f"报告类型: {self.config.category}")
        logging.info(f"板块: {self.config.plate}")
        logging.info(f"排除关键词: {', '.join(self.config.exclude_keywords)}")
        logging.info("【量化提示】使用公告日期(announcement_date)避免前视偏差")
        logging.info("=" * 60)

        date_ranges = DateRangeGenerator.generate_daily_ranges(
            self.config.start_date, self.config.end_date
        )
        logging.info(f"共 {len(date_ranges)} 个日期需要爬取")

        output_filename = f"定期报告链接_{self.config.start_date}_{self.config.end_date}.xlsx"
        output_path = Path(self.config.output_dir) / output_filename

        parsed_data = []
        total_raw = 0
        filtered = 0
        last_save_count = 0

        for idx, date_range in enumerate(date_ranges, 1):
            logging.info(f"[{idx}/{len(date_ranges)}] 正在爬取: {date_range}")
            results = self.client.fetch_all_pages(date_range)
            total_raw += len(results)

            for item in results:
                parsed = self._parse_announcement(item)
                if parsed:
                    parsed_data.append(parsed)
                else:
                    filtered += 1

            # 增量保存
            if len(parsed_data) - last_save_count >= self.config.save_interval:
                self._save_to_excel(parsed_data, str(output_path))
                last_save_count = len(parsed_data)
                logging.info(f"增量保存: {len(parsed_data)} 条")

            if idx < len(date_ranges):
                time.sleep(0.5)

        if parsed_data:
            self._save_to_excel(parsed_data, str(output_path))

        logging.info("=" * 60)
        logging.info(f"爬取完成: 原始{total_raw}条, 过滤{filtered}条, 有效{len(parsed_data)}条")
        logging.info(f"保存路径: {output_path}")
        logging.info("=" * 60)



if __name__ == '__main__':
    # ==================== 配置区域 ====================

    # 日期范围（公告发布日期）
    # 首次爬取：从2007-01-01开始（2007年财务报告改革）
    # 增量爬取：从上次爬取的结束日期开始
    START_DATE = "2007-01-01"
    END_DATE = "2025-12-15"  # 爬取到今天或指定日期

    # 排除关键词
    EXCLUDE_KEYWORDS = ['英文']

    # 板块控制：深市sz 沪市sh（不含北交所bj）
    PLATE = "sz;sh"

    # 报告类型：年报/半年报/一季报/三季报
    CATEGORY = "category_ndbg_szsh;category_bndbg_szsh;category_yjdbg_szsh;category_sjdbg_szsh"

    # 行业过滤（为空则不过滤）
    TRADE = ""

    # 爬虫参数
    MAX_RETRIES = 3
    RETRY_DELAY = 5
    TIMEOUT = 10
    OUTPUT_DIR = "."
    SAVE_INTERVAL = 500
    PAGE_DELAY = 0.3

    # ==================== 执行 ====================

    config = CrawlerConfig(
        start_date=START_DATE,
        end_date=END_DATE,
        exclude_keywords=EXCLUDE_KEYWORDS,
        category=CATEGORY,
        trade=TRADE,
        plate=PLATE,
        max_retries=MAX_RETRIES,
        retry_delay=RETRY_DELAY,
        timeout=TIMEOUT,
        output_dir=OUTPUT_DIR,
        save_interval=SAVE_INTERVAL,
        page_delay=PAGE_DELAY
    )

    crawler = ReportCrawler(config)
    crawler.run()
