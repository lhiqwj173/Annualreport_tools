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

import csv
import logging
import re
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Optional, Dict, Any
from zoneinfo import ZoneInfo

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("crawler.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
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
    max_retries: int = 5  # 最大重试次数
    retry_delay: int = 5  # 重试延迟（秒）
    timeout: int = 15  # 请求超时（秒）
    output_dir: str = "."  # 输出目录
    save_interval: int = 500  # 增量保存间隔（条数）
    page_delay: float = 0.3  # 页面间延迟（秒）
    progress_file: str = "crawler_progress.txt"  # 断点续爬进度文件


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
        "Referer": "http://www.cninfo.com.cn/new/commonUrl/pageOfSearch/index",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36",
        "X-Requested-With": "XMLHttpRequest"
    }

    def __init__(self, config: CrawlerConfig) -> None:
        self.config = config
        self.session = requests.Session()
        self.session.headers.update(self.HEADERS)
        # 配置重试策略
        retries = Retry(
            total=config.max_retries,
            backoff_factor=1,
            status_forcelist=[500, 502, 503, 504]
        )
        self.session.mount('http://', HTTPAdapter(max_retries=retries))
        self.session.mount('https://', HTTPAdapter(max_retries=retries))

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
        last_error: Optional[Exception] = None

        for attempt in range(1, self.config.max_retries + 1):
            try:
                response = self.session.post(
                    self.BASE_URL, data=data, timeout=self.config.timeout
                )
                response.raise_for_status()
                
                # 显式处理JSON解析，区分网络错误和数据格式错误
                try:
                    result = response.json()
                except ValueError as json_err:
                    raise RuntimeError(
                        f"JSON解析失败: {json_err}。响应内容前200字符: {response.text[:200]}"
                    ) from json_err
                
                # 校验响应结构完整性
                if not isinstance(result, dict):
                    raise RuntimeError(f"API响应格式异常，期望dict，实际: {type(result).__name__}")
                
                return result
                
            except requests.exceptions.RequestException as e:
                last_error = e
                logging.warning(f"网络请求失败 (尝试 {attempt}/{self.config.max_retries}): {e}")
                if attempt < self.config.max_retries:
                    time.sleep(self.config.retry_delay)
            except RuntimeError:
                # JSON解析或响应格式错误，不重试，直接抛出
                raise

        raise RuntimeError(
            f"获取数据失败（已重试{self.config.max_retries}次）: {date_range} 第{page_num}页。最后错误: {last_error}"
        )

    def fetch_all_pages(self, date_range: str) -> List[Dict[str, Any]]:
        """获取指定日期范围的所有页面数据。"""
        all_results = []
        page_num = 1
        expected_total: Optional[int] = None

        while True:
            page_data = self.fetch_page(page_num, date_range)

            if page_num == 1:
                # 严格校验首页响应结构
                if "totalAnnouncement" not in page_data:
                    raise RuntimeError(
                        f"API响应缺少totalAnnouncement字段: {date_range}。响应keys: {list(page_data.keys())}"
                    )
                expected_total = page_data["totalAnnouncement"]
                if not isinstance(expected_total, int):
                    raise RuntimeError(
                        f"totalAnnouncement类型异常，期望int，实际: {type(expected_total).__name__}"
                    )
                if expected_total == 0:
                    logging.info(f"日期 {date_range}: 无公告数据")
                    return all_results

            # 严格校验announcements字段
            if "announcements" not in page_data:
                raise RuntimeError(
                    f"API响应缺少announcements字段: {date_range} 第{page_num}页"
                )
            
            announcements = page_data["announcements"]
            
            # announcements为None表示该页无数据（正常结束）
            if announcements is None:
                if page_num == 1 and expected_total > 0:
                    raise RuntimeError(
                        f"数据异常: {date_range} 声称有{expected_total}条数据，但首页announcements为None"
                    )
                break
            
            if not isinstance(announcements, list):
                raise RuntimeError(
                    f"announcements类型异常，期望list，实际: {type(announcements).__name__}"
                )

            all_results.extend(announcements)
            print(f"\r日期 {date_range}: 第{page_num}页, 已获取 {len(all_results)}/{expected_total} 条", end='', flush=True)

            # 严格校验hasMore字段
            if "hasMore" not in page_data:
                raise RuntimeError(
                    f"API响应缺少hasMore字段: {date_range} 第{page_num}页"
                )
            
            if not page_data["hasMore"]:
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

    CSV_HEADERS = [
        "company_code", "company_name", "title",
        "announcement_time", "announcement_id", "url"
    ]

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
        """解析公告时间戳，显式指定Asia/Shanghai时区。"""
        tz_shanghai = ZoneInfo("Asia/Shanghai")
        dt = datetime.fromtimestamp(timestamp_ms / 1000, tz=tz_shanghai)
        return dt.strftime("%Y-%m-%d %H:%M:%S")



    def _parse_announcement(self, item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """解析单条公告数据。返回None仅表示被排除关键词过滤，其他情况严格抛出异常。"""
        # 严格校验必要字段存在性
        required_fields = ["announcementTitle", "announcementTime", "secCode", "secName", "adjunctUrl"]
        missing_fields = [f for f in required_fields if f not in item]
        if missing_fields:
            raise RuntimeError(f"解析公告数据失败，缺少必要字段: {missing_fields}。数据: {item}")
        
        title = self._clean_title(item["announcementTitle"])

        # 排除关键词过滤 - 唯一允许返回None的情况
        if self._should_exclude(title):
            logging.debug(f"关键词过滤: {title}")
            return None

        # 提取公告日期
        announcement_time = item["announcementTime"]
        if not isinstance(announcement_time, (int, float)):
            raise RuntimeError(
                f"announcementTime类型异常，期望数值，实际: {type(announcement_time).__name__}。标题: {title}"
            )
        announcement_time_str = self._parse_announcement_time(int(announcement_time))

        # 严格校验公告ID字段
        announcement_id = item.get("announcementId")
        if announcement_id is None:
            raise RuntimeError(f"缺少announcementId字段，无法唯一标识公告: {title}")

        return {
            "company_code": item["secCode"],
            "company_name": item["secName"],
            "title": title,
            "announcement_time": announcement_time_str,
            "announcement_id": str(announcement_id),
            "url": f"http://static.cninfo.com.cn/{item['adjunctUrl']}"
        }

    def _init_csv(self, output_path: Path) -> None:
        """初始化CSV文件（写入表头）。"""
        if not output_path.exists():
            with open(output_path, 'w', newline='', encoding='utf-8-sig') as f:
                writer = csv.DictWriter(f, fieldnames=self.CSV_HEADERS)
                writer.writeheader()

    def _append_to_csv(self, data: List[Dict[str, Any]], output_path: Path) -> None:
        """追加数据到CSV文件。"""
        with open(output_path, 'a', newline='', encoding='utf-8-sig') as f:
            writer = csv.DictWriter(f, fieldnames=self.CSV_HEADERS)
            writer.writerows(data)

    def _get_progress_path(self) -> Path:
        """获取进度文件路径。"""
        return Path(self.config.output_dir) / self.config.progress_file

    def _load_completed_dates(self) -> set:
        """加载已完成的日期集合。"""
        progress_path = self._get_progress_path()
        if not progress_path.exists():
            return set()
        with open(progress_path, 'r', encoding='utf-8') as f:
            return set(line.strip() for line in f if line.strip())

    def _save_completed_date(self, date_range: str) -> None:
        """保存已完成的日期到进度文件。"""
        progress_path = self._get_progress_path()
        with open(progress_path, 'a', encoding='utf-8') as f:
            f.write(f"{date_range}\n")

    def run(self) -> None:
        """执行爬取任务。所有异常严格向上抛出，不静默处理。"""
        logging.info("=" * 60)
        logging.info("巨潮资讯定期报告爬虫启动")
        logging.info(f"日期范围: {self.config.start_date} ~ {self.config.end_date}")
        logging.info(f"报告类型: {self.config.category}")
        logging.info(f"板块: {self.config.plate}")
        logging.info(f"排除关键词: {', '.join(self.config.exclude_keywords)}")
        logging.info("=" * 60)

        # 校验日期格式
        try:
            datetime.strptime(self.config.start_date, "%Y-%m-%d")
            datetime.strptime(self.config.end_date, "%Y-%m-%d")
        except ValueError as e:
            raise ValueError(f"日期格式错误，期望YYYY-MM-DD: {e}") from e

        all_date_ranges = DateRangeGenerator.generate_daily_ranges(
            self.config.start_date, self.config.end_date
        )
        
        if not all_date_ranges:
            raise ValueError(
                f"日期范围无效: {self.config.start_date} ~ {self.config.end_date}，未生成任何日期"
            )

        # 断点续爬：加载已完成的日期，过滤掉
        completed_dates = self._load_completed_dates()
        if completed_dates:
            logging.info(f"检测到进度文件，已完成 {len(completed_dates)} 个日期")
        date_ranges = [d for d in all_date_ranges if d not in completed_dates]
        logging.info(f"共 {len(all_date_ranges)} 个日期，待爬取 {len(date_ranges)} 个")

        if not date_ranges:
            logging.info("所有日期已爬取完成，无需继续")
            return

        output_filename = "财报公告链接.csv"
        output_path = Path(self.config.output_dir) / output_filename

        # 初始化CSV文件
        self._init_csv(output_path)

        # 运行模式判断
        is_resume = len(completed_dates) > 0
        if is_resume:
            logging.info("【增量模式】从上次中断位置继续爬取")
        else:
            logging.info("【全量模式】首次运行，从头开始爬取")

        total_saved = 0
        total_raw = 0
        filtered = 0
        current_date_range: Optional[str] = None

        try:
            for idx, date_range in enumerate(date_ranges, 1):
                current_date_range = date_range
                logging.info(f"[{idx}/{len(date_ranges)}] 正在爬取: {date_range}")
                results = self.client.fetch_all_pages(date_range)
                total_raw += len(results)

                daily_parsed = []
                for item in results:
                    parsed = self._parse_announcement(item)
                    if parsed:
                        daily_parsed.append(parsed)
                    else:
                        filtered += 1

                # 关键：先写入CSV，再记录进度，确保数据不丢失
                if daily_parsed:
                    self._append_to_csv(daily_parsed, output_path)
                    total_saved += len(daily_parsed)
                    logging.info(f"已保存 {len(daily_parsed)} 条，累计: {total_saved} 条")

                # 数据已持久化后，才记录进度
                self._save_completed_date(date_range)

                if idx < len(date_ranges):
                    time.sleep(0.5)

        except Exception as e:
            logging.error(f"爬取过程中发生异常，当前日期: {current_date_range}")
            logging.error(f"已完成: 原始{total_raw}条, 过滤{filtered}条, 有效{total_saved}条")
            logging.error(f"进度已保存，可重新运行继续爬取")
            raise RuntimeError(f"爬取失败于 {current_date_range}: {e}") from e

        logging.info("=" * 60)
        logging.info(f"爬取完成: 原始{total_raw}条, 过滤{filtered}条, 有效{total_saved}条")
        logging.info(f"保存路径: {output_path}")
        logging.info(f"进度文件: {self._get_progress_path()}")
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

    # 报告类型：年报/半年报/一季报/三季报/业绩预告
    CATEGORY = "category_ndbg_szsh;category_bndbg_szsh;category_yjdbg_szsh;category_sjdbg_szsh;category_yjygjxz_szsh"

    # 行业过滤（为空则不过滤）
    TRADE = ""

    # 爬虫参数
    MAX_RETRIES = 5
    RETRY_DELAY = 5
    TIMEOUT = 15
    OUTPUT_DIR = "."
    SAVE_INTERVAL = 500
    PAGE_DELAY = 0.3
    PROGRESS_FILE = "crawler_progress.txt"  # 断点续爬进度文件

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
        page_delay=PAGE_DELAY,
        progress_file=PROGRESS_FILE
    )

    crawler = ReportCrawler(config)
    crawler.run()
