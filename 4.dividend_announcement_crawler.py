#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
@Project ：PycharmProjects
@File    ：巨潮资讯复权公告爬虫
@Date    ：2025/12/16
@Description: 爬取股票/基金复权相关公告（权益分派、配股等）
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
        logging.FileHandler("dividend_crawler.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)


@dataclass(frozen=True)
class CrawlerConfig:
    """爬虫配置类。"""
    start_date: str  # 开始日期 YYYY-MM-DD
    end_date: str  # 结束日期 YYYY-MM-DD
    exclude_keywords: List[str]  # 排除关键词列表
    category: str  # 报告类型
    plate: str = "sz;sh"  # 板块控制（不含北交所bj）
    max_retries: int = 5  # 最大重试次数
    retry_delay: int = 5  # 重试延迟（秒）
    timeout: int = 15  # 请求超时（秒）
    output_dir: str = "."  # 输出目录
    page_delay: float = 0.3  # 页面间延迟（秒）
    progress_file: str = "dividend_crawler_progress.txt"  # 进度文件名
    output_file: str = "复权公告链接.csv"  # 输出文件名


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
        "Referer": "http://www.cninfo.com.cn/new/commonUrl/pageOfSearch?url=disclosure/list/search",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36",
        "X-Requested-With": "XMLHttpRequest"
    }

    # API单次查询最大返回条数限制
    API_MAX_RESULTS = 3000
    # 多次爬取合并的最大尝试次数
    MAX_MERGE_ATTEMPTS = 10

    def __init__(self, config: CrawlerConfig) -> None:
        self.config = config
        self.session = requests.Session()
        self.session.headers.update(self.HEADERS)
        retries = Retry(
            total=config.max_retries,
            backoff_factor=1,
            status_forcelist=[500, 502, 503, 504]
        )
        self.session.mount('http://', HTTPAdapter(max_retries=retries))
        self.session.mount('https://', HTTPAdapter(max_retries=retries))

    def _build_request_data(self, page_num: int, date_range: str, plate: Optional[str] = None) -> Dict[str, Any]:
        return {
            "pageNum": page_num,
            "pageSize": 30,
            "column": "szse",
            "tabName": "fulltext",
            "plate": plate if plate else self.config.plate,
            "searchkey": "",
            "secid": "",
            "category": self.config.category,
            "trade": "",
            "seDate": date_range,
            "sortName": "code",
            "sortType": "asc",
            "isHLtitle": "false"
        }

    def fetch_page(self, page_num: int, date_range: str, plate: Optional[str] = None) -> Dict[str, Any]:
        """获取单页数据。失败时抛出异常。"""
        data = self._build_request_data(page_num, date_range, plate)
        last_error: Optional[Exception] = None

        for attempt in range(1, self.config.max_retries + 1):
            try:
                response = self.session.post(
                    self.BASE_URL, data=data, timeout=self.config.timeout
                )
                response.raise_for_status()
                
                try:
                    result = response.json()
                except ValueError as json_err:
                    raise RuntimeError(
                        f"JSON解析失败: {json_err}。响应内容前200字符: {response.text[:200]}"
                    ) from json_err
                
                if not isinstance(result, dict):
                    raise RuntimeError(f"API响应格式异常，期望dict，实际: {type(result).__name__}")
                
                return result
                
            except requests.exceptions.RequestException as e:
                last_error = e
                logging.warning(f"网络请求失败 (尝试 {attempt}/{self.config.max_retries}): {e}")
                if attempt < self.config.max_retries:
                    time.sleep(self.config.retry_delay)
            except RuntimeError:
                raise

        raise RuntimeError(
            f"获取数据失败（已重试{self.config.max_retries}次）: {date_range} 第{page_num}页。最后错误: {last_error}"
        )

    def _fetch_single_pass(
        self, date_range: str, plate: str, seen_ids: set, data_by_id: Dict[str, Dict[str, Any]],
        check_split: bool = False
    ) -> int:
        """单次遍历所有页面，收集数据并去重合并。"""
        page_num = 1
        max_total = 0
        local_seen_this_pass: set = set()

        while True:
            page_data = self.fetch_page(page_num, date_range, plate)
            
            total = page_data.get("totalAnnouncement", 0)
            if isinstance(total, int) and total > max_total:
                max_total = total

            if page_num == 1:
                if "totalAnnouncement" not in page_data:
                    raise RuntimeError(
                        f"API响应缺少totalAnnouncement字段: {date_range}。响应keys: {list(page_data.keys())}"
                    )
                if not isinstance(total, int):
                    raise RuntimeError(
                        f"totalAnnouncement类型异常，期望int，实际: {type(total).__name__}"
                    )
                if total == 0:
                    return 0
                if check_split and total > self.API_MAX_RESULTS:
                    logging.info(
                        f"日期 {date_range} 数据量({total})超过API限制({self.API_MAX_RESULTS})，启用分板块查询"
                    )
                    return -1

            if "announcements" not in page_data:
                raise RuntimeError(
                    f"API响应缺少announcements字段: {date_range} 第{page_num}页"
                )
            
            announcements = page_data["announcements"]
            
            if announcements is None:
                break
            if not isinstance(announcements, list):
                raise RuntimeError(
                    f"announcements类型异常，期望list，实际: {type(announcements).__name__}"
                )
            if len(announcements) == 0:
                break

            current_page_ids = set()
            for item in announcements:
                ann_id = item.get("announcementId")
                if ann_id is not None:
                    current_page_ids.add(ann_id)
                    if ann_id not in seen_ids:
                        seen_ids.add(ann_id)
                        data_by_id[ann_id] = item

            if current_page_ids and current_page_ids.issubset(local_seen_this_pass):
                logging.debug(f"{date_range} 第{page_num}页全部重复，终止本次遍历")
                break
            
            local_seen_this_pass.update(current_page_ids)
            
            print(f"\r日期 {date_range} (plate={plate}): 第{page_num}页, 本次累计 {len(local_seen_this_pass)}, 总唯一 {len(seen_ids)}/{max_total}", end='', flush=True)

            if "hasMore" not in page_data:
                raise RuntimeError(
                    f"API响应缺少hasMore字段: {date_range} 第{page_num}页"
                )
            
            if not page_data["hasMore"]:
                break

            page_num += 1
            time.sleep(self.config.page_delay)

        return max_total

    def _fetch_with_retry(self, date_range: str, plate: str, check_split: bool = False) -> List[Dict[str, Any]]:
        """多次爬取合并，直到唯一数量等于 max(totalAnnouncement)。"""
        seen_ids: set = set()
        data_by_id: Dict[str, Dict[str, Any]] = {}
        max_total = 0

        for attempt in range(1, self.MAX_MERGE_ATTEMPTS + 1):
            current_max = self._fetch_single_pass(
                date_range, plate, seen_ids, data_by_id,
                check_split=(attempt == 1 and check_split)
            )
            
            if current_max == -1:
                return self._fetch_by_split_plates(date_range)
            
            max_total = max(max_total, current_max)
            print()
            
            if max_total == 0:
                logging.info(f"日期 {date_range} (plate={plate}): 无公告数据")
                return []
            
            unique_count = len(seen_ids)
            
            if unique_count > max_total:
                raise AssertionError(
                    f"数据异常: {date_range} (plate={plate}) "
                    f"唯一数量({unique_count})超过max(totalAnnouncement)({max_total})，不符合预期"
                )
            
            if unique_count == max_total:
                if attempt > 1:
                    logging.info(f"日期 {date_range} (plate={plate}): 第{attempt}次尝试后数据完整，共{unique_count}条")
                return list(data_by_id.values())
            
            logging.info(
                f"日期 {date_range} (plate={plate}): 第{attempt}次尝试，"
                f"唯一{unique_count}/{max_total}，差{max_total - unique_count}条，继续重试"
            )
            time.sleep(self.config.page_delay)

        raise AssertionError(
            f"数据完整性校验失败: {date_range} (plate={plate}) "
            f"经过{self.MAX_MERGE_ATTEMPTS}次尝试，唯一数量({len(seen_ids)})仍小于max(totalAnnouncement)({max_total})，"
            f"差{max_total - len(seen_ids)}条"
        )

    def _fetch_by_split_plates(self, date_range: str) -> List[Dict[str, Any]]:
        """分板块查询数据，用于绕过API的3000条限制。"""
        plates = [p.strip() for p in self.config.plate.split(";") if p.strip()]
        if len(plates) <= 1:
            raise RuntimeError(
                f"无法分板块查询: 配置的plate='{self.config.plate}'只有一个板块，"
                f"但日期{date_range}数据量超过{self.API_MAX_RESULTS}条限制"
            )
        
        all_results = []
        seen_ids: set = set()
        
        for plate in plates:
            logging.info(f"  分板块查询: {date_range} plate={plate}")
            plate_results = self._fetch_with_retry(date_range, plate)
            
            for item in plate_results:
                ann_id = item.get("announcementId")
                if ann_id not in seen_ids:
                    seen_ids.add(ann_id)
                    all_results.append(item)
            
            logging.info(f"    板块 {plate} 获取 {len(plate_results)} 条，累计 {len(all_results)} 条")
        
        return all_results

    def fetch_all_pages(self, date_range: str) -> List[Dict[str, Any]]:
        """获取指定日期范围的所有页面数据。"""
        return self._fetch_with_retry(date_range, self.config.plate, check_split=True)


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


class DividendCrawler:
    """复权公告爬虫主类。"""

    CSV_HEADERS = [
        "company_code", "company_name", "title",
        "announcement_time", "announcement_id", "url", "category"
    ]

    # 公告分类映射
    CATEGORY_MAP = {
        "category_qyfpxzcs_szsh": "权益分派",
        "category_pg_szsh": "配股",
        "category_fh_jjgg": "基金分红",
        "category_qt_jjgg": "基金其他",
    }

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

    def _get_category_name(self, item: Dict[str, Any]) -> str:
        """获取公告分类名称。"""
        # 尝试从announcementType字段获取分类
        ann_type = item.get("announcementType", "")
        if ann_type:
            for code, name in self.CATEGORY_MAP.items():
                if code in ann_type:
                    return name
        return "其他"

    def _parse_announcement(self, item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """解析单条公告数据。"""
        required_fields = ["announcementTitle", "announcementTime", "secCode", "secName", "adjunctUrl"]
        missing_fields = [f for f in required_fields if f not in item]
        if missing_fields:
            raise RuntimeError(f"解析公告数据失败，缺少必要字段: {missing_fields}。数据: {item}")
        
        title = self._clean_title(item["announcementTitle"])

        if self._should_exclude(title):
            logging.debug(f"关键词过滤: {title}")
            return None

        announcement_time = item["announcementTime"]
        if not isinstance(announcement_time, (int, float)):
            raise RuntimeError(
                f"announcementTime类型异常，期望数值，实际: {type(announcement_time).__name__}。标题: {title}"
            )
        announcement_time_str = self._parse_announcement_time(int(announcement_time))

        announcement_id = item.get("announcementId")
        if announcement_id is None:
            raise RuntimeError(f"缺少announcementId字段，无法唯一标识公告: {title}")

        return {
            "company_code": item["secCode"],
            "company_name": item["secName"],
            "title": title,
            "announcement_time": announcement_time_str,
            "announcement_id": str(announcement_id),
            "url": f"http://static.cninfo.com.cn/{item['adjunctUrl']}",
            "category": self._get_category_name(item)
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

    def _load_last_completed_date(self) -> Optional[str]:
        """从进度文件加载最后完成的日期。"""
        progress_path = self._get_progress_path()
        if not progress_path.exists():
            return None
        with open(progress_path, 'r', encoding='utf-8') as f:
            content = f.read().strip()
            return content if content else None

    def _save_last_completed_date(self, date_str: str) -> None:
        """保存最后完成的日期到进度文件。"""
        progress_path = self._get_progress_path()
        with open(progress_path, 'w', encoding='utf-8') as f:
            f.write(date_str)

    def run(self) -> None:
        """执行爬取任务。"""
        logging.info("=" * 60)
        logging.info("巨潮资讯复权公告爬虫启动")
        logging.info(f"日期范围: {self.config.start_date} ~ {self.config.end_date}")
        logging.info(f"公告类型: {self.config.category}")
        logging.info(f"板块: {self.config.plate}")
        logging.info(f"排除关键词: {', '.join(self.config.exclude_keywords)}")
        logging.info("=" * 60)

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

        last_completed = self._load_last_completed_date()
        if last_completed:
            date_ranges = [d for d in all_date_ranges if d.split("~")[0] > last_completed]
            logging.info(f"检测到进度文件，上次完成: {last_completed}")
            logging.info(f"共 {len(all_date_ranges)} 个日期，待爬取 {len(date_ranges)} 个")
        else:
            date_ranges = all_date_ranges
            logging.info(f"共 {len(all_date_ranges)} 个日期")

        if not date_ranges:
            logging.info("所有日期已爬取完成，无需继续")
            return

        output_path = Path(self.config.output_dir) / self.config.output_file
        self._init_csv(output_path)

        if last_completed:
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

                if daily_parsed:
                    self._append_to_csv(daily_parsed, output_path)
                    total_saved += len(daily_parsed)
                    logging.info(f"已保存 {len(daily_parsed)} 条，累计: {total_saved} 条")

                self._save_last_completed_date(date_range.split("~")[0])

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
        logging.info("=" * 60)


if __name__ == '__main__':
    # ==================== 配置区域 ====================

    # 日期范围（公告发布日期）
    # 复权相关公告从1990年代就有，但主要从2000年后数据较完整
    START_DATE = "2000-01-01"
    END_DATE = "2025-12-16"

    # 排除关键词
    EXCLUDE_KEYWORDS = ['英文']

    # 板块控制：深市sz 沪市sh（不含北交所bj）
    PLATE = "sz;sh"

    # 复权相关公告类型：
    # - category_qyfpxzcs_szsh: 权益分派（股票分红、送股、转增股本等）
    # - category_pg_szsh: 配股
    # - category_fh_jjgg: 基金分红
    # - category_qt_jjgg: 基金其他（含基金份额拆分等）
    CATEGORY = "category_qyfpxzcs_szsh;category_pg_szsh;category_fh_jjgg;category_qt_jjgg"

    # 爬虫参数
    MAX_RETRIES = 5
    RETRY_DELAY = 5
    TIMEOUT = 15
    OUTPUT_DIR = "."
    PAGE_DELAY = 0.3

    # ==================== 执行 ====================

    config = CrawlerConfig(
        start_date=START_DATE,
        end_date=END_DATE,
        exclude_keywords=EXCLUDE_KEYWORDS,
        category=CATEGORY,
        plate=PLATE,
        max_retries=MAX_RETRIES,
        retry_delay=RETRY_DELAY,
        timeout=TIMEOUT,
        output_dir=OUTPUT_DIR,
        page_delay=PAGE_DELAY,
        progress_file="dividend_crawler_progress.txt",
        output_file="复权公告链接.csv"
    )

    crawler = DividendCrawler(config)
    crawler.run()
