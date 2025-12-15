'''
@Project ：PycharmProjects
@File    ：年报批量下载.py
@IDE     ：PyCharm
@Author  ：lingxiaotian
@Date    ：2023/5/30 11:39
@LastEditTime: 2025/12/15
'''

from __future__ import annotations

import logging
import os
import re
import warnings
from dataclasses import dataclass
from multiprocessing import Pool, cpu_count
from pathlib import Path
from typing import Optional, Tuple, List

from tqdm import tqdm
import pandas as pd
import pdfplumber
import requests

# 抑制pdfplumber的CropBox警告
warnings.filterwarnings('ignore', message='.*CropBox.*')

# 日志配置
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)


@dataclass(frozen=True)
class ConverterConfig:
    """PDF批量下载转换配置类。"""
    excel_file: str  # 数据文件路径（支持Excel和CSV）
    pdf_dir: str  # PDF存储目录
    csv_dir: str  # CSV存储目录
    target_year: int  # 目标年份
    max_retries: int = 3  # 下载最大重试次数
    timeout: int = 15  # 请求超时时间（秒）
    chunk_size: int = 8192  # 下载块大小
    processes: Optional[int] = None  # 进程数，None表示自动


class PDFDownloader:
    """PDF下载器类。"""
    
    HEADERS = {
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        "Cache-Control": "no-cache",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "same-origin",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    }
    
    def __init__(self, timeout: int = 15, chunk_size: int = 8192) -> None:
        self.timeout = timeout
        self.chunk_size = chunk_size
        self.session = requests.Session()
        self.session.headers.update(self.HEADERS)
    
    def close(self) -> None:
        """关闭Session释放资源。"""
        self.session.close()
    
    def __enter__(self) -> 'PDFDownloader':
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()
    
    def download(self, pdf_url: str, pdf_file_path: str) -> bool:
        """下载PDF文件并验证完整性。"""
        try:
            response = self.session.get(pdf_url, stream=True, timeout=self.timeout)
            
            if response.status_code == 403:
                logging.error(f"403 Forbidden: 服务器禁止访问 {pdf_url}")
                return False
            elif response.status_code != 200:
                logging.error(f"请求失败: {response.status_code}")
                return False
            
            content_type = response.headers.get("Content-Type", "")
            if "pdf" not in content_type.lower():
                logging.error(f"服务器返回的不是 PDF: {content_type}")
                return False
            
            with open(pdf_file_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=self.chunk_size):
                    if chunk:
                        f.write(chunk)
            
            if not self._verify_pdf(pdf_file_path):
                return False
            
            logging.info(f"PDF 下载成功: {pdf_file_path}")
            return True
            
        except requests.exceptions.Timeout:
            logging.error(f"下载超时: {pdf_url}")
            return False
        except requests.exceptions.RequestException as e:
            logging.error(f"下载 PDF 文件失败: {e}")
            return False
        except OSError as e:
            logging.error(f"文件写入失败: {e}")
            return False
    
    @staticmethod
    def _verify_pdf(pdf_file_path: str) -> bool:
        """验证PDF文件完整性。"""
        if not os.path.exists(pdf_file_path):
            logging.error(f"文件不存在: {pdf_file_path}")
            return False
        
        if os.path.getsize(pdf_file_path) == 0:
            logging.error(f"下载失败，文件大小为 0 KB: {pdf_file_path}")
            return False
        
        try:
            with open(pdf_file_path, "rb") as f:
                first_bytes = f.read(5)
                if not first_bytes.startswith(b"%PDF"):
                    logging.error(f"下载的文件不是有效的 PDF: {pdf_file_path}")
                    return False
        except OSError as e:
            logging.error(f"文件验证失败: {e}")
            return False
        
        return True



class PDFTableExtractor:
    """PDF表格提取器类。"""
    
    # 文件名非法字符正则
    INVALID_CHARS = r'[\\/:*?"<>|]'
    
    def __init__(self, config: ConverterConfig) -> None:
        self.config = config
        self.downloader = PDFDownloader(
            timeout=config.timeout,
            chunk_size=config.chunk_size
        )
    
    @staticmethod
    def _sanitize_filename(filename: str) -> str:
        """清理文件名中的非法字符。"""
        return re.sub(PDFTableExtractor.INVALID_CHARS, '', filename)
    
    def _download_with_retry(self, pdf_url: str, pdf_file_path: str) -> bool:
        """带重试机制的下载。"""
        for attempt in range(1, self.config.max_retries + 1):
            if self.downloader.download(pdf_url, pdf_file_path):
                return True
            if attempt < self.config.max_retries:
                logging.warning(f"重试下载 ({attempt}/{self.config.max_retries}): {pdf_url}")
        
        logging.error(f"下载失败（已重试 {self.config.max_retries} 次）: {pdf_url}")
        return False
    
    @staticmethod
    def _get_table_title(page, table_bbox) -> Optional[str]:
        """获取表格上方的章节标题。"""
        table_top = table_bbox[1]
        
        words = page.extract_words()
        # 按y坐标分组
        lines = {}
        for w in words:
            y = round(w['top'])
            if y not in lines:
                lines[y] = []
            lines[y].append((w['x0'], w['text']))
        
        # 合并同一行的文字
        line_texts = {}
        for y, words_list in lines.items():
            words_list.sort(key=lambda x: x[0])
            line_texts[y] = ''.join([w[1] for w in words_list])
        
        # 章节标题模式（以数字或括号开头）
        title_pattern = re.compile(r'^[（(一二三四五六七八九十\d]')
        
        for y in sorted(line_texts.keys(), reverse=True):
            if y >= table_top - 5:
                continue
            text = line_texts[y].strip()
            if len(text) < 4:
                continue
            if text.startswith('□'):
                continue
            if title_pattern.match(text):
                # 清理序号
                clean = re.sub(r'^[（(][一二三四五六七八九十]+[)）]\s*', '', text)
                clean = re.sub(r'^\d+[、.．]\s*', '', clean)
                return clean[:40]
        
        return None
    
    @staticmethod
    def _clean_table_data(table: List[List[str]]) -> Tuple[List[str], List[List[str]]]:
        """清理表格数据，返回表头和数据行。"""
        header = [
            cell.replace('\n', ' ').strip() if cell else f'col_{i}'
            for i, cell in enumerate(table[0])
        ]
        data_rows = []
        for row in table[1:]:
            cleaned_row = [
                cell.replace('\n', ' ').strip() if cell else ''
                for cell in row
            ]
            data_rows.append(cleaned_row)
        return header, data_rows
    
    def _extract_tables_to_csv(self, pdf_path: str, csv_dir: str, base_name: str) -> int:
        """从PDF中提取所有表格，支持跨页表格合并。
        
        跨页合并逻辑：如果当前表格没有标题，将所有数据行追加到上一个表格。
        续表通常没有重复表头，第一行就是数据。
        """
        # 收集所有表格信息
        all_tables = []  # [(title, header, data_rows), ...]
        
        try:
            with pdfplumber.open(pdf_path) as pdf:
                for page_num, page in enumerate(pdf.pages, 1):
                    try:
                        tables_obj = page.find_tables()
                        tables_data = page.extract_tables()
                        
                        for table_obj, table in zip(tables_obj, tables_data):
                            if table and len(table) >= 1:
                                title = self._get_table_title(page, table_obj.bbox)
                                header, data_rows = self._clean_table_data(table)
                                all_tables.append((title, header, data_rows, page_num))
                                
                    except Exception as e:
                        logging.debug(f"提取第 {page_num} 页表格失败: {e}")
                        continue
        except Exception as e:
            logging.error(f"打开PDF失败: {pdf_path}, 错误: {e}")
            raise
        
        if not all_tables:
            logging.warning(f"未提取到任何表格: {base_name}")
            return 0
        
        # 合并跨页表格
        merged_tables = []  # [(title, header, all_data_rows), ...]
        
        for title, header, data_rows, page_num in all_tables:
            if title is None and merged_tables:
                # 无标题 = 续表，尝试合并到上一个表格
                last_title, last_header, last_data = merged_tables[-1]
                
                # 调整列数以匹配（PDF跨页解析可能导致列数不一致）
                target_cols = len(last_header)
                
                # 调整header行（实际是数据行）
                if len(header) < target_cols:
                    header = header + [''] * (target_cols - len(header))
                elif len(header) > target_cols:
                    header = header[:target_cols]
                
                # 调整所有数据行
                adjusted_rows = []
                for row in data_rows:
                    if len(row) < target_cols:
                        row = row + [''] * (target_cols - len(row))
                    elif len(row) > target_cols:
                        row = row[:target_cols]
                    adjusted_rows.append(row)
                
                last_data.append(header)
                last_data.extend(adjusted_rows)
                continue
            
            # 新表格（有标题）
            if title is None:
                title = f"表格_p{page_num}"
            merged_tables.append((title, header, data_rows))
        
        # 保存CSV
        title_counter = {}
        for title, header, data_rows in merged_tables:
            title_clean = self._sanitize_filename(title)
            if title_clean in title_counter:
                title_counter[title_clean] += 1
                title_clean = f"{title_clean}_{title_counter[title_clean]}"
            else:
                title_counter[title_clean] = 1
            
            df = pd.DataFrame(data_rows, columns=header)
            csv_filename = f"{base_name}_{title_clean}.csv"
            csv_path = os.path.join(csv_dir, csv_filename)
            df.to_csv(csv_path, index=False, encoding='utf-8-sig')
        
        table_count = len(merged_tables)
        if table_count > 0:
            logging.info(f"CSV保存成功 ({table_count}个表格): {base_name}")
        else:
            logging.warning(f"未提取到任何表格: {base_name}")
        
        return table_count
    
    def process_single_file(
        self,
        code: int,
        name: str,
        title: str,
        announcement_time: str,
        pdf_url: str
    ) -> bool:
        """处理单个文件的下载和表格提取。"""
        # 生成文件名: {发布时间}_{标题}_{code}_{公司名称}
        datetime_str = str(announcement_time)[:19].replace('-', '').replace(':', '').replace(' ', '_')
        base_name = self._sanitize_filename(f"{datetime_str}_{title}_{code:06}_{name}")
        pdf_file_path = os.path.join(self.config.pdf_dir, f"{base_name}.pdf")
        
        try:
            # 断点续传：检查是否已有CSV文件（以base_name开头的文件）
            existing_csvs = [f for f in os.listdir(self.config.csv_dir) 
                           if f.startswith(base_name) and f.endswith('.csv')]
            if existing_csvs:
                logging.info(f"CSV已存在({len(existing_csvs)}个)，跳过: {base_name}")
                return True
            
            # 断点续传：检查PDF是否已存在且有效
            pdf_valid = (
                os.path.exists(pdf_file_path) and 
                os.path.getsize(pdf_file_path) > 0 and
                PDFDownloader._verify_pdf(pdf_file_path)
            )
            
            if not pdf_valid:
                if os.path.exists(pdf_file_path):
                    try:
                        os.remove(pdf_file_path)
                        logging.warning(f"删除损坏的PDF: {pdf_file_path}")
                    except OSError:
                        pass
                
                if not self._download_with_retry(pdf_url, pdf_file_path):
                    return False
            else:
                logging.info(f"PDF已存在，跳过下载: {base_name}.pdf")
            
            # 提取表格并保存为CSV（每个表格一个文件）
            table_count = self._extract_tables_to_csv(pdf_file_path, self.config.csv_dir, base_name)
            return table_count > 0
            
        except Exception as e:
            logging.error(f"处理文件失败 {code:06}_{name}: {e}")
            return False
        finally:
            self.downloader.close()



def _process_task(args: Tuple[ConverterConfig, int, str, str, str, str]) -> bool:
    """多进程任务包装函数。"""
    config, code, name, title, announcement_time, pdf_url = args
    extractor = PDFTableExtractor(config)
    return extractor.process_single_file(code, name, title, announcement_time, pdf_url)


class AnnualReportProcessor:
    """年报批量处理器。"""
    
    def __init__(self, config: ConverterConfig) -> None:
        self.config = config
    
    def _load_data(self) -> pd.DataFrame:
        """加载数据文件（支持Excel和CSV）。"""
        file_path = self.config.excel_file
        try:
            if file_path.endswith('.csv'):
                df = pd.read_csv(file_path)
            else:
                df = pd.read_excel(file_path)
            logging.info(f"成功加载数据文件: {file_path}")
            return df
        except FileNotFoundError:
            logging.error(f"数据文件不存在: {file_path}")
            raise
        except pd.errors.EmptyDataError:
            logging.error(f"数据文件为空: {file_path}")
            raise
        except pd.errors.ParserError as e:
            logging.error(f"数据文件解析失败: {e}")
            raise
    
    def _prepare_directories(self) -> None:
        """创建必要的目录。"""
        try:
            Path(self.config.pdf_dir).mkdir(parents=True, exist_ok=True)
            Path(self.config.csv_dir).mkdir(parents=True, exist_ok=True)
            logging.info(f"目录准备完成: PDF={self.config.pdf_dir}, CSV={self.config.csv_dir}")
        except OSError as e:
            logging.error(f"创建目录失败: {e}")
            raise RuntimeError(f"创建目录失败: {e}")
    
    def _filter_data_by_year(self, df: pd.DataFrame) -> pd.DataFrame:
        """按年份过滤数据。"""
        required_columns = ['company_code', 'company_name', 'title', 'announcement_time', 'url']
        
        missing_cols = [col for col in required_columns if col not in df.columns]
        if missing_cols:
            raise ValueError(f"数据文件缺少必需列: {missing_cols}")
        
        df['year'] = pd.to_datetime(df['announcement_time']).dt.year
        filtered = df[df['year'] == self.config.target_year]
        logging.info(f"找到 {len(filtered)} 条 {self.config.target_year} 年的记录")
        return filtered
    
    def run(self) -> None:
        """执行批量处理流程。"""
        logging.info("="*60)
        logging.info("年报批量下载与表格提取程序启动")
        logging.info(f"目标年份: {self.config.target_year}")
        logging.info("输出格式: CSV (结构化表格数据)")
        logging.info("支持断点续传：已存在的PDF和CSV文件将被跳过")
        logging.info("="*60)
        
        df = self._load_data()
        self._prepare_directories()
        
        filtered_df = self._filter_data_by_year(df)
        if filtered_df.empty:
            logging.warning(f"未找到 {self.config.target_year} 年的数据")
            return
        
        tasks = [
            (self.config, row['company_code'], row['company_name'], 
             row['title'], row['announcement_time'], row['url'])
            for _, row in filtered_df.iterrows()
        ]
        
        worker_count = self.config.processes or min(cpu_count(), len(tasks))
        logging.info(f"使用 {worker_count} 个进程处理 {len(tasks)} 个文件")
        
        with Pool(processes=worker_count) as pool:
            results = list(tqdm(
                pool.imap(_process_task, tasks),
                total=len(tasks),
                desc=f"{self.config.target_year}年处理进度"
            ))
            success_count = sum(results)
        
        logging.info("="*60)
        logging.info(f"处理完成: 成功 {success_count}/{len(tasks)}")
        logging.info("="*60)



if __name__ == '__main__':
    # ==================== 配置区域 ====================
    
    # 数据文件路径（支持Excel和CSV格式）
    DATA_FILE = "财报公告链接.csv"
    
    # 是否批量处理多个年份
    BATCH_MODE = True
    
    # 批量模式：年份区间（包含起始和结束年份）
    START_YEAR = 2022
    END_YEAR = 2024
    
    # 单独模式：指定年份
    SINGLE_YEAR = 2023
    
    # 下载配置
    MAX_RETRIES = 3  # 最大重试次数
    TIMEOUT = 15  # 请求超时（秒）
    PROCESSES = None  # 进程数（None表示自动）
    
    # ==================== 执行逻辑 ====================
    
    if BATCH_MODE:
        for year in range(START_YEAR, END_YEAR + 1):
            config = ConverterConfig(
                excel_file=DATA_FILE,
                pdf_dir=f'年报文件/{year}/pdf年报',
                csv_dir=f'年报文件/{year}/csv表格',
                target_year=year,
                max_retries=MAX_RETRIES,
                timeout=TIMEOUT,
                processes=PROCESSES
            )
            
            processor = AnnualReportProcessor(config)
            processor.run()
            
            print(f"\n{year}年年报处理完毕\n")
    else:
        config = ConverterConfig(
            excel_file=DATA_FILE,
            pdf_dir=f'年报文件/{SINGLE_YEAR}/pdf年报',
            csv_dir=f'年报文件/{SINGLE_YEAR}/csv表格',
            target_year=SINGLE_YEAR,
            max_retries=MAX_RETRIES,
            timeout=TIMEOUT,
            processes=PROCESSES
        )
        
        processor = AnnualReportProcessor(config)
        processor.run()
        
        print(f"\n{SINGLE_YEAR}年年报处理完毕\n")
