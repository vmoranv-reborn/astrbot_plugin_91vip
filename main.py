import os
import json
import uuid
import time
import argparse
import csv
import random
import shutil
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, asdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from itertools import count
from threading import Lock
from urllib.parse import urlparse

from astrbot.api.event import filter, AstrMessageEvent
from astrbot.api.star import Context, Star, StarTools
from astrbot.api import logger
from astrbot.api import AstrBotConfig

# 爬虫相关导入
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

# 异步HTTP和图片处理导入
import aiohttp
from PIL import Image

# 爬虫相关常量和类
VIDEO_CARD_SELECTOR = ".well-sm.videos-text-align"
TITLE_SELECTOR = ".video-title"

@dataclass
class VideoRecord:
    link: str
    title: str

@dataclass
class TaskStatus:
    """任务状态"""
    task_id: str
    task_type: str
    status: str  # "running", "completed", "failed", "cancelled"
    progress: int  # 0-100
    message: str
    result: Optional[Any] = None
    error: Optional[str] = None


# 91porn爬虫函数
def build_page_url(category: str, viewtype: str, page: int) -> str:
    return f"https://91porn.com/v.php?category={category}&viewtype={viewtype}&page={page}"

def fetch_page(
    session: requests.Session,
    category: str,
    viewtype: str,
    page: int,
    timeout: float,
    delay: float,
) -> List[VideoRecord]:
    url = build_page_url(category, viewtype, page)
    time.sleep(delay)
    try:
        # 增强请求头以避免403错误
        enhanced_headers = {
            'User-Agent': session.headers.get('User-Agent',
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Cache-Control': 'max-age=0',
            'Referer': 'https://91porn.com/',
            'DNT': '1',
            'Sec-GPC': '1'
        }
        
        response = session.get(url, timeout=timeout, headers=enhanced_headers)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")
        cards = soup.select(VIDEO_CARD_SELECTOR)
        results: List[VideoRecord] = []
        seen_links = set()
        for card in cards:
            link_el = card.find("a")
            title_el = card.select_one(TITLE_SELECTOR)
            link = getattr(link_el, "get", lambda *_: None)("href")
            title = title_el.text.strip() if title_el and title_el.text else "无标题"
            if not link or not title or link in seen_links:
                continue
            seen_links.add(link)
            results.append(VideoRecord(link=link, title=title))
        return results
    except requests.RequestException as e:
        logger.debug("Request failed for page %d: %s", page, e)
        raise

def crawl_91porn(args: argparse.Namespace) -> None:
    """增强版91porn爬虫函数，支持更好的错误处理和日志记录"""
    session = requests.Session()
    session.headers.update({"User-Agent": args.user_agent})

    logger.info(
        "Starting crawl: category=%s, viewtype=%s, start_page=%s, max_pages=%s, workers=%s",
        args.category,
        args.viewtype,
        args.start_page,
        args.max_pages,
        args.workers,
    )

    csv_writer = None
    jsonl_file = None
    csv_file = None
    lock = Lock()
    total_records = 0
    pages_processed = 0
    empty_pages_in_a_row = 0
    failed_pages = 0

    try:
        # 验证输出文件路径
        if args.output_csv:
            try:
                csv_file = open(args.output_csv, "w", encoding="utf-8", newline="")
                fieldnames = list(VideoRecord.__annotations__.keys())
                csv_writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
                csv_writer.writeheader()
                logger.info(f"CSV输出文件已创建: {args.output_csv}")
            except Exception as e:
                logger.error(f"创建CSV文件失败: {e}")
                raise

        if args.output_jsonl:
            try:
                jsonl_file = open(args.output_jsonl, "w", encoding="utf-8")
                logger.info(f"JSONL输出文件已创建: {args.output_jsonl}")
            except Exception as e:
                logger.error(f"创建JSONL文件失败: {e}")
                raise

        with ThreadPoolExecutor(max_workers=args.workers) as executor, tqdm(
            desc="Crawling pages", unit="page", total=args.max_pages
        ) as pbar:
            page_generator = count(args.start_page)
            futures = {
                executor.submit(
                    fetch_page,
                    session,
                    args.category,
                    args.viewtype,
                    next(page_generator),
                    args.timeout,
                    args.delay,
                )
                for _ in range(args.workers)
            }

            while futures:
                done, futures = as_completed(futures), set()

                for future in done:
                    try:
                        records = future.result()
                        pages_processed += 1
                        pbar.update(1)

                        if records:
                            empty_pages_in_a_row = 0
                            logger.debug(f"页面 {pages_processed} 获取到 {len(records)} 条记录")
                            
                            with lock:
                                if csv_writer and csv_file:
                                    for record in records:
                                        csv_writer.writerow(asdict(record))
                                if jsonl_file:
                                    for record in records:
                                        jsonl_file.write(
                                            json.dumps(asdict(record), ensure_ascii=False)
                                            + "\n"
                                        )
                                total_records += len(records)
                        else:
                            empty_pages_in_a_row += 1
                            logger.warning(f"页面 {pages_processed} 没有获取到数据")

                        pbar.set_postfix(records=f"{total_records}", failed=f"{failed_pages}")

                    except Exception as e:
                        logger.error(f"页面 {pages_processed} 获取失败: {e}")
                        failed_pages += 1
                        empty_pages_in_a_row += 1

                    # 检查停止条件
                    stop_condition_met = False
                    if args.max_pages and pages_processed >= args.max_pages:
                        logger.info(f"达到最大页数限制: {args.max_pages}")
                        stop_condition_met = True
                    
                    if empty_pages_in_a_row >= args.stop_on_empty_pages:
                        logger.info(f"连续 {args.stop_on_empty_pages} 页无数据，停止爬取")
                        stop_condition_met = True

                    if stop_condition_met:
                        logger.info("停止条件满足，不再提交新任务")
                        continue

                    # 提交新任务
                    try:
                        futures.add(
                            executor.submit(
                                fetch_page,
                                session,
                                args.category,
                                args.viewtype,
                                next(page_generator),
                                args.timeout,
                                args.delay,
                            )
                        )
                    except Exception as e:
                        logger.error(f"提交新任务失败: {e}")

                # 清理剩余任务
                if not futures:
                    logger.debug("清理剩余任务...")
                    for f in executor._threads:
                        try:
                            if hasattr(f, '_work_queue') and f._work_queue:
                                f._work_queue.queue.clear()
                        except Exception as e:
                            logger.debug(f"清理任务时出错: {e}")

    except KeyboardInterrupt:
        logger.warning("用户中断爬取过程")
    except Exception as e:
        logger.error(f"爬取过程中发生严重错误: {e}")
        raise
    finally:
        # 清理资源
        try:
            if csv_file:
                csv_file.close()
                logger.info("CSV文件已关闭")
            if jsonl_file:
                jsonl_file.close()
                logger.info("JSONL文件已关闭")
            session.close()
            logger.info("HTTP会话已关闭")
        except Exception as e:
            logger.error(f"清理资源时出错: {e}")

    logger.info(
        "爬取完成。处理页面: %d, 成功记录: %d, 失败页面: %d",
        pages_processed,
        total_records,
        failed_pages,
    )


class MyPlugin(Star):
    def __init__(self, context: Context, config: AstrBotConfig):
        super().__init__(context)
        self.config = config
        
        # 使用StarTools获取数据目录 - 按照参考代码的方式
        data_dir = StarTools.get_data_dir("astrbot_plugin_91vip")
        data_dir.mkdir(parents=True, exist_ok=True)
        
        # 设置子目录
        self.data_dir = str(data_dir)
        self.outputs_dir = os.path.join(self.data_dir, "outputs")
        self.temp_dir = os.path.join(self.data_dir, "temp")
        
        # 确保目录存在
        os.makedirs(self.outputs_dir, exist_ok=True)
        os.makedirs(self.temp_dir, exist_ok=True)
        
        # 任务管理
        self.tasks: Dict[str, TaskStatus] = {}
        self.executor = ThreadPoolExecutor(max_workers=config.get("max_concurrent_tasks", 2))
        
        # 异步HTTP客户端
        self.http_client: Optional[aiohttp.ClientSession] = None
        
        logger.info("91porn爬虫插件初始化完成")

    async def initialize(self):
        """可选择实现异步的插件初始化方法，当实例化该插件类之后会自动调用该方法。"""
        await self.initialize_async()
        logger.info("91porn爬虫插件异步初始化完成")

    async def initialize_async(self):
        """异步初始化HTTP客户端"""
        try:
            # 初始化HTTP客户端
            proxy = self.config.get("proxy", "") if self.config else ""
            timeout = self.config.get("timeout", 30) if self.config else 30

            # 设置默认请求头
            headers = {
                'User-Agent': self.config.get("91porn_user_agent",
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"),
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
                'Accept-Encoding': 'gzip, deflate',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
                'Sec-Fetch-Dest': 'document',
                'Sec-Fetch-Mode': 'navigate',
                'Sec-Fetch-Site': 'none',
                'Cache-Control': 'max-age=0'
            }

            connector = aiohttp.TCPConnector(limit=10)
            timeout_config = aiohttp.ClientTimeout(total=timeout)

            # 正确的代理配置方式
            if proxy:
                self.http_client = aiohttp.ClientSession(
                    connector=connector,
                    timeout=timeout_config,
                    proxy=proxy,
                    headers=headers
                )
            else:
                self.http_client = aiohttp.ClientSession(
                    connector=connector,
                    timeout=timeout_config,
                    headers=headers
                )

            logger.info("HTTP客户端初始化完成")
        except Exception as e:
            logger.error(f"HTTP客户端初始化失败: {e}")

    def _generate_task_id(self) -> str:
        """生成任务ID"""
        return str(uuid.uuid4())[:8]

    def _get_task_status(self, task_id: str) -> Optional[TaskStatus]:
        """获取任务状态"""
        return self.tasks.get(task_id)

    def _get_all_tasks(self) -> Dict[str, TaskStatus]:
        """获取所有任务状态"""
        return self.tasks.copy()

    def _cancel_task(self, task_id: str) -> bool:
        """取消任务"""
        if task_id in self.tasks:
            self.tasks[task_id].status = "cancelled"
            return True
        return False

    def _run_91porn_scraper(self, task_id: str, category: str, max_pages: int, output_format: str, return_content: bool = False):
        """运行增强版91porn爬虫，支持更好的进度跟踪和错误处理"""
        try:
            # 更新任务状态
            self.tasks[task_id].progress = 5
            self.tasks[task_id].message = "准备爬取参数..."
            
            # 创建输出文件路径
            timestamp = int(time.time())
            
            if output_format == "jsonl":
                output_file = os.path.join(self.outputs_dir, f"91porn_{category}_{timestamp}.jsonl") if not return_content else None
                csv_file = None
            else:
                output_file = os.path.join(self.outputs_dir, f"91porn_{category}_{timestamp}.csv") if not return_content else None
                csv_file = output_file
                output_file = None
            
            # 验证输出目录
            if not return_content:
                try:
                    os.makedirs(self.outputs_dir, exist_ok=True)
                except Exception as e:
                    raise Exception(f"无法创建输出目录: {e}")
            
            # 构建增强版参数
            import argparse
            args = argparse.Namespace()
            args.category = category
            args.viewtype = self.config.get("91porn_viewtype", "basic")
            args.start_page = self.config.get("91porn_start_page", 1)
            args.max_pages = max_pages
            args.workers = self.config.get("91porn_workers", 3)
            args.delay = self.config.get("91porn_delay", 1.0)
            args.timeout = self.config.get("91porn_timeout", 15.0)
            args.user_agent = self.config.get("91porn_user_agent",
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
            args.output_jsonl = output_file
            args.output_csv = csv_file
            args.verbose = self.config.get("91porn_verbose", False)
            args.stop_on_empty_pages = self.config.get("91porn_stop_on_empty_pages", 3)
            
            # 更新任务状态
            self.tasks[task_id].progress = 15
            self.tasks[task_id].message = f"开始爬取 {category} 分类数据，最多 {max_pages} 页..."
            
            # 记录爬取开始
            start_time = time.time()
            logger.info(f"开始爬取任务 {task_id}: {category} 分类，最多 {max_pages} 页")
            
            # 如果需要直接返回内容，使用内存爬取
            if return_content:
                records = self._crawl_to_memory(args)
                crawl_duration = time.time() - start_time
                
                # 更新任务状态
                self.tasks[task_id].progress = 100
                self.tasks[task_id].message = f"爬取完成！耗时 {crawl_duration:.1f} 秒，获取 {len(records)} 条记录"
                self.tasks[task_id].result = {
                    "records": records,
                    "records_count": len(records),
                    "crawl_duration": crawl_duration,
                    "category": category,
                    "max_pages": max_pages,
                    "output_format": output_format,
                    "return_content": True
                }
            else:
                # 运行增强版爬虫（保存到文件）
                crawl_91porn(args)
                
                # 计算爬取耗时
                crawl_duration = time.time() - start_time
                
                # 统计结果文件
                records_count = 0
                final_file = output_file or csv_file
                
                if final_file and os.path.exists(final_file):
                    try:
                        if output_format == "jsonl":
                            with open(final_file, 'r', encoding='utf-8') as f:
                                records_count = sum(1 for _ in f)
                        else:  # CSV
                            with open(final_file, 'r', encoding='utf-8') as f:
                                records_count = sum(1 for _ in f) - 1  # 减去标题行
                    except Exception as e:
                        logger.warning(f"统计记录数量失败: {e}")
                        records_count = "未知"
                
                # 更新任务状态
                self.tasks[task_id].progress = 100
                self.tasks[task_id].message = f"爬取完成！耗时 {crawl_duration:.1f} 秒，获取 {records_count} 条记录"
                self.tasks[task_id].result = {
                    "output_file": final_file,
                    "records_count": records_count,
                    "crawl_duration": crawl_duration,
                    "category": category,
                    "max_pages": max_pages,
                    "output_format": output_format,
                    "return_content": False
                }
            
            logger.info(f"爬取任务 {task_id} 完成")
            
        except KeyboardInterrupt:
            logger.warning(f"爬取任务 {task_id} 被用户中断")
            self.tasks[task_id].status = "cancelled"
            self.tasks[task_id].message = "爬取被用户中断"
        except Exception as e:
            logger.error(f"91porn爬虫任务 {task_id} 失败: {e}")
            self.tasks[task_id].status = "failed"
            self.tasks[task_id].error = str(e)
            self.tasks[task_id].message = f"爬取失败: {str(e)}"
        finally:
            if self.tasks[task_id].status not in ["failed", "cancelled"]:
                self.tasks[task_id].status = "completed"

    def _crawl_to_memory(self, args: argparse.Namespace) -> List[VideoRecord]:
        """爬取数据到内存，不保存到文件"""
        session = requests.Session()
        session.headers.update({"User-Agent": args.user_agent})
        
        # 增强请求头以避免403错误
        enhanced_headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Cache-Control': 'max-age=0',
            'Referer': 'https://91porn.com/',
            'DNT': '1',
            'Sec-GPC': '1'
        }
        session.headers.update(enhanced_headers)
        
        all_records = []
        pages_processed = 0
        empty_pages_in_a_row = 0
        failed_pages = 0
        
        try:
            with ThreadPoolExecutor(max_workers=args.workers) as executor:
                page_generator = count(args.start_page)
                futures = {
                    executor.submit(
                        fetch_page,
                        session,
                        args.category,
                        args.viewtype,
                        next(page_generator),
                        args.timeout,
                        args.delay,
                    )
                    for _ in range(min(args.workers, args.max_pages or args.workers))
                }

                while futures and pages_processed < (args.max_pages or float('inf')):
                    done, futures = as_completed(futures), set()

                    for future in done:
                        try:
                            records = future.result()
                            pages_processed += 1

                            if records:
                                empty_pages_in_a_row = 0
                                all_records.extend(records)
                                logger.debug(f"页面 {pages_processed} 获取到 {len(records)} 条记录")
                            else:
                                empty_pages_in_a_row += 1
                                logger.warning(f"页面 {pages_processed} 没有获取到数据")

                            # 检查停止条件
                            if empty_pages_in_a_row >= args.stop_on_empty_pages:
                                logger.info(f"连续 {args.stop_on_empty_pages} 页无数据，停止爬取")
                                break

                            # 提交新任务
                            if pages_processed < (args.max_pages or float('inf')):
                                try:
                                    futures.add(
                                        executor.submit(
                                            fetch_page,
                                            session,
                                            args.category,
                                            args.viewtype,
                                            next(page_generator),
                                            args.timeout,
                                            args.delay,
                                        )
                                    )
                                except Exception as e:
                                    logger.error(f"提交新任务失败: {e}")
                                    break

                        except Exception as e:
                            logger.error(f"页面 {pages_processed} 获取失败: {e}")
                            failed_pages += 1
                            empty_pages_in_a_row += 1

                            # 检查停止条件
                            if empty_pages_in_a_row >= args.stop_on_empty_pages:
                                logger.info(f"连续 {args.stop_on_empty_pages} 页无数据，停止爬取")
                                break

                            # 提交新任务
                            if pages_processed < (args.max_pages or float('inf')):
                                try:
                                    futures.add(
                                        executor.submit(
                                            fetch_page,
                                            session,
                                            args.category,
                                            args.viewtype,
                                            next(page_generator),
                                            args.timeout,
                                            args.delay,
                                        )
                                    )
                                except Exception as e:
                                    logger.error(f"提交新任务失败: {e}")
                                    break

        except Exception as e:
            logger.error(f"内存爬取过程中发生错误: {e}")
            raise
        finally:
            session.close()

        logger.info(
            "内存爬取完成。处理页面: %d, 成功记录: %d, 失败页面: %d",
            pages_processed,
            len(all_records),
            failed_pages,
        )
        
        return all_records


    async def download_image(self, image_url: str) -> Optional[str]:
        """下载图片到临时目录"""
        try:
            if not image_url:
                logger.error("图片URL为空")
                return None

            # 生成临时文件路径
            file_extension = os.path.splitext(urlparse(image_url).path)[1] or ".jpg"
            temp_file_path = os.path.join(
                self.temp_dir,
                f"91vip_image_{random.randint(1000, 9999)}{file_extension}",
            )

            # 确保HTTP客户端已初始化
            if not self.http_client:
                await self.initialize_async()

            # 下载图片
            async with self.http_client.get(image_url) as response:
                if response.status != 200:
                    logger.error(f"下载图片失败，状态码: {response.status}")
                    return None

                content = await response.read()
                with open(temp_file_path, "wb") as f:
                    f.write(content)

            logger.info(f"图片下载成功: {temp_file_path}")
            return temp_file_path

        except Exception as e:
            logger.error(f"下载图片失败: {e}")
            return None

    async def censor_image(self, image_path: str) -> str:
        """对图片进行打码处理"""
        try:
            if not image_path or not os.path.exists(image_path):
                logger.error("图片文件不存在")
                return ""

            # 检查是否启用打码功能
            if not self.config.get("image_censorship_enabled", True):
                logger.info("图片打码功能已禁用，返回原图")
                return image_path

            # 打开图片
            with Image.open(image_path) as img:
                # 转换为RGB模式（如果是RGBA或其他模式）
                if img.mode != "RGB":
                    img = img.convert("RGB")

                # 获取图片尺寸
                width, height = img.size

                # 计算马赛克块大小（基于图片尺寸的百分比）
                mosaic_level = self.config.get("mosaic_level", 0.8)
                if mosaic_level <= 0 or mosaic_level > 1:
                    mosaic_level = 0.8

                # 根据马赛克程度计算块大小
                # 马赛克程度越高，块大小越大
                block_size = int(
                    min(width, height) * mosaic_level * 0.05
                )  # 5% * 马赛克程度
                block_size = max(block_size, 5)  # 最小块大小为5像素

                # 创建马赛克效果
                for y in range(0, height, block_size):
                    for x in range(0, width, block_size):
                        # 获取当前块的平均颜色
                        block = img.crop((x, y, x + block_size, y + block_size))
                        if block.size[0] > 0 and block.size[1] > 0:
                            # 计算平均颜色
                            avg_color = tuple(
                                int(sum(c) / len(c)) for c in zip(*block.getdata())
                            )

                            # 创建纯色块
                            solid_block = Image.new(
                                "RGB", (block_size, block_size), avg_color
                            )
                            img.paste(solid_block, (x, y))

                # 保存打码后的图片
                censored_path = os.path.join(
                    self.temp_dir, f"censored_{os.path.basename(image_path)}"
                )
                img.save(censored_path, "JPEG", quality=85)

            # 删除原始图片
            try:
                os.remove(image_path)
            except Exception as e:
                logger.warning(f"删除原始图片失败: {e}")

            logger.info(f"图片打码完成: {censored_path}")
            return censored_path

        except Exception as e:
            logger.error(f"图片打码失败: {e}")
            # 如果打码失败，尝试删除原始图片
            try:
                if os.path.exists(image_path):
                    os.remove(image_path)
            except Exception as e:
                logger.warning(f"删除原始图片失败: {e}")
            return ""  # 返回空字符串表示打码失败

    async def get_91porn_thumbnails(self, category: str = "rf", max_thumbnails: int = 5) -> List[Dict[str, str]]:
        """获取91porn视频缩略图"""
        try:
            # 确保HTTP客户端已初始化
            if not self.http_client:
                await self.initialize_async()

            # 构建URL
            url = build_page_url(category, "basic", 1)
            
            # 获取页面内容
            async with self.http_client.get(url) as response:
                if response.status != 200:
                    logger.error(f"获取页面失败，状态码: {response.status}")
                    return []
                
                html = await response.text()
                soup = BeautifulSoup(html, "html.parser")
                
                # 查找视频卡片
                cards = soup.select(VIDEO_CARD_SELECTOR)
                thumbnails = []
                
                for card in cards[:max_thumbnails]:
                    try:
                        # 获取标题
                        title_el = card.select_one(TITLE_SELECTOR)
                        title = title_el.text.strip() if title_el and title_el.text else "无标题"
                        
                        # 获取链接
                        link_el = card.find("a")
                        link = getattr(link_el, "get", lambda *_: None)("href")
                        
                        # 获取图片
                        img_el = card.find("img")
                        img_src = getattr(img_el, "get", lambda *_: None)("src")
                        
                        if link and img_src:
                            # 确保图片URL是完整的
                            if img_src.startswith("//"):
                                img_src = "https:" + img_src
                            elif img_src.startswith("/"):
                                img_src = "https://91porn.com" + img_src
                            
                            thumbnails.append({
                                "title": title,
                                "link": link,
                                "image_url": img_src
                            })
                    except Exception as e:
                        logger.warning(f"解析视频卡片失败: {e}")
                        continue
                
                return thumbnails
                
        except Exception as e:
            logger.error(f"获取91porn缩略图失败: {e}")
            return []

    # 注册指令：爬取91porn视频列表（直接返回内容）
    @filter.command("91porn")
    async def scrape_91porn(self, event: AstrMessageEvent, category: str = None, count: int = None):
        """爬取91porn视频列表并直接返回结果
        
        用法: /91porn [分类] [数量]
        分类: rf(热门), mv(最新), vd(视频)等，默认为rf
        数量: 要获取的视频数量，默认为5（建议不要超过10个）
        
        示例:
        /91porn rf 5
        /91porn mv 3
        /91porn
        """
        user_name = event.get_sender_name()
        
        try:
            # 检查功能是否启用
            if not self.config.get("91porn_enabled", True):
                yield event.plain_result(f"❌ {user_name}, 91porn爬虫功能已禁用")
                return
            
            # 使用配置默认值，但限制数量以避免消息过长
            category = category or self.config.get("91porn_category", "rf")
            count = min(count or 5, 10)  # 限制最大10个
            
            yield event.plain_result(f"🔍 {user_name}, 正在获取{category}分类的{count}个视频，请稍候...")
            
            # 直接执行爬取，不使用任务系统
            try:
                # 记录爬取开始
                start_time = time.time()
                logger.info(f"开始直接爬取: {category} 分类，获取 {count} 个视频")
                
                # 获取缩略图列表
                thumbnails = await self.get_91porn_thumbnails(category, count)
                
                if thumbnails:
                    # 处理每个缩略图
                    processed_thumbnails = []
                    for i, thumbnail in enumerate(thumbnails, 1):
                        try:
                            # 下载图片
                            image_path = await self.download_image(thumbnail["image_url"])
                            if not image_path:
                                logger.warning(f"第{i}个封面下载失败")
                                continue
                            
                            # 打码处理
                            censored_image_path = await self.censor_image(image_path)
                            if not censored_image_path:
                                logger.warning(f"第{i}个封面处理失败")
                                continue
                            
                            processed_thumbnails.append({
                                "title": thumbnail["title"],
                                "link": thumbnail["link"],
                                "image_path": censored_image_path
                            })
                            
                        except Exception as e:
                            logger.error(f"处理第{i}个封面失败: {e}")
                            continue
                    
                    crawl_duration = time.time() - start_time
                    
                    if processed_thumbnails:
                        # 格式化结果
                        message = f"✅ {user_name}, 获取完成！\n\n"
                        message += "📊 统计信息:\n"
                        message += f"   🏷️ 分类: {category}\n"
                        message += f"   📝 视频数: {len(processed_thumbnails)}\n"
                        message += f"   ⏱️ 耗时: {crawl_duration:.1f}秒\n\n"
                        
                        message += "📋 视频列表:\n\n"
                        
                        # 发送每个视频的信息和图片
                        for i, thumbnail in enumerate(processed_thumbnails, 1):
                            # 发送图片
                            yield event.image_result(thumbnail["image_path"])
                            
                            # 发送视频信息
                            info_text = f"📹 视频 {i}/{len(processed_thumbnails)}\n"
                            info_text += f"🏷️ 标题: {thumbnail['title']}\n"
                            info_text += f"🔗 链接: {thumbnail['link']}"
                            yield event.plain_result(info_text)
                    else:
                        yield event.plain_result(f"❌ {user_name}, 所有视频封面处理失败")
                else:
                    yield event.plain_result(f"❌ {user_name}, 未找到任何视频")
                
                logger.info(f"直接爬取完成: {len(processed_thumbnails) if 'processed_thumbnails' in locals() else 0} 个视频，耗时 {crawl_duration:.1f} 秒")
                
            except Exception as e:
                logger.error(f"直接爬取失败: {e}")
                yield event.plain_result(f"❌ {user_name}, 爬取失败: {str(e)}")
            
        except Exception as e:
            logger.error(f"启动91porn爬取失败: {e}")
            yield event.plain_result(f"❌ 启动爬取失败: {str(e)}")




    async def terminate(self):
        """可选择实现异步的插件销毁方法，当插件被卸载/停用时会调用。"""
        logger.info("91porn爬虫插件正在关闭...")
        
        # 关闭HTTP客户端
        if self.http_client:
            await self.http_client.close()
        
        # 清理临时文件
        try:
            if os.path.exists(self.temp_dir):
                shutil.rmtree(self.temp_dir)
                logger.info("临时文件清理完成")
        except Exception as e:
            logger.error(f"清理临时文件失败: {e}")
        
        self.executor.shutdown(wait=True)
        logger.info("91porn爬虫插件已关闭")