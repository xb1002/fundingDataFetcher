from __future__ import annotations

from abc import ABC, abstractmethod
import os
import time
import requests
import pandas as pd
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional, Tuple
import json
import logging
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from enum import Enum

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataType(Enum):
    PRICE_INDEX = "price_index"
    PRICE = "price"
    FUNDING_RATE = "funding_rate"
    PREMIUM_INDEX = "premium_index"

class DataFetcherBase(ABC):
    """
    数据获取器基类
    所有数据获取器都需要继承此基类并实现抽象方法
    """

    def __init__(self, base_url: str, max_limits: dict[DataType, int],
                 output_dir: str = "./data", max_retries: int = 3, timeout: int = 30, max_workers: int = 5):
        """
        初始化数据获取器
        
        Args:
            output_dir: 数据保存的根目录
            max_retries: 最大重试次数
            timeout: 请求超时时间（秒）
        """
        self.base_url = base_url
        self.max_limits = max_limits
        self.output_dir = output_dir
        self.max_retries = max_retries
        self.timeout = timeout
        self.max_workers = max_workers

        self.session = requests.Session()
        
        # 创建输出目录
        self._create_output_directories()
    
    def _create_output_directories(self):
        """创建数据保存目录结构"""
        exchange_name = self.get_exchange_name()
        directories = [
            f"{self.output_dir}/{exchange_name}/price_index",
            f"{self.output_dir}/{exchange_name}/price", 
            f"{self.output_dir}/{exchange_name}/funding_rate",
            f"{self.output_dir}/{exchange_name}/premium_index"
        ]
        
        for directory in directories:
            Path(directory).mkdir(parents=True, exist_ok=True)
            logger.info(f"创建目录: {directory}")
    
    @abstractmethod
    def get_exchange_name(self) -> str:
        """
        获取交易所名称
        
        Returns:
            str: 交易所名称（如 'binance', 'bybit'）
        """
        pass
    
    def date_to_timestamp(self, date_str: str, format_str: str = "%Y-%m-%d") -> int:
        """
        将日期字符串转换为时间戳（毫秒）
        
        Args:
            date_str: 日期字符串
            format_str: 日期格式，默认为 "%Y-%m-%d"
            
        Returns:
            int: 时间戳（毫秒）
        """
        try:
            dt = datetime.strptime(date_str, format_str)
            # 转换为UTC时间戳（毫秒）
            timestamp = int(dt.replace(tzinfo=timezone.utc).timestamp() * 1000)
            logger.debug(f"日期 {date_str} 转换为时间戳: {timestamp}")
            return timestamp
        except ValueError as e:
            logger.error(f"日期转换失败: {date_str}, 错误: {e}")
            raise
    
    def split_time_range(self, start_timestamp: int, end_timestamp: int, 
                        max_limit: int, interval_ms: int) -> List[Tuple[int, int]]:
        """
        将时间范围按照请求限制进行分割
        
        Args:
            start_timestamp: 开始时间戳（毫秒）
            end_timestamp: 结束时间戳（毫秒）
            max_limit: 单次请求最大数据条数
            interval_ms: 时间间隔（毫秒）
            
        Returns:
            List[Tuple[int, int]]: 时间范围列表，每个元素为(start, end)
        """
        time_ranges = []
        current_start = start_timestamp
        
        while current_start < end_timestamp:
            # 计算当前批次的结束时间
            current_end = min(
                current_start + (max_limit - 1) * interval_ms,
                end_timestamp
            )
            time_ranges.append((current_start, current_end))
            current_start = current_end + interval_ms
            
        logger.info(f"时间范围分割完成，共 {len(time_ranges)} 个批次")
        return time_ranges
    
    def make_request(self, url: str, params: Optional[Dict[str, Any]] = None, 
                    headers: Optional[Dict[str, str]] = None) -> Optional[Dict]:
        """
        发送HTTP请求（带重连机制）
        
        Args:
            url: 请求URL
            params: 请求参数
            headers: 请求头
            
        Returns:
            Optional[Dict]: 响应数据，失败时返回None
        """
        if params is None:
            params = {}
        if headers is None:
            headers = {}
            
        for attempt in range(self.max_retries + 1):
            try:
                logger.debug(f"发送请求 (尝试 {attempt + 1}/{self.max_retries + 1}): {url}")
                response = self.session.get(
                    url, 
                    params=params, 
                    headers=headers, 
                    timeout=self.timeout
                )
                response.raise_for_status()
                
                data = response.json()
                logger.debug(f"请求成功: {url}")
                return data
                
            except requests.exceptions.RequestException as e:
                logger.warning(f"请求失败 (尝试 {attempt + 1}/{self.max_retries + 1}): {e}")
                if attempt < self.max_retries:
                    time.sleep(2 ** attempt)  # 指数退避
                else:
                    logger.error(f"请求最终失败: {url}, 错误: {e}")
                    return None
            except json.JSONDecodeError as e:
                logger.error(f"JSON解析失败: {e}")
                return None

    def canonicalize_symbol(self, symbol: str) -> str:
        """将交易所特有的交易对格式转换为通用格式。

        默认返回原始值，子类可根据需要进行覆盖。
        """

        return symbol

    def translate_symbol(self, symbol: str) -> str:
        """将通用交易对格式转换为交易所实际使用的符号。

        默认返回原始值，子类可根据需要进行覆盖。
        """

        return symbol

    def save_data(self, data: pd.DataFrame, data_type: DataType, symbol: str, start: str,
                 end: str, interval: Optional[str] = None) -> str:
        """
        将数据保存到对应目录
        
        Args:
            data: 要保存的DataFrame数据
            data_type: 数据类型 ('price_index', 'price', 'funding_rate')
            symbol: 交易对符号
            start: 开始日期字符串
            end: 结束日期字符串
            interval: 时间间隔（对于K线数据）
            
        Returns:
            str: 保存的文件路径
        """
        exchange_name = self.get_exchange_name()

        # 构建文件名
        if data_type != DataType.FUNDING_RATE:
            filename = f"{symbol}_{start}_{end}_{interval}.csv"
        else:
            filename = f"{symbol}_{start}_{end}.csv"

        # 构建文件路径
        file_path = os.path.join(self.output_dir, exchange_name, data_type.value, filename)

        data_to_save = data.copy()
        if "timestamp" not in data_to_save.columns:
            data_to_save = data_to_save.reset_index()
        if "timestamp" in data_to_save.columns:
            data_to_save["timestamp"] = pd.to_datetime(data_to_save["timestamp"])

        data_to_save.to_csv(file_path, index=False)
        logger.info(f"数据已保存: {file_path}")
        
        return file_path
    
    def get_interval_milliseconds(self, interval: str) -> int:
        """
        获取时间间隔对应的毫秒数
        
        Args:
            interval: 时间间隔字符串（如 '1m', '1h', '1d'）
            
        Returns:
            int: 毫秒数
        """
        interval_map = {
            '1m': 60 * 1000,
            '3m': 3 * 60 * 1000,
            '5m': 5 * 60 * 1000,
            '15m': 15 * 60 * 1000,
            '30m': 30 * 60 * 1000,
            '1h': 60 * 60 * 1000,
            '2h': 2 * 60 * 60 * 1000,
            '4h': 4 * 60 * 60 * 1000,
            '6h': 6 * 60 * 60 * 1000,
            '8h': 8 * 60 * 60 * 1000,
            '12h': 12 * 60 * 60 * 1000,
            '1d': 24 * 60 * 60 * 1000,
            '3d': 3 * 24 * 60 * 60 * 1000,
            '1w': 7 * 24 * 60 * 60 * 1000,
            '1M': 30 * 24 * 60 * 60 * 1000  # 近似值
        }
        
        if interval not in interval_map:
            raise ValueError(f"不支持的时间间隔: {interval}")
            
        return interval_map[interval]
    
    # 获取价格指数
    @abstractmethod
    def _fetch_price_index_data(self, symbol: str, start_timestamp: int, 
                            end_timestamp: int, interval: str) -> pd.DataFrame:
        """
        获取现货价格指数数据
        注意，这里需要返回的dateframe需要包含索引，最好使用时间戳
        
        Args:
            symbol: 交易对符号
            start_timestamp: 开始时间戳
            end_timestamp: 结束时间戳
            interval: 时间间隔
            
        Returns:
            pd.DataFrame: 价格指数数据
        """
        pass

    def fetch_data(self, symbol: str, start_date: str,
                   end_date: str, interval: str, data_type: DataType) -> tuple[pd.DataFrame, bool]:
        """
        通过多线程调用_fetch_price_index_data获取指定时间范围的数据

        Args:
            symbol: 交易对符号
            start_date: 开始日期字符串
            end_date: 结束日期字符串
            interval: 时间间隔

        Returns:
            pd.DataFrame: 价格指数数据
        """
        from_cache = False
        canonical_symbol = self.canonicalize_symbol(symbol)
        # 如果数据已经存在，则不再获取
        exchange_name = self.get_exchange_name()
        if data_type != DataType.FUNDING_RATE:
            filename = f"{canonical_symbol}_{start_date}_{end_date}_{interval}.csv"
        else:
            filename = f"{canonical_symbol}_{start_date}_{end_date}.csv"
        file_path = os.path.join(self.output_dir, exchange_name, data_type.value, filename)
        if os.path.exists(file_path):
            logger.info(f"数据文件已存在，跳过获取: {file_path}")
            from_cache = True
            cached = pd.read_csv(file_path)
            if "timestamp" in cached.columns:
                cached["timestamp"] = pd.to_datetime(cached["timestamp"])
                cached.set_index("timestamp", inplace=True)
                cached.sort_index(inplace=True)
            return cached, from_cache

        # 如果是获取资费，则将interval_ms设置为1小时的
        if data_type == DataType.FUNDING_RATE:
            interval_ms = self.get_interval_milliseconds("1h")
        else:
            interval_ms = self.get_interval_milliseconds(interval)
        time_ranges = self.split_time_range(
            self.date_to_timestamp(start_date),
            self.date_to_timestamp(end_date),
            max_limit=self.max_limits[data_type],
            interval_ms=interval_ms
        )

        exec_func = None
        if data_type == DataType.PRICE_INDEX:
            exec_func = self._fetch_price_index_data
        elif data_type == DataType.PRICE:
            exec_func = self._fetch_price_data
        elif data_type == DataType.FUNDING_RATE:
            exec_func = self._fetch_funding_rate_data
        elif data_type == DataType.PREMIUM_INDEX:
            exec_func = self._fetch_premium_index_data
        else:
            raise ValueError(f"不支持的数据类型: {data_type}")

        api_symbol = self.translate_symbol(symbol)

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = []
            for start, end in time_ranges:
                futures.append(executor.submit(exec_func, api_symbol, start, end, interval))

            results = []
            for future in as_completed(futures):
                try:
                    data = future.result()
                    results.append(data)
                except Exception as e:
                    logger.error(f"获取价格指数数据失败: {e}")

        if not results:
            return pd.DataFrame(), from_cache

        # 合并所有结果
        res = pd.concat(results)
        # 去重
        if not res.index.is_unique:
            res = res[~res.index.duplicated(keep="last")]
        res = res.sort_index()
        # 保存
        self.save_data(res, data_type, canonical_symbol, start_date, end_date, interval)
        return res, from_cache

    # 获取价格
    @abstractmethod
    def _fetch_price_data(self, symbol: str, start_timestamp: int, 
                          end_timestamp: int, interval: str) -> pd.DataFrame:
        """
        获取现货价格数据
        注意，这里需要返回的dateframe需要包含索引，最好使用时间戳
        
        Args:
            symbol: 交易对符号
            start_timestamp: 开始时间戳
            end_timestamp: 结束时间戳
            interval: 时间间隔
            
        Returns:
            pd.DataFrame: 价格数据
        """
        pass
    
    @abstractmethod
    def _fetch_funding_rate_data(self, symbol: str, start_timestamp: int, 
                                 end_timestamp: int, interval: Optional[str]=None) -> pd.DataFrame:
        """
        获取资金费率数据
        
        Args:
            symbol: 交易对符号
            start_timestamp: 开始时间戳
            end_timestamp: 结束时间戳
            
        Returns:
            pd.DataFrame: 资金费率数据
        """
        pass

    @abstractmethod
    def _fetch_premium_index_data(self, symbol: str, start_timestamp: int, 
                                 end_timestamp: int, interval: str) -> pd.DataFrame:
        """
        获取溢价指数数据
        
        Args:
            symbol: 交易对符号
            start_timestamp: 开始时间戳
            end_timestamp: 结束时间戳
            
        Returns:
            pd.DataFrame: 溢价指数数据
        """
        pass