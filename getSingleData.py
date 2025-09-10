#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
单个交易对数据获取器
支持从外部获取参数，获取两个交易所的单个symbol数据，允许指定时间范围
"""

import argparse
import sys
import os
from datetime import datetime, timedelta
import pandas as pd
import logging
from typing import Dict, Any, List, Optional

# 添加项目根目录到Python路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from dataFetcher.binance_data_fetcher import BinanceDataFetcher
from dataFetcher.bybit_data_fetcher import BybitDataFetcher
from dataFetcher.data_fetcher_base import DataType

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('data_fetcher.log', encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)


class SingleSymbolDataFetcher:
    """单个交易对数据获取器"""
    
    def __init__(self, output_dir: str = "./data", max_workers: int = 5):
        """
        初始化数据获取器
        
        Args:
            output_dir: 数据保存目录
            max_workers: 最大并发线程数
        """
        self.output_dir = output_dir
        self.max_workers = max_workers
        
        # 初始化交易所数据获取器
        self.fetchers = self._init_fetchers()
        
    def _init_fetchers(self) -> Dict[str, Any]:
        """初始化各交易所的数据获取器"""
        fetchers = {}
        
        # Binance配置
        binance_max_limits = {
            DataType.PRICE_INDEX: 1500,
            DataType.PRICE: 1500,
            DataType.FUNDING_RATE: 1000,
            DataType.PREMIUM_INDEX: 1500
        }
        
        # Bybit配置
        bybit_max_limits = {
            DataType.PRICE_INDEX: 1000,
            DataType.PRICE: 1000,
            DataType.FUNDING_RATE: 200,
            DataType.PREMIUM_INDEX: 1000
        }
        
        try:
            fetchers['binance'] = BinanceDataFetcher(
                base_url="https://fapi.binance.com",
                max_limits=binance_max_limits,
                output_dir=self.output_dir,
                max_retries=3,
                timeout=30,
                max_workers=self.max_workers
            )
            logger.info("Binance数据获取器初始化成功")
        except Exception as e:
            logger.error(f"Binance数据获取器初始化失败: {e}")
            
        try:
            fetchers['bybit'] = BybitDataFetcher(
                base_url="https://api.bybit.com",
                max_limits=bybit_max_limits,
                output_dir=self.output_dir,
                max_retries=3,
                timeout=30,
                max_workers=self.max_workers
            )
            logger.info("Bybit数据获取器初始化成功")
        except Exception as e:
            logger.error(f"Bybit数据获取器初始化失败: {e}")
            
        return fetchers
    
    def get_available_exchanges(self) -> List[str]:
        """获取可用的交易所列表"""
        return list(self.fetchers.keys())
    
    def get_available_data_types(self) -> List[str]:
        """获取可用的数据类型列表"""
        return [dt.value for dt in DataType]
    
    def fetch_single_symbol_data(self, 
                                symbol: str,
                                start_date: str,
                                end_date: str,
                                exchanges: List[str],
                                data_types: List[str],
                                interval: str = "1m") -> Dict[str, Dict[str, pd.DataFrame]]:
        """
        获取单个交易对的数据
        
        Args:
            symbol: 交易对符号（如 "BTCUSDT"）
            start_date: 开始日期 (格式: "YYYY-MM-DD")
            end_date: 结束日期 (格式: "YYYY-MM-DD")
            exchanges: 交易所列表 (如 ["binance", "bybit"])
            data_types: 数据类型列表 (如 ["price", "funding_rate"])
            interval: 时间间隔 (如 "1m", "5m", "1h")
            
        Returns:
            Dict[str, Dict[str, pd.DataFrame]]: 嵌套字典，结构为 {exchange: {data_type: dataframe}}
        """
        results = {}
        
        # 验证输入参数
        self._validate_inputs(symbol, start_date, end_date, exchanges, data_types, interval)
        
        logger.info(f"开始获取 {symbol} 数据，时间范围: {start_date} - {end_date}")
        logger.info(f"交易所: {exchanges}, 数据类型: {data_types}, 时间间隔: {interval}")
        
        for exchange in exchanges:
            if exchange not in self.fetchers:
                logger.warning(f"交易所 {exchange} 不可用，跳过")
                continue
                
            results[exchange] = {}
            fetcher = self.fetchers[exchange]
            
            for data_type_str in data_types:
                try:
                    data_type = DataType(data_type_str)
                    logger.info(f"获取 {exchange} 的 {data_type_str} 数据...")
                    
                    data, from_cache = fetcher.fetch_data(
                        symbol=symbol,
                        start_date=start_date,
                        end_date=end_date,
                        interval=interval,
                        data_type=data_type
                    )
                    
                    if not data.empty:
                        results[exchange][data_type_str] = data
                        cache_info = "（来自缓存）" if from_cache else "（新获取）"
                        logger.info(f"成功获取 {exchange} 的 {data_type_str} 数据 {cache_info}，共 {len(data)} 条记录")
                    else:
                        logger.warning(f"未获取到 {exchange} 的 {data_type_str} 数据")
                        
                except Exception as e:
                    logger.error(f"获取 {exchange} 的 {data_type_str} 数据失败: {e}")
                    
        return results
    
    def _validate_inputs(self, symbol: str, start_date: str, end_date: str, 
                        exchanges: List[str], data_types: List[str], interval: str):
        """验证输入参数"""
        
        # 验证日期格式
        try:
            start_dt = datetime.strptime(start_date, "%Y-%m-%d")
            end_dt = datetime.strptime(end_date, "%Y-%m-%d")
            if start_dt >= end_dt:
                raise ValueError("开始日期必须早于结束日期")
        except ValueError as e:
            raise ValueError(f"日期格式错误: {e}")
        
        # 验证交易所
        available_exchanges = self.get_available_exchanges()
        for exchange in exchanges:
            if exchange not in available_exchanges:
                raise ValueError(f"不支持的交易所: {exchange}，可用交易所: {available_exchanges}")
        
        # 验证数据类型
        available_data_types = self.get_available_data_types()
        for data_type in data_types:
            if data_type not in available_data_types:
                raise ValueError(f"不支持的数据类型: {data_type}，可用数据类型: {available_data_types}")
        
        # 验证时间间隔
        valid_intervals = ['1m', '3m', '5m', '15m', '30m', '1h', '2h', '4h', '6h', '8h', '12h', '1d', '3d', '1w', '1M']
        if interval not in valid_intervals:
            raise ValueError(f"不支持的时间间隔: {interval}，可用间隔: {valid_intervals}")

def parse_arguments():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(
        description="单个交易对数据获取器 - 支持多交易所数据获取和比较",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用示例:
  # 获取BTCUSDT的价格和资金费率数据（默认所有交易所）
  python getSingleData.py BTCUSDT 2025-08-01 2025-08-10 --data-types price funding_rate
  
  # 只获取Binance的数据
  python getSingleData.py BTCUSDT 2025-08-01 2025-08-10 --exchanges binance --data-types price
  
  # 获取5分钟间隔的数据
  python getSingleData.py BTCUSDT 2025-08-01 2025-08-10 --interval 5m --data-types price premium_index
        """
    )
    
    parser.add_argument("symbol", help="交易对符号（如 BTCUSDT）")
    parser.add_argument("start_date", help="开始日期（格式: YYYY-MM-DD）")
    parser.add_argument("end_date", help="结束日期（格式: YYYY-MM-DD）")
    
    parser.add_argument(
        "--exchanges", 
        nargs="+", 
        choices=["binance", "bybit"],
        default=["binance", "bybit"],
        help="指定交易所（默认: 所有交易所）"
    )
    
    parser.add_argument(
        "--data-types",
        nargs="+",
        choices=["price_index", "price", "funding_rate", "premium_index"],
        default=["price_index", "price", "funding_rate", "premium_index"],
        help="指定数据类型（默认: price_index price funding_rate premium_index）"
    )
    
    parser.add_argument(
        "--interval",
        default="1m",
        choices=['1m', '3m', '5m', '15m', '30m', '1h', '2h', '4h', '6h', '8h', '12h', '1d', '3d', '1w', '1M'],
        help="时间间隔（默认: 1m）"
    )
    
    parser.add_argument(
        "--output-dir",
        default="./data",
        help="数据保存目录（默认: ./data）"
    )
    
    parser.add_argument(
        "--max-workers",
        type=int,
        default=5,
        help="最大并发线程数（默认: 5）"
    )
    
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="显示详细日志"
    )
    
    return parser.parse_args()


def main():
    """主函数"""
    args = parse_arguments()
    
    # 设置日志级别
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    try:
        # 初始化数据获取器
        fetcher = SingleSymbolDataFetcher(
            output_dir=args.output_dir,
            max_workers=args.max_workers
        )
        
        # 显示配置信息
        logger.info("=" * 50)
        logger.info("单个交易对数据获取器")
        logger.info("=" * 50)
        logger.info(f"交易对: {args.symbol}")
        logger.info(f"时间范围: {args.start_date} - {args.end_date}")
        logger.info(f"交易所: {args.exchanges}")
        logger.info(f"数据类型: {args.data_types}")
        logger.info(f"时间间隔: {args.interval}")
        logger.info(f"输出目录: {args.output_dir}")
        logger.info("=" * 50)
        
        # 获取数据
        results = fetcher.fetch_single_symbol_data(
            symbol=args.symbol,
            start_date=args.start_date,
            end_date=args.end_date,
            exchanges=args.exchanges,
            data_types=args.data_types,
            interval=args.interval
        )
        
        # 显示结果摘要
        logger.info("\n" + "=" * 50)
        logger.info("数据获取结果摘要")
        logger.info("=" * 50)
        
        for exchange, data_dict in results.items():
            logger.info(f"\n{exchange.upper()}:")
            for data_type, df in data_dict.items():
                logger.info(f"  {data_type}: {len(df)} 条记录")
                if not df.empty:
                    logger.info(f"    时间范围: {df.index.min()} - {df.index.max()}")
        
        logger.info("\n" + "=" * 50)
        logger.info("数据获取完成!")
        logger.info("=" * 50)
        
    except Exception as e:
        logger.error(f"程序执行失败: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
