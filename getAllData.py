import sys
import os
import time
import random
from datetime import datetime, timedelta
from dataFetcher.binance_data_fetcher import BinanceDataFetcher
from dataFetcher.bybit_data_fetcher import BybitDataFetcher
from dataFetcher.data_fetcher_base import DataType
import logging

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def get_date_range():
    """
    获取最近一个月的日期范围
    
    Returns:
        tuple: (start_date, end_date) 格式为 "YYYY-MM-DD"
    """
    end_date = datetime.now()
    start_date = end_date - timedelta(days=30)
    
    return start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")


def get_common_symbols(binance_fetcher, bybit_fetcher):
    """
    获取两个交易所的共同交易对
    
    Args:
        binance_fetcher: Binance数据获取器
        bybit_fetcher: Bybit数据获取器
        
    Returns:
        list: 共同的交易对列表
    """
    logger.info("正在获取Binance交易对列表...")
    binance_symbols = set(binance_fetcher.fetch_all_symbol())
    logger.info(f"Binance交易对数量: {len(binance_symbols)}")
    
    logger.info("正在获取Bybit交易对列表...")
    bybit_symbols = set(bybit_fetcher.fetch_all_symbol())
    logger.info(f"Bybit交易对数量: {len(bybit_symbols)}")
    
    # 计算交集
    common_symbols = list(binance_symbols.intersection(bybit_symbols))
    logger.info(f"共同交易对数量: {len(common_symbols)}")
    
    return sorted(common_symbols)


def fetch_all_data_for_symbols(symbols, start_date, end_date, interval="1m"):
    """
    为指定的交易对获取所有类型的数据
    
    Args:
        symbols: 交易对列表
        start_date: 开始日期
        end_date: 结束日期
        interval: 时间间隔，默认为1分钟
    """
    # 配置各交易所的限制参数
    binance_max_limits = {
        DataType.PRICE_INDEX: 1500,
        DataType.PRICE: 1500,
        DataType.FUNDING_RATE: 1000,
        DataType.PREMIUM_INDEX: 1500
    }
    
    bybit_max_limits = {
        DataType.PRICE_INDEX: 1000,
        DataType.PRICE: 1000,
        DataType.FUNDING_RATE: 200,
        DataType.PREMIUM_INDEX: 1000
    }
    
    # 初始化数据获取器
    binance_fetcher = BinanceDataFetcher(
        base_url="https://fapi.binance.com",
        max_limits=binance_max_limits,
        output_dir="./data",
        max_retries=3,
        timeout=30,
        max_workers=3  # 降低并发数避免频率限制
    )
    
    bybit_fetcher = BybitDataFetcher(
        base_url="https://api.bybit.com",
        max_limits=bybit_max_limits,
        output_dir="./data",
        max_retries=3,
        timeout=30,
        max_workers=3  # 降低并发数避免频率限制
    )
    
    data_types = [
        DataType.PRICE,
        DataType.PRICE_INDEX,
        DataType.PREMIUM_INDEX,
        DataType.FUNDING_RATE
    ]
    
    total_tasks = len(symbols) * len(data_types) * 2  # 2个交易所
    completed_tasks = 0
    
    logger.info(f"开始获取数据，总任务数: {total_tasks}")
    logger.info(f"时间范围: {start_date} 到 {end_date}")
    logger.info(f"时间间隔: {interval}")
    
    for symbol in symbols:
        logger.info(f"正在处理交易对: {symbol}")
        
        for data_type in data_types:
            # Binance数据
            try:
                logger.info(f"  获取Binance {data_type.value}数据...")
                binance_data = binance_fetcher.fetch_data(
                    symbol=symbol,
                    start_date=start_date,
                    end_date=end_date,
                    interval=interval,
                    data_type=data_type
                )
                completed_tasks += 1
                logger.info(f"  ✓ Binance {data_type.value}数据获取完成 ({completed_tasks}/{total_tasks})")
                
                # 添加随机等待时间0.5-1
                wait_time = random.uniform(1, 3)
                logger.info(f"  等待 {wait_time:.1f} 秒...")
                time.sleep(wait_time)
                
            except Exception as e:
                logger.error(f"  ✗ Binance {symbol} {data_type.value}数据获取失败: {e}")
                completed_tasks += 1
            
            # Bybit数据
            try:
                logger.info(f"  获取Bybit {data_type.value}数据...")
                bybit_data = bybit_fetcher.fetch_data(
                    symbol=symbol,
                    start_date=start_date,
                    end_date=end_date,
                    interval=interval,
                    data_type=data_type
                )
                completed_tasks += 1
                logger.info(f"  ✓ Bybit {data_type.value}数据获取完成 ({completed_tasks}/{total_tasks})")
                
                # 添加随机等待时间
                wait_time = random.uniform(1, 3)
                logger.info(f"  等待 {wait_time:.1f} 秒...")
                time.sleep(wait_time)
                
            except Exception as e:
                logger.error(f"  ✗ Bybit {symbol} {data_type.value}数据获取失败: {e}")
                completed_tasks += 1
    
    logger.info("所有数据获取完成！")


def main():
    """
    主函数
    """
    try:
        # 获取日期范围
        start_date, end_date = get_date_range()
        logger.info(f"数据获取时间范围: {start_date} 到 {end_date}")
        
        # 配置各交易所的限制参数
        binance_max_limits = {
            DataType.PRICE_INDEX: 1500,
            DataType.PRICE: 1500,
            DataType.FUNDING_RATE: 1000,
            DataType.PREMIUM_INDEX: 1500
        }
        
        bybit_max_limits = {
            DataType.PRICE_INDEX: 1000,
            DataType.PRICE: 1000,
            DataType.FUNDING_RATE: 200,
            DataType.PREMIUM_INDEX: 1000
        }
        
        # 初始化数据获取器（仅用于获取交易对列表）
        binance_fetcher = BinanceDataFetcher(
            base_url="https://fapi.binance.com",
            max_limits=binance_max_limits,
            output_dir="./data",
            max_retries=3,
            timeout=30,
            max_workers=3  # 降低并发数避免频率限制
        )
        
        bybit_fetcher = BybitDataFetcher(
            base_url="https://api.bybit.com",
            max_limits=bybit_max_limits,
            output_dir="./data",
            max_retries=3,
            timeout=30,
            max_workers=3  # 降低并发数避免频率限制
        )
        
        # 获取共同交易对
        common_symbols = get_common_symbols(binance_fetcher, bybit_fetcher)
        
        if not common_symbols:
            logger.error("没有找到共同的交易对！")
            return
        
        logger.info(f"找到 {len(common_symbols)} 个共同交易对:")
        for i, symbol in enumerate(common_symbols):
            logger.info(f"  {i+1}. {symbol}")
        
        # # 为了演示，我们只获取前10个交易对的数据
        # # 如果要获取所有交易对，请移除这个限制
        # demo_symbols = common_symbols[:10]
        # logger.info(f"演示模式：只获取前 {len(demo_symbols)} 个交易对的数据")
        
        # # 获取所有数据
        # fetch_all_data_for_symbols(demo_symbols, start_date, end_date, interval="1m")
        
        # 获取所有数据
        fetch_all_data_for_symbols(common_symbols, start_date, end_date, interval="1m")
        
        logger.info("脚本执行完成！")
        
    except Exception as e:
        logger.error(f"脚本执行出错: {e}")
        raise


if __name__ == "__main__":
    main()
