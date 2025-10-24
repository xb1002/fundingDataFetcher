import random
import time
from datetime import datetime, timedelta
from typing import Dict

import logging

from dataFetcher.binance_data_fetcher import BinanceDataFetcher
from dataFetcher.bitget_data_fetcher import BitgetDataFetcher
from dataFetcher.bybit_data_fetcher import BybitDataFetcher
from dataFetcher.data_fetcher_base import DataFetcherBase, DataType
from dataFetcher.okx_data_fetcher import OKXDataFetcher

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

EXCHANGE_CONFIGS: Dict[str, Dict] = {
    'binance': {
        'class': BinanceDataFetcher,
        'kwargs': {
            'base_url': "https://fapi.binance.com",
            'max_limits': {
                DataType.PRICE_INDEX: 1500,
                DataType.PRICE: 1500,
                DataType.FUNDING_RATE: 1000,
                DataType.PREMIUM_INDEX: 1500,
            },
        },
    },
    'bybit': {
        'class': BybitDataFetcher,
        'kwargs': {
            'base_url': "https://api.bybit.com",
            'max_limits': {
                DataType.PRICE_INDEX: 1000,
                DataType.PRICE: 1000,
                DataType.FUNDING_RATE: 200,
                DataType.PREMIUM_INDEX: 1000,
            },
        },
    },
    'okx': {
        'class': OKXDataFetcher,
        'kwargs': {
            'base_url': "https://www.okx.com",
            'max_limits': {
                DataType.PRICE_INDEX: 100,
                DataType.PRICE: 100,
                DataType.FUNDING_RATE: 100,
                DataType.PREMIUM_INDEX: 100,
            },
        },
    },
    'bitget': {
        'class': BitgetDataFetcher,
        'kwargs': {
            'base_url': "https://api.bitget.com",
            'max_limits': {
                DataType.PRICE_INDEX: 100,
                DataType.PRICE: 100,
                DataType.FUNDING_RATE: 100,
                DataType.PREMIUM_INDEX: 100,
            },
        },
    },
}


def get_date_range():
    """获取最近一个月的日期范围"""
    end_date = datetime.now()
    start_date = end_date - timedelta(days=30)

    return start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")


def init_fetchers(max_workers: int = 3, output_dir: str = "./data") -> Dict[str, DataFetcherBase]:
    fetchers: Dict[str, DataFetcherBase] = {}
    for name, config in EXCHANGE_CONFIGS.items():
        fetcher_cls = config['class']
        kwargs = config['kwargs']
        try:
            fetchers[name] = fetcher_cls(
                output_dir=output_dir,
                max_retries=3,
                timeout=30,
                max_workers=max_workers,
                **kwargs,
            )
            logger.info(f"{name.capitalize()}数据获取器初始化成功")
        except Exception as exc:
            logger.error(f"{name.capitalize()}数据获取器初始化失败: {exc}")
    return fetchers


def get_common_symbols(fetchers: Dict[str, DataFetcherBase]):
    """获取多个交易所的共同交易对"""
    symbol_sets = []
    for name, fetcher in fetchers.items():
        logger.info(f"正在获取{name.capitalize()}交易对列表...")
        symbols = set(fetcher.fetch_all_symbol())
        logger.info(f"{name.capitalize()}交易对数量: {len(symbols)}")
        if symbols:
            symbol_sets.append(symbols)

    if not symbol_sets:
        return []

    common_symbols = sorted(set.intersection(*symbol_sets))
    logger.info(f"共同交易对数量: {len(common_symbols)}")
    return common_symbols


def fetch_all_data_for_symbols(fetchers: Dict[str, DataFetcherBase], symbols, start_date, end_date, interval="1m"):
    """为指定的交易对获取所有类型的数据"""
    data_types = [
        DataType.PRICE,
        DataType.PRICE_INDEX,
        DataType.PREMIUM_INDEX,
        DataType.FUNDING_RATE,
    ]

    total_tasks = len(symbols) * len(data_types) * len(fetchers)
    completed_tasks = 0

    logger.info(f"开始获取数据，总任务数: {total_tasks}")
    logger.info(f"时间范围: {start_date} 到 {end_date}")
    logger.info(f"时间间隔: {interval}")

    for symbol in symbols:
        logger.info(f"正在处理交易对: {symbol}")

        for exchange, fetcher in fetchers.items():
            for data_type in data_types:
                try:
                    logger.info(f"  获取{exchange.capitalize()} {data_type.value}数据...")
                    data, from_cache = fetcher.fetch_data(
                        symbol=symbol,
                        start_date=start_date,
                        end_date=end_date,
                        interval=interval,
                        data_type=data_type,
                    )
                    completed_tasks += 1
                    logger.info(f"  ✓ {exchange.capitalize()} {data_type.value}数据获取完成 ({completed_tasks}/{total_tasks})")

                    if not from_cache:
                        wait_time = random.uniform(1, 3)
                        logger.info(f"  等待 {wait_time:.1f} 秒...")
                        time.sleep(wait_time)

                except Exception as e:
                    logger.error(f"  ✗ {exchange.capitalize()} {symbol} {data_type.value}数据获取失败: {e}")
                    completed_tasks += 1

    logger.info("所有数据获取完成！")


def main():
    """主函数"""
    try:
        start_date, end_date = get_date_range()
        logger.info(f"数据获取时间范围: {start_date} 到 {end_date}")

        fetchers = init_fetchers(max_workers=3, output_dir="./data")

        common_symbols = get_common_symbols(fetchers)

        if not common_symbols:
            logger.error("没有找到共同的交易对！")
            return

        logger.info(f"找到 {len(common_symbols)} 个共同交易对:")
        for i, symbol in enumerate(common_symbols):
            logger.info(f"  {i + 1}. {symbol}")

        fetch_all_data_for_symbols(fetchers, common_symbols, start_date, end_date, interval="1m")

        logger.info("脚本执行完成！")

    except Exception as e:
        logger.error(f"脚本执行出错: {e}")
        raise


if __name__ == "__main__":
    main()
