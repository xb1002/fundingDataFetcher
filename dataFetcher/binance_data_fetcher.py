from dataFetcher.data_fetcher_base import DataFetcherBase, DataType
import pandas as pd
from datetime import datetime
import logging
from typing import Optional

logger = logging.getLogger(__name__)


class BinanceDataFetcher(DataFetcherBase):

    def get_exchange_name(self) -> str:
        return "binance"

    def _fetch_price_index_data(self, symbol: str, start_timestamp: int,
                                 end_timestamp: int, interval: str) -> pd.DataFrame:
        """
        获取现货价格指数数据

        Args:
            symbol: 交易对符号
            start_timestamp: 开始时间戳
            end_timestamp: 结束时间戳
            interval: 时间间隔

        Returns:
            pd.DataFrame: 价格指数数据
        """
        end_point = "/fapi/v1/indexPriceKlines"
        url = f"{self.base_url}{end_point}"
        params = {
            "pair": symbol,
            "interval": interval,
            "startTime": start_timestamp,
            "endTime": end_timestamp,
            "limit": self.max_limits[DataType.PRICE_INDEX]

        }
        data = self.make_request(url, params=params)
        if data:
            df = pd.DataFrame(data, columns=["open_time", "open", "high", "low", "close", "volume",
                                              "close_time", "quote_asset_volume", "number_of_trades",
                                              "taker_buy_base_asset_volume", "taker_buy_quote_asset_volume", "ignore"])
            df["timestamp"] = pd.to_datetime(df["open_time"], unit="ms")
            df.set_index("timestamp", inplace=True)
            return df
        return pd.DataFrame()
    
    def _fetch_price_data(self, symbol: str, start_timestamp: int, end_timestamp: int, interval: str) -> pd.DataFrame:
        """
        获取价格数据

        Args:
            symbol: 交易对符号
            start_timestamp: 开始时间戳
            end_timestamp: 结束时间戳
            interval: 时间间隔

        Returns:
            pd.DataFrame: 价格数据
        """
        end_point = "/fapi/v1/klines"
        url = f"{self.base_url}{end_point}"
        params = {
            "symbol": symbol,
            "interval": interval,
            "startTime": start_timestamp,
            "endTime": end_timestamp,
            "limit": self.max_limits[DataType.PRICE]
        }
        data = self.make_request(url, params=params)
        if data:
            df = pd.DataFrame(data, columns=["open_time", "open", "high", "low", "close", "volume",
                                              "close_time", "quote_asset_volume", "number_of_trades",
                                              "taker_buy_base_asset_volume", "taker_buy_quote_asset_volume", "ignore"])
            df["timestamp"] = pd.to_datetime(df["open_time"], unit="ms")
            df.set_index("timestamp", inplace=True)
            return df
        return pd.DataFrame()

    def _fetch_funding_rate_data(self, symbol: str, start_timestamp: int, end_timestamp: int, interval: str | None = None) -> pd.DataFrame:
        """
        获取资金费率数据

        Args:
            symbol: 交易对符号
            start_timestamp: 开始时间戳
            end_timestamp: 结束时间戳
            interval: 时间间隔

        Returns:
            pd.DataFrame: 资金费率数据
        """
        end_point = "/fapi/v1/fundingRate"
        url = f"{self.base_url}{end_point}"
        params = {
            "symbol": symbol,
            "startTime": start_timestamp,
            "endTime": end_timestamp,
            "limit": self.max_limits[DataType.FUNDING_RATE]
        }
        data = self.make_request(url, params=params)
        if data:
            df = pd.DataFrame(data)
            # df["fundingTime"] 向下取整到分钟
            df["fundingTime"] = pd.to_datetime(df["fundingTime"], unit="ms")
            df["fundingTime"] = df["fundingTime"].dt.floor("T")
            df["timestamp"] = df["fundingTime"]
            df.set_index("timestamp", inplace=True)
            return df
        return pd.DataFrame()

if __name__ == "__main__":
    max_limits = {
        DataType.PRICE_INDEX: 1500,
        DataType.PRICE: 1500,
        DataType.FUNDING_RATE: 1000
    }
    fetcher = BinanceDataFetcher(
        base_url="https://fapi.binance.com",
        max_limits=max_limits,
        output_dir="./data",
        max_retries=3,
        timeout=30,
        max_workers=5
    )

    # 获取数据
    symbol = "BTCUSDT"
    start_date = "2025-08-01"
    end_date = "2025-08-17"
    interval = "1m"

    price_index_data = fetcher.fetch_data(
        symbol=symbol,
        start_date=start_date,
        end_date=end_date,
        interval=interval,
        data_type=DataType.PRICE_INDEX
    )
    print(price_index_data)

    price_data = fetcher.fetch_data(
        symbol=symbol,
        start_date=start_date,
        end_date=end_date,
        interval=interval,
        data_type=DataType.PRICE
    )
    print(price_data)

    funding_rate_data = fetcher.fetch_data(
        symbol=symbol,
        start_date=start_date,
        end_date=end_date,
        interval=interval,
        data_type=DataType.FUNDING_RATE
    )
    print(funding_rate_data)