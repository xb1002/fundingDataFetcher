from .data_fetcher_base import DataFetcherBase, DataType
import pandas as pd
from datetime import datetime
import logging
from typing import Optional

logger = logging.getLogger(__name__)


class BybitDataFetcher(DataFetcherBase):

    def get_exchange_name(self) -> str:
        return "bybit"

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
        end_point = "/v5/market/index-price-kline"
        url = f"{self.base_url}{end_point}"
        interval = interval.rstrip("m")  # 确保时间间隔格式正确
        params = {
            "symbol": symbol,
            "interval": interval,
            "start": start_timestamp,
            "end": end_timestamp,
            "limit": self.max_limits[DataType.PRICE_INDEX]

        }
        columns = ["open_time", "open", "high", "low", "close"]
        response = self.make_request(url, params=params)
        if response and response["retMsg"] == "OK":
            if response["result"]["list"]:
                data = response["result"]["list"]
                df = pd.DataFrame(data, columns=columns)
                df["timestamp"] = pd.to_datetime(df["open_time"].astype("int64"), unit="ms")
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
        end_point = "/v5/market/kline"
        url = f"{self.base_url}{end_point}"
        interval = interval.rstrip("m")  # 确保时间间隔格式正确
        params = {
            "symbol": symbol,
            "interval": interval,
            "start": start_timestamp,
            "end": end_timestamp,
            "limit": self.max_limits[DataType.PRICE]
        }
        columns = ["open_time","open","high","low","close","volume",
                   "turnover"]
        response = self.make_request(url, params=params)
        if response and response["retMsg"] == "OK":
            if response["result"]["list"]:
                data = response["result"]["list"]
                df = pd.DataFrame(data, columns=columns)
                df["timestamp"] = pd.to_datetime(df["open_time"].astype("int64"), unit="ms")
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
        end_point = "/v5/market/funding/history"
        url = f"{self.base_url}{end_point}"
        params = {
            "category": "linear",
            "symbol": symbol,
            "start": start_timestamp,
            "end": end_timestamp,
            "limit": self.max_limits[DataType.FUNDING_RATE]
        }
        columns = ["symbol","fundingRate","fundingRateTimestamp"]
        response = self.make_request(url, params=params)
        if response and response["retMsg"] == "OK":
            if response["result"]["list"]:
                data = response["result"]["list"]
                df = pd.DataFrame(data, columns=columns)
                # 将fundingRateTimestamp重命名为fundingTime
                df.rename(columns={"fundingRateTimestamp": "fundingTime"}, inplace=True)
                # 将fundingTime转换为datetime格式并向下取整到分钟
                df["fundingTime"] = pd.to_datetime(df["fundingTime"].astype("int64"), unit="ms")
                df["fundingTime"] = df["fundingTime"].dt.floor("min")
                df["timestamp"] = df["fundingTime"]
                df.set_index("timestamp", inplace=True)
                df.sort_index(inplace=True)
                return df
        return pd.DataFrame()
    
    def _fetch_premium_index_data(self, symbol: str, start_timestamp: int, end_timestamp: int, interval: str) -> pd.DataFrame:
        """
        获取永续合约溢价指数数据

        Args:
            symbol: 交易对符号
            start_timestamp: 开始时间戳
            end_timestamp: 结束时间戳
            interval: 时间间隔

        Returns:
            pd.DataFrame: 溢价指数数据
        """
        end_point = "/v5/market/premium-index-price-kline"
        url = f"{self.base_url}{end_point}"
        interval = interval.rstrip("m")  # 确保时间间隔格式正确
        params = {
            "symbol": symbol,
            "interval": interval,
            "start": start_timestamp,
            "end": end_timestamp,
            "limit": self.max_limits[DataType.PREMIUM_INDEX]

        }
        columns = ["open_time", "open", "high", "low", "close"]
        response = self.make_request(url, params=params)
        if response and response["retMsg"] == "OK":
            if response["result"]["list"]:
                data = response["result"]["list"]
                df = pd.DataFrame(data, columns=columns)
                df["timestamp"] = pd.to_datetime(df["open_time"].astype("int64"), unit="ms")
                df.set_index("timestamp", inplace=True)
                return df
        return pd.DataFrame()
    
    def fetch_all_symbol(self) -> list[str]:
        """
        获取所有交易对符号

        Returns:
            list[str]: 交易对符号列表
        """
        end_point = "/v5/market/instruments-info"
        url = f"{self.base_url}{end_point}"
        params = {
            "category": "linear"
        }
        response = self.make_request(url, params=params)
        symbols = []
        if response and response["retMsg"] == "OK":
            if response["result"]["list"]:
                data = response["result"]["list"]
                symbols = [item["symbol"] for item in data]
                symbols = [s for s in symbols if s.endswith("USDT")]
        return symbols

if __name__ == "__main__":
    max_limits = {
        DataType.PRICE_INDEX: 1000,
        DataType.PRICE: 1000,
        DataType.FUNDING_RATE: 200,
        DataType.PREMIUM_INDEX: 1000
    }
    fetcher = BybitDataFetcher(
        base_url="https://api.bybit.com",
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

    # all_symbols = fetcher.fetch_all_symbol()
    # print(all_symbols)

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

    premium_index_data = fetcher.fetch_data(
        symbol=symbol,
        start_date=start_date,
        end_date=end_date,
        interval=interval,
        data_type=DataType.PREMIUM_INDEX
    )
    print(premium_index_data)