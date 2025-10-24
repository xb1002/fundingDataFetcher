from __future__ import annotations

from typing import Optional

import pandas as pd

from .data_fetcher_base import DataFetcherBase, DataType


class BitgetDataFetcher(DataFetcherBase):
    """Bitget 数据获取器"""

    _INTERVAL_MAP = {
        "1m": 60,
        "3m": 180,
        "5m": 300,
        "15m": 900,
        "30m": 1800,
        "1h": 3600,
        "2h": 7200,
        "4h": 14400,
        "6h": 21600,
        "8h": 28800,
        "12h": 43200,
        "1d": 86400,
        "3d": 259200,
        "1w": 604800,
        "1M": 2592000,
    }

    def __init__(self, *args, product_type: str = "umcbl", **kwargs):
        super().__init__(*args, **kwargs)
        self.product_type = product_type

    def get_exchange_name(self) -> str:
        return "bitget"

    def _convert_interval(self, interval: str) -> int:
        try:
            return self._INTERVAL_MAP[interval]
        except KeyError as exc:
            raise ValueError(f"Unsupported interval for Bitget: {interval}") from exc

    def _normalise_candles(self, data: list[list[str]], columns: list[str]) -> pd.DataFrame:
        df = pd.DataFrame(data, columns=columns)
        df["timestamp"] = pd.to_datetime(df[columns[0]].astype("int64"), unit="ms")
        df.set_index("timestamp", inplace=True)
        return df

    def _fetch_price_index_data(
        self,
        symbol: str,
        start_timestamp: int,
        end_timestamp: int,
        interval: str,
    ) -> pd.DataFrame:
        # Bitget 没有单独的指数K线，使用标记价格替代
        return self._fetch_premium_index_data(symbol, start_timestamp, end_timestamp, interval)

    def _fetch_price_data(
        self,
        symbol: str,
        start_timestamp: int,
        end_timestamp: int,
        interval: str,
    ) -> pd.DataFrame:
        end_point = "/api/mix/v1/market/candles"
        url = f"{self.base_url}{end_point}"
        params = {
            "symbol": symbol,
            "granularity": self._convert_interval(interval),
            "startTime": start_timestamp,
            "endTime": end_timestamp,
            "productType": self.product_type,
            "limit": self.max_limits[DataType.PRICE],
        }
        response = self.make_request(url, params=params)
        if response and response.get("msg") == "success":
            data = response.get("data", [])
            if data:
                columns = [
                    "ts",
                    "open",
                    "high",
                    "low",
                    "close",
                    "volume",
                    "quote_volume",
                ]
                df = self._normalise_candles(data, columns)
                return df
        return pd.DataFrame()

    def _fetch_funding_rate_data(
        self,
        symbol: str,
        start_timestamp: int,
        end_timestamp: int,
        interval: Optional[str] = None,
    ) -> pd.DataFrame:
        end_point = "/api/mix/v1/market/historyFundRate"
        url = f"{self.base_url}{end_point}"
        params = {
            "symbol": symbol,
            "productType": self.product_type,
            "pageSize": self.max_limits[DataType.FUNDING_RATE],
        }
        response = self.make_request(url, params=params)
        if response and response.get("msg") == "success":
            data = response.get("data", [])
            if data:
                df = pd.DataFrame(data)
                if "fundingTime" not in df.columns and "fundingRateTime" in df.columns:
                    df.rename(columns={"fundingRateTime": "fundingTime"}, inplace=True)
                df["fundingTime"] = pd.to_datetime(df["fundingTime"].astype("int64"), unit="ms")
                df["fundingTime"] = df["fundingTime"].dt.floor("min")
                df["timestamp"] = df["fundingTime"]
                df.set_index("timestamp", inplace=True)
                df.sort_index(inplace=True)
                # 根据时间范围筛选
                mask = (df.index >= pd.to_datetime(start_timestamp, unit="ms")) & (
                    df.index <= pd.to_datetime(end_timestamp, unit="ms")
                )
                return df.loc[mask]
        return pd.DataFrame()

    def _fetch_premium_index_data(
        self,
        symbol: str,
        start_timestamp: int,
        end_timestamp: int,
        interval: str,
    ) -> pd.DataFrame:
        end_point = "/api/mix/v1/market/mark-candles"
        url = f"{self.base_url}{end_point}"
        params = {
            "symbol": symbol,
            "granularity": self._convert_interval(interval),
            "startTime": start_timestamp,
            "endTime": end_timestamp,
            "productType": self.product_type,
            "limit": self.max_limits[DataType.PREMIUM_INDEX],
        }
        response = self.make_request(url, params=params)
        if response and response.get("msg") == "success":
            data = response.get("data", [])
            if data:
                columns = [
                    "ts",
                    "open",
                    "high",
                    "low",
                    "close",
                    "volume",
                    "quote_volume",
                ]
                df = self._normalise_candles(data, columns)
                return df
        return pd.DataFrame()

    def fetch_all_symbol(self) -> list[str]:
        end_point = "/api/mix/v1/market/contracts"
        url = f"{self.base_url}{end_point}"
        params = {"productType": self.product_type}
        response = self.make_request(url, params=params)
        symbols: list[str] = []
        if response and response.get("msg") == "success":
            data = response.get("data", [])
            if data:
                symbols = [item["symbol"] for item in data]
        return symbols
