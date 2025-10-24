from __future__ import annotations

from typing import Optional

import pandas as pd

from .data_fetcher_base import DataFetcherBase, DataType


class OKXDataFetcher(DataFetcherBase):
    """OKX 数据获取器"""

    _INTERVAL_MAP = {
        "1m": "1m",
        "3m": "3m",
        "5m": "5m",
        "15m": "15m",
        "30m": "30m",
        "1h": "1H",
        "2h": "2H",
        "4h": "4H",
        "6h": "6H",
        "8h": "8H",
        "12h": "12H",
        "1d": "1D",
        "3d": "3D",
        "1w": "1W",
        "1M": "1M",
    }

    def get_exchange_name(self) -> str:
        return "okx"

    def _convert_interval(self, interval: str) -> str:
        try:
            return self._INTERVAL_MAP[interval]
        except KeyError as exc:
            raise ValueError(f"Unsupported interval for OKX: {interval}") from exc

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
        end_point = "/api/v5/market/index-candles"
        url = f"{self.base_url}{end_point}"
        params = {
            "instId": symbol,
            "bar": self._convert_interval(interval),
            "after": start_timestamp,
            "before": end_timestamp,
            "limit": self.max_limits[DataType.PRICE_INDEX],
        }
        response = self.make_request(url, params=params)
        if response and response.get("code") == "0":
            data = response.get("data", [])
            if data:
                columns = [
                    "ts",
                    "open",
                    "high",
                    "low",
                    "close",
                    "volume",
                    "volume_ccy",
                    "volume_ccy_quote",
                    "confirm",
                ]
                df = self._normalise_candles(data, columns)
                return df
        return pd.DataFrame()

    def _fetch_price_data(
        self,
        symbol: str,
        start_timestamp: int,
        end_timestamp: int,
        interval: str,
    ) -> pd.DataFrame:
        end_point = "/api/v5/market/candles"
        url = f"{self.base_url}{end_point}"
        params = {
            "instId": symbol,
            "bar": self._convert_interval(interval),
            "after": start_timestamp,
            "before": end_timestamp,
            "limit": self.max_limits[DataType.PRICE],
        }
        response = self.make_request(url, params=params)
        if response and response.get("code") == "0":
            data = response.get("data", [])
            if data:
                columns = [
                    "ts",
                    "open",
                    "high",
                    "low",
                    "close",
                    "volume",
                    "volume_ccy",
                    "volume_ccy_quote",
                    "confirm",
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
        end_point = "/api/v5/public/funding-rate-history"
        url = f"{self.base_url}{end_point}"
        params = {
            "instId": symbol,
            "after": start_timestamp,
            "before": end_timestamp,
            "limit": self.max_limits[DataType.FUNDING_RATE],
        }
        response = self.make_request(url, params=params)
        if response and response.get("code") == "0":
            data = response.get("data", [])
            if data:
                df = pd.DataFrame(data)
                if "fundingTime" not in df.columns:
                    # API returns fundingRateTimestamp field
                    if "fundingTime" not in df.columns and "fundingRateTimestamp" in df.columns:
                        df.rename(columns={"fundingRateTimestamp": "fundingTime"}, inplace=True)
                df["fundingTime"] = pd.to_datetime(df["fundingTime"].astype("int64"), unit="ms")
                df["fundingTime"] = df["fundingTime"].dt.floor("min")
                df["timestamp"] = df["fundingTime"]
                df.set_index("timestamp", inplace=True)
                df.sort_index(inplace=True)
                return df
        return pd.DataFrame()

    def _fetch_premium_index_data(
        self,
        symbol: str,
        start_timestamp: int,
        end_timestamp: int,
        interval: str,
    ) -> pd.DataFrame:
        end_point = "/api/v5/market/mark-price-candles"
        url = f"{self.base_url}{end_point}"
        params = {
            "instId": symbol,
            "bar": self._convert_interval(interval),
            "after": start_timestamp,
            "before": end_timestamp,
            "limit": self.max_limits[DataType.PREMIUM_INDEX],
        }
        response = self.make_request(url, params=params)
        if response and response.get("code") == "0":
            data = response.get("data", [])
            if data:
                columns = [
                    "ts",
                    "open",
                    "high",
                    "low",
                    "close",
                    "volume",
                    "volume_ccy",
                    "volume_ccy_quote",
                    "confirm",
                ]
                df = self._normalise_candles(data, columns)
                return df
        return pd.DataFrame()

    def fetch_all_symbol(self) -> list[str]:
        end_point = "/api/v5/public/instruments"
        url = f"{self.base_url}{end_point}"
        params = {"instType": "SWAP"}
        response = self.make_request(url, params=params)
        symbols: list[str] = []
        if response and response.get("code") == "0":
            data = response.get("data", [])
            if data:
                symbols = [item["instId"] for item in data]
        return symbols
