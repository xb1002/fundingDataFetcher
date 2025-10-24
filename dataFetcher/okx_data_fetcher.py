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

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._symbol_map: dict[str, str] = {}

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
        df.sort_index(inplace=True)
        return df

    def _fetch_price_index_data(
        self,
        symbol: str,
        start_timestamp: int,
        end_timestamp: int,
        interval: str,
    ) -> pd.DataFrame:
        end_point = "/api/v5/market/history-index-candles"
        url = f"{self.base_url}{end_point}"
        params = {
            "instId": symbol,
            "bar": self._convert_interval(interval),
            "after": str(start_timestamp),
            "before": str(end_timestamp),
            "limit": str(self.max_limits[DataType.PRICE_INDEX]),
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
                start_dt = pd.to_datetime(start_timestamp, unit="ms")
                end_dt = pd.to_datetime(end_timestamp, unit="ms")
                return df.loc[(df.index >= start_dt) & (df.index <= end_dt)]
        return pd.DataFrame()

    def _fetch_price_data(
        self,
        symbol: str,
        start_timestamp: int,
        end_timestamp: int,
        interval: str,
    ) -> pd.DataFrame:
        end_point = "/api/v5/market/history-candles"
        url = f"{self.base_url}{end_point}"
        params = {
            "instId": symbol,
            "bar": self._convert_interval(interval),
            "after": str(start_timestamp),
            "before": str(end_timestamp),
            "limit": str(self.max_limits[DataType.PRICE]),
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
                start_dt = pd.to_datetime(start_timestamp, unit="ms")
                end_dt = pd.to_datetime(end_timestamp, unit="ms")
                return df.loc[(df.index >= start_dt) & (df.index <= end_dt)]
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
            "after": str(start_timestamp),
            "before": str(end_timestamp),
            "limit": str(self.max_limits[DataType.FUNDING_RATE]),
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
                start_dt = pd.to_datetime(start_timestamp, unit="ms")
                end_dt = pd.to_datetime(end_timestamp, unit="ms")
                return df.loc[(df.index >= start_dt) & (df.index <= end_dt)]
        return pd.DataFrame()

    def _fetch_premium_index_data(
        self,
        symbol: str,
        start_timestamp: int,
        end_timestamp: int,
        interval: str,
    ) -> pd.DataFrame:
        end_point = "/api/v5/market/history-mark-price-candles"
        url = f"{self.base_url}{end_point}"
        params = {
            "instId": symbol,
            "bar": self._convert_interval(interval),
            "after": str(start_timestamp),
            "before": str(end_timestamp),
            "limit": str(self.max_limits[DataType.PREMIUM_INDEX]),
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
                start_dt = pd.to_datetime(start_timestamp, unit="ms")
                end_dt = pd.to_datetime(end_timestamp, unit="ms")
                return df.loc[(df.index >= start_dt) & (df.index <= end_dt)]
        return pd.DataFrame()

    def _to_canonical_symbol(self, inst_id: str) -> str:
        parts = inst_id.split("-")
        if len(parts) >= 2:
            return f"{parts[0]}{parts[1]}"
        return inst_id.replace("-", "")

    def canonicalize_symbol(self, symbol: str) -> str:
        self._ensure_symbol_map()
        if symbol in self._symbol_map:
            return symbol
        if symbol in self._symbol_map.values():
            return self._to_canonical_symbol(symbol)
        candidate = symbol.replace("-", "")
        if candidate in self._symbol_map:
            return candidate
        return symbol

    def translate_symbol(self, symbol: str) -> str:
        self._ensure_symbol_map()
        if symbol in self._symbol_map.values():
            return symbol
        if symbol in self._symbol_map:
            return self._symbol_map[symbol]
        candidate = symbol.replace("-", "")
        if candidate in self._symbol_map:
            return self._symbol_map[candidate]
        return symbol

    def _ensure_symbol_map(self) -> None:
        if not self._symbol_map:
            self.fetch_all_symbol()

    def fetch_all_symbol(self) -> list[str]:
        if self._symbol_map:
            return sorted(self._symbol_map.keys())
        end_point = "/api/v5/public/instruments"
        url = f"{self.base_url}{end_point}"
        params = {"instType": "SWAP"}
        response = self.make_request(url, params=params)
        symbols: list[str] = []
        if response and response.get("code") == "0":
            data = response.get("data", [])
            if data:
                self._symbol_map = {}
                for item in data:
                    inst_id = item.get("instId", "")
                    if not inst_id:
                        continue
                    canonical = self._to_canonical_symbol(inst_id)
                    self._symbol_map[canonical] = inst_id
                symbols = sorted(self._symbol_map.keys())
        return symbols
