import pandas as pd
from dataclasses import dataclass

@dataclass
class Candlestick:
    open: float
    high: float
    low: float
    close: float

    def to_dict(self):
        """vela a dict para WebSocket/JSON."""

        return {
            "open": self.open,
            "high": self.high,
            "low": self.low,
            "close": self.close}

class candleBuilder:
    def __init__(self, time_interval: int):
        self.ohlc: dict = None
        self.timestamp: pd.Timestamp = None
        self.time_inverval: int = time_interval
        
    def normalize_time(self, time: int):
        timestamp = pd.to_datetime(time, unit='s', utc=True) \
              .tz_convert('America/New_York') \
              .floor(self.time_inverval) \
              .tz_localize(None)

        return timestamp

    def historical_update(self, timestamp: pd.Timestamp, last_candle: dict):
        self.ohlc = last_candle
        self.timestamp = timestamp

    def new_candle(self, interval: pd.Timestamp, price: float):
        self.timestamp = interval
        self.ohlc = {
            'open': price,
            'high': price,
            'low': price,
            'close': price}

    def update(self, current_interval: int, tick: float):
        current_interval = self.normalize_time(current_interval)

        if current_interval != self.timestamp:
            self.new_candle(current_interval, tick)

        self.ohlc['high'] = max(self.ohlc['high'], tick)
        self.ohlc['low'] = min(self.ohlc['low'], tick)
        self.ohlc['close'] = tick

    def get_timestamp(self):        
        return self.timestamp

    def get_candle(self):
        return Candlestick(
            open=self.ohlc['open'],
            high=self.ohlc['high'],
            low=self.ohlc['low'],
            close=self.ohlc['close'])