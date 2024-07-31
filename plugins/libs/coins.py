import pandas as pd


def transform_raw_data(df: pd.DataFrame):
    return df.rename(
        columns={
            "candle_date_time_utc": "candleDateTimeUtc",
            "candle_date_time_kst": "candleDateTimeKst",
            "opening_price": "openingPrice",
            "high_price": "highPrice",
            "low_price": "lowPrice",
            "trade_price": "tradePrice",
            "candle_acc_trade_price": "candleAccTradePrice",
            "candle_acc_trade_volume": "candleAccTradeVolume",
        }
    )
