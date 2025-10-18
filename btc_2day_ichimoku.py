#!/usr/bin/env python3
"""
btc_2day_ichimoku.py
Fetch BTC/USDT klines from Binance, aggregate to 2-day candles,
compute Ichimoku (9, 26, 52, 26), and save a PNG + CSV locally.

Usage (examples):
  python btc_2day_ichimoku.py --symbol BTCUSDT --interval 1h --lookback-days 400
  python btc_2day_ichimoku.py --symbol BTCUSDT --interval 4h --lookback-days 1200

Notes:
- Uses public Binance Spot API (no key required).
- If US IP is throttled/blocked, run via VPN as you mentioned.
- Output files:
    ./btc_2day_ichimoku.csv
    ./btc_2day_ichimoku.png
"""

import argparse
import time
from datetime import datetime, timedelta, timezone

import requests
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt


BINANCE_URL = "https://api.binance.com/api/v3/klines"

def fetch_klines(symbol: str, interval: str, start_ms: int, end_ms: int | None = None, limit: int = 1000) -> list[list]:
    """
    Fetch klines from Binance with pagination.
    Returns a list of kline arrays.
    """
    out = []
    params = {
        "symbol": symbol.upper(),
        "interval": interval,
        "limit": min(limit, 1000),
        "startTime": start_ms,
    }
    if end_ms is not None:
        params["endTime"] = end_ms

    while True:
        r = requests.get(BINANCE_URL, params=params, timeout=30)
        r.raise_for_status()
        batch = r.json()
        if not batch:
            break
        out.extend(batch)
        next_start = batch[-1][6] + 1  # last close_time + 1ms
        if end_ms is not None and next_start >= end_ms:
            break
        params["startTime"] = next_start
        time.sleep(0.2)
        if len(batch) < params["limit"]:
            break
    return out


def klines_to_df(klines: list[list]) -> pd.DataFrame:
    cols = [
        "open_time","open","high","low","close","volume",
        "close_time","quote_asset_volume","number_of_trades",
        "taker_buy_base","taker_buy_quote","ignore"
    ]
    df = pd.DataFrame(klines, columns=cols)
    for c in ["open","high","low","close","volume","quote_asset_volume","taker_buy_base","taker_buy_quote"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df["open_time"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)
    df["close_time"] = pd.to_datetime(df["close_time"], unit="ms", utc=True)
    df = df.set_index("open_time").sort_index()
    return df


def resample_to_2d(df: pd.DataFrame) -> pd.DataFrame:
    o = df["open"].resample("2D", label="right", closed="right").first()
    h = df["high"].resample("2D", label="right", closed="right").max()
    l = df["low"].resample("2D", label="right", closed="right").min()
    c = df["close"].resample("2D", label="right", closed="right").last()
    v = df["volume"].resample("2D", label="right", closed="right").sum()
    out = pd.concat({"open": o, "high": h, "low": l, "close": c, "volume": v}, axis=1).dropna()
    return out


def ichimoku(df_2d: pd.DataFrame, conv_len=9, base_len=26, span_b_len=52, displacement=26) -> pd.DataFrame:
    high = df_2d["high"]
    low = df_2d["low"]
    close = df_2d["close"]

    conv = (high.rolling(conv_len).max() + low.rolling(conv_len).min()) / 2.0
    base = (high.rolling(base_len).max() + low.rolling(base_len).min()) / 2.0
    span_a = ((conv + base) / 2.0).shift(displacement)  # forward shift
    span_b = ((high.rolling(span_b_len).max() + low.rolling(span_b_len).min()) / 2.0).shift(displacement)  # forward shift
    chikou = close.shift(-displacement)  # lagging, plotted back

    out = df_2d.copy()
    out["tenkan"] = conv
    out["kijun"] = base
    out["senkou_a"] = span_a
    out["senkou_b"] = span_b
    out["chikou"] = chikou
    return out


def plot_ichimoku(df_i: pd.DataFrame, out_png: str, title: str):
    plt.figure(figsize=(12, 6))
    plt.plot(df_i.index, df_i["close"], label="Close")
    plt.plot(df_i.index, df_i["tenkan"], label="Tenkan (9)")
    plt.plot(df_i.index, df_i["kijun"], label="Kijun (26)")
    plt.plot(df_i.index, df_i["senkou_a"], label="Senkou A (shifted +26)")
    plt.plot(df_i.index, df_i["senkou_b"], label="Senkou B (shifted +26)")
    plt.plot(df_i.index, df_i["chikou"], label="Chikou (shifted -26)")

    plt.title(title)
    plt.xlabel("Time (UTC) — 2D candles")
    plt.ylabel("Price (USDT)")
    plt.legend(loc="best")
    plt.tight_layout()
    plt.savefig(out_png, dpi=150)
    plt.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbol", default="BTCUSDT", help="Symbol, e.g., BTCUSDT")
    parser.add_argument("--interval", default="1h", help="Binance kline interval (e.g., 1m, 5m, 15m, 1h, 4h)")
    parser.add_argument("--lookback-days", type=int, default=400, help="How many days back to fetch from now.")
    parser.add_argument("--outfile-prefix", default="btc_2day_ichimoku", help="Output file prefix")
    args = parser.parse_args()

    now_utc = datetime.now(timezone.utc)
    start_dt = now_utc - timedelta(days=args.lookback_days)
    start_ms = int(start_dt.timestamp() * 1000)
    end_ms = int(now_utc.timestamp() * 1000)

    print(f"Fetching {args.symbol} {args.interval} from {start_dt.isoformat()} to {now_utc.isoformat()} ...")
    kl = fetch_klines(args.symbol, args.interval, start_ms, end_ms=end_ms)

    if not kl:
        raise SystemExit("No data returned. Try increasing lookback or changing interval.")

    df = klines_to_df(kl)
    df_2d = resample_to_2d(df)
    df_i = ichimoku(df_2d, conv_len=9, base_len=26, span_b_len=52, displacement=26)

    csv_path = f"{args.outfile_prefix}.csv"
    png_path = f"{args.outfile_prefix}.png"

    df_i.to_csv(csv_path, index=True)
    title = f"{args.symbol} — 2-Day Ichimoku (9,26,52,26) — Generated {now_utc.date().isoformat()}"
    plot_ichimoku(df_i, png_path, title)

    print(f"Saved CSV: {csv_path}")
    print(f"Saved PNG: {png_path}")


if __name__ == "__main__":
    main()
