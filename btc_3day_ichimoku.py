#!/usr/bin/env python3
"""
btc_3day_ichimoku.py
Fetch BTC/USDT klines from Binance, aggregate to 3-day candles,
compute Ichimoku (9, 26, 52, 26), and save a PNG + CSV locally.

Usage examples:
  python btc_3day_ichimoku.py --symbol BTCUSDT --interval 1h --lookback-days 900
  python btc_3day_ichimoku.py --symbol BTCUSDT --interval 4h --lookback-days 2000

Outputs:
  ./btc_3day_ichimoku.csv
  ./btc_3day_ichimoku.png
"""

import argparse
import time
from datetime import datetime, timedelta, timezone

import requests
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt


BINANCE_URL = "https://api.binance.com/api/v3/klines"


def fetch_klines(symbol, interval, start_ms, end_ms=None, limit=1000):
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


def klines_to_df(klines):
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


def resample_to_3d(df):
    """
    Aggregate to 3-day OHLCV using UTC boundaries.
    """
    o = df["open"].resample("3D", label="right", closed="right").first()
    h = df["high"].resample("3D", label="right", closed="right").max()
    l = df["low"].resample("3D", label="right", closed="right").min()
    c = df["close"].resample("3D", label="right", closed="right").last()
    v = df["volume"].resample("3D", label="right", closed="right").sum()
    out = pd.concat({"open": o, "high": h, "low": l, "close": c, "volume": v}, axis=1).dropna()
    return out


def ichimoku(df_agg, conv_len=9, base_len=26, span_b_len=52, displacement=26):
    """
    Compute Ichimoku components on aggregated candles.
    """
    high = df_agg["high"]
    low = df_agg["low"]
    close = df_agg["close"]

    conv = (high.rolling(conv_len).max() + low.rolling(conv_len).min()) / 2.0
    base = (high.rolling(base_len).max() + low.rolling(base_len).min()) / 2.0
    span_a = ((conv + base) / 2.0).shift(displacement)  # forward shift
    span_b = ((high.rolling(span_b_len).max() + low.rolling(span_b_len).min()) / 2.0).shift(displacement)  # forward shift
    chikou = close.shift(-displacement)  # lagging, plotted back

    out = df_agg.copy()
    out["tenkan"] = conv
    out["kijun"] = base
    out["senkou_a"] = span_a
    out["senkou_b"] = span_b
    out["chikou"] = chikou
    return out


def plot_ichimoku(df_i, out_png, title):
    plt.figure(figsize=(12, 6))
    plt.plot(df_i.index, df_i["close"], label="Close")
    plt.plot(df_i.index, df_i["tenkan"], label="Tenkan (9)")
    plt.plot(df_i.index, df_i["kijun"], label="Kijun (26)")
    plt.plot(df_i.index, df_i["senkou_a"], label="Senkou A (shifted +26)")
    plt.plot(df_i.index, df_i["senkou_b"], label="Senkou B (shifted +26)")
    plt.plot(df_i.index, df_i["chikou"], label="Chikou (shifted -26)")
    plt.title(title)
    plt.xlabel("Time (UTC) — 3D candles")
    plt.ylabel("Price (USDT)")
    plt.legend(loc="best")
    # Draw the series
    plt.plot(df_i.index, df_i["close"], label="Close")
    plt.plot(df_i.index, df_i["tenkan"], label="Tenkan (9)")
    plt.plot(df_i.index, df_i["kijun"], label="Kijun (26)")
    plt.plot(df_i.index, df_i["senkou_a"], label="Senkou A (shifted +26)")
    plt.plot(df_i.index, df_i["senkou_b"], label="Senkou B (shifted +26)")
    plt.plot(df_i.index, df_i["chikou"], label="Chikou (shifted -26)")

    # Latest Senkou B value (most recent non-NaN)
    valid = df_i["senkou_b"].dropna()
    latest_sb = None
    latest_index = None
    if not valid.empty:
        latest_sb = valid.iloc[-1]
        latest_index = valid.index[-1]
        # Horizontal reference line at latest Senkou B
        plt.axhline(latest_sb, linestyle="--", linewidth=1, label=f"Senkou B: {latest_sb:.2f}")
        # Place a text label slightly to the right/top of the last valid point
        plt.annotate(f"Senkou B = {latest_sb:.2f}",
                     xy=(latest_index, latest_sb),
                     xytext=(10, 10),
                     textcoords="offset points",
                     fontsize=9)

    plt.title(title)
    plt.xlabel("Time (UTC)")
    plt.ylabel("Price (USDT)")
    plt.legend(loc="best")
    plt.tight_layout()
    plt.savefig(out_png, dpi=150)
    plt.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbol", default="BTCUSDT", help="Symbol, e.g., BTCUSDT")
    parser.add_argument("--interval", default="1h", help="Binance kline interval (e.g., 1m, 5m, 15m, 1h, 4h)")
    parser.add_argument("--lookback-days", type=int, default=900, help="How many days back to fetch from now.")
    parser.add_argument("--outfile-prefix", default="btc_3day_ichimoku", help="Output file prefix")
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
    df_3d = resample_to_3d(df)
    df_i = ichimoku(df_3d, conv_len=9, base_len=26, span_b_len=52, displacement=26)

    csv_path = f"{args.outfile_prefix}.csv"
    png_path = f"{args.outfile_prefix}.png"

    df_i.to_csv(csv_path, index=True)
    title = f"{args.symbol} — 3-Day Ichimoku (9,26,52,26) — Generated {now_utc.date().isoformat()}"
    plot_ichimoku(df_i, png_path, title)

    print(f"Saved CSV: {csv_path}")
    print(f"Saved PNG: {png_path}")


if __name__ == "__main__":
    main()
