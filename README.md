# btc_apis

Repo for BTC data collection and chart generation APIs. You will need a VPN to run this from the US. 

### 1) Setup a virtual environment
**Linux/macOS/WSL**
```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

**Windows PowerShell**
```powershell
python -m venv venv
venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

> The included `requirements.txt` installs: `pandas`, `requests`, `matplotlib`, `numpy`.

---

## 2‑Day BTC Ichimoku (9, 26, 52, 26)

Generates 2‑day candles by resampling Binance klines, computes Ichimoku (9/26/52/26), and saves **PNG + CSV**.

**Recommended run (good granularity pre‑aggregation):**
```bash
python btc_2day_ichimoku.py --symbol BTCUSDT --interval 1h --lookback-days 400
```

**Alternative (fewer requests, larger interval):**
```bash
python btc_2day_ichimoku.py --symbol BTCUSDT --interval 4h --lookback-days 1200
```

**Outputs**
- `btc_2day_ichimoku.csv` — 2D OHLCV + Tenkan/Kijun/Senkou A/Senkou B/Chikou
- `btc_2day_ichimoku.png` — line chart of Close + Ichimoku components

**Flags**
- `--symbol` (default: `BTCUSDT`)
- `--interval` (e.g., `1m`, `5m`, `15m`, `1h`, `4h`)
- `--lookback-days` how many days of raw klines to fetch before resampling

---

## 3‑Day BTC Ichimoku (9, 26, 52, 26)

Same flow as above, but aggregates to **3‑day** candles.

**Recommended run:**
```bash
python btc_3day_ichimoku.py --symbol BTCUSDT --interval 1h --lookback-days 900
```

**Alternative:**
```bash
python btc_3day_ichimoku.py --symbol BTCUSDT --interval 4h --lookback-days 2000
```

**Outputs**
- `btc_3day_ichimoku.csv`
- `btc_3day_ichimoku.png`

---

## Notes & Tips

- **Exchange API**: Uses **Binance Spot public klines** (no API key). If your US IP is throttled/blocked, connect your VPN and retry.
- **Time zone**: Resampling uses **UTC** boundaries for 2‑day/3‑day candles.
- **Candles vs. Lines**: Current plots are line charts (Close + Ichimoku). If you want candlesticks/cloud fill, we can add that easily.
- **Other symbols**: Works with any symbol Binance supports, e.g., `ETHUSDT`. Just pass `--symbol ETHUSDT`.

---

## Existing Scripts

If you’re also running your earlier Binance helpers (e.g., `bin_1min_input.py`, `bin_1min_ratecap.py`), keep using them as before. These new scripts are focused on **higher‑timeframe Ichimoku** chart generation.

---

## Examples at a Glance

```bash
# 2‑Day Ichimoku with 1h raw klines, ~400 days back
python btc_2day_ichimoku.py --symbol BTCUSDT --interval 1h --lookback-days 400

# 3‑Day Ichimoku with 1h raw klines, ~900 days back
python btc_3day_ichimoku.py --symbol BTCUSDT --interval 1h --lookback-days 900

# 2‑Day Ichimoku using 4h raw klines (fewer requests)
python btc_2day_ichimoku.py --symbol BTCUSDT --interval 4h --lookback-days 1200

# 3‑Day Ichimoku using 4h raw klines (fewer requests)
python btc_3day_ichimoku.py --symbol BTCUSDT --interval 4h --lookback-days 2000
```

---

## Roadmap / TODO
- Add candlestick rendering + cloud fill (Senkou A/B shaded).
- Configurable output directories and filenames.
- Optional Bybit/Kraken endpoints as fallbacks.
- CLI flag to export **only CSV** or **only PNG**.
