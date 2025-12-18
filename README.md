# 24h Volume Pump Scanner (Telegram Bot)

A Telegram bot that monitors 24h volume spikes on crypto perpetual futures across multiple exchanges (Bybit, Binance, MEXC). It detects coins where the last 24 hours' volume is significantly higher than the previous 24 hours and sends alerts to a chat.


- 🔍 Scans USDT linear futures on:
  - Binance (futures)
  - Bybit (USDT perpetual)
  - MEXC (USDT perpetual)
- 📈 Signal conditions:
  - Last 24h volume ≥ 20M USD
  - Volume growth ≥ 500% (last 24h / previous 24h)
- 🧠 Deduplication:
  - Shows only the **best exchange per coin** (max volume growth)
- 🆕 New vs old:
  - New coins (not seen in the previous scan) are flagged with `🆕` and listed first
- ⏰ Scheduling:
  - Worker runs every minute
  - Full scan is executed on each **hourly close**
  - Heartbeat message every hour to confirm the bot is alive
