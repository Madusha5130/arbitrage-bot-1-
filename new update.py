import asyncio
import aiohttp
import time
import ssl
import certifi
import traceback

from aiohttp import ClientConnectorError

# --- CONFIGURATION --- #
COINMARKETCAP_API_KEY = "7cd882b6-efa9-44ef-8715-22faca85eba3"
TELEGRAM_BOT_TOKEN = "7769765331:AAEw12H4-98xYfP_2tBGQQPe10prkXF-lGM"
TELEGRAM_CHAT_ID = "5556378872"
ARBITRAGE_THRESHOLD = 1.5

EXCHANGES = {
    "BINANCE": lambda symbol: f"https://api.binance.com/api/v3/ticker/price?symbol={symbol}USDT",
    "BITGET": lambda symbol: f"https://api.bitget.com/api/spot/v1/market/ticker?symbol={symbol}USDT",
    "BYBIT": lambda symbol: f"https://api.bybit.com/v5/market/tickers?category=spot&symbol={symbol}USDT",
    "GATEIO": lambda symbol: f"https://api.gate.io/api/v4/spot/tickers?currency_pair={symbol}_USDT",
    "COINBASE": lambda symbol: f"https://api.coinbase.com/v2/prices/{symbol}-USD/spot",
    "KUCOIN": lambda symbol: f"https://api.kucoin.com/api/v1/market/orderbook/level1?symbol={symbol}-USDT",
    "OKX": lambda symbol: f"https://www.okx.com/api/v5/market/ticker?instId={symbol}-USDT",
    "MEXC": lambda symbol: f"https://api.mexc.com/api/v3/ticker/price?symbol={symbol}USDT",
}

ssl_context = ssl.create_default_context(cafile=certifi.where())

async def fetch_price(session, url, exchange, symbol):
    try:
        async with session.get(url, ssl=ssl_context, timeout=10) as response:
            data = await response.json()
            if exchange == "BINANCE": return float(data["price"])
            elif exchange == "BITGET": return float(data['data']['close'])
            elif exchange == "BYBIT": return float(data['result']['list'][0]['lastPrice'])
            elif exchange == "GATEIO": return float(data[0]['last'])
            elif exchange == "COINBASE": return float(data['data']['amount'])
            elif exchange == "KUCOIN": return float(data['data']['price'])
            elif exchange == "OKX": return float(data['data'][0]['last'])
            elif exchange == "MEXC": return float(data['price'])
    except Exception as e:
        print(f"[ERROR] Failed to fetch {symbol} from {exchange}: {e}")
    return None

async def fetch_top_coins():
    url = f"https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest?limit=200"
    headers = {"X-CMC_PRO_API_KEY": COINMARKETCAP_API_KEY}
    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=headers, ssl=ssl_context) as response:
            data = await response.json()
            return [(coin['symbol'], coin['cmc_rank']) for coin in data['data']]

async def send_telegram_message(message):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": "Markdown"
    }
    async with aiohttp.ClientSession() as session:
        try:
            await session.post(url, data=payload)
        except Exception as e:
            print(f"[ERROR] Failed to send Telegram message: {e}")

async def check_arbitrage():
    coins = await fetch_top_coins()
    async with aiohttp.ClientSession() as session:
        for symbol, rank in coins:
            prices = {}
            tasks = []
            for exchange, url_fn in EXCHANGES.items():
                url = url_fn(symbol)
                tasks.append(fetch_price(session, url, exchange, symbol))
            results = await asyncio.gather(*tasks)

            for idx, (exchange, _) in enumerate(EXCHANGES.items()):
                if results[idx] is not None:
                    prices[exchange] = results[idx]

            if len(prices) < 2:
                continue

            min_exchange = min(prices, key=prices.get)
            max_exchange = max(prices, key=prices.get)
            min_price = prices[min_exchange]
            max_price = prices[max_exchange]

            if min_price == 0:
                continue

            diff_percent = ((max_price - min_price) / min_price) * 100
            if diff_percent >= ARBITRAGE_THRESHOLD:
                message = (
                    f"**Arbitrage Opportunity Found:**\n"
                    f"- *{symbol}* (Rank #{rank})\n"
                    f"- {min_price:.4f} ({min_exchange}) -> {max_price:.4f} ({max_exchange})\n"
                    f"- Difference: {diff_percent:.2f}%"
                )
                print(message)
                await send_telegram_message(message)

async def main():
    while True:
        print(f"[SCAN STARTED] {time.strftime('%Y-%m-%d %H:%M:%S')}")
        try:
            await check_arbitrage()
        except Exception:
            traceback.print_exc()
        await asyncio.sleep(60)

if __name__ == '__main__':
    asyncio.run(main())
