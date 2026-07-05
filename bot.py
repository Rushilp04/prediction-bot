import os
import asyncio
import aiohttp
from dotenv import load_dotenv

load_dotenv()

ODDPOOL_URL = "https://api.oddpool.com/v1/arbitrage"
KALSHI_API_BASE = "https://api.kalshi.com/v2"
POLY_API_BASE = "https://clob.polymarket.com"

class ArbitrageBot:
    def __init__(self):
        self.oddpool_key = os.getenv("ODDPOOL_API_KEY")
        self.kalshi_email = os.getenv("KALSHI_EMAIL")
        self.kalshi_password = os.getenv("KALSHI_PASSWORD")
        self.kalshi_token = None

    async def login_kalshi(self, session):
        url = f"{KALSHI_API_BASE}/login"
        payload = {"email": self.kalshi_email, "password": self.kalshi_password}
        async with session.post(url, json=payload) as resp:
            if resp.status == 200:
                data = await resp.json()
                self.kalshi_token = data["token"]
                print("[+] Authenticated with Kalshi.")
            else:
                raise ConnectionError(f"[-] Kalshi login failed: {await resp.text()}")

    async def place_kalshi_order(self, session, ticker, side, count, price):
        url = f"{KALSHI_API_BASE}/portfolio/orders"
        headers = {"Authorization": f"Bearer {self.kalshi_token}"}
        price_in_cents = int(price * 100)
        payload = {
            "ticker": ticker,
            "side": side,
            "action": "buy",
            "type": "limit",
            "count": count,
            "yes_price": price_in_cents if side == "yes" else None,
            "no_price": price_in_cents if side == "no" else None,
            "post_only": True
        }
        payload = {k: v for k, v in payload.items() if v is not None}
        async with session.post(url, json=payload, headers=headers) as resp:
            return await resp.json(), resp.status

    async def place_poly_order(self, session, token_id, side, size, price):
        url = f"{POLY_API_BASE}/order"
        headers = {
            "POLY-API-KEY": os.getenv("POLY_API_KEY"),
            "POLY-PASSPHRASE": os.getenv("POLY_PASSPHRASE"),
        }
        payload = {
            "tokenID": token_id,
            "price": float(price),
            "side": side,
            "size": float(size),
            "type": "limit",
            "postOnly": True
        }
        async with session.post(url, json=payload, headers=headers) as resp:
            return await resp.json(), resp.status

    async def run(self):
        headers = {"Authorization": f"Bearer {self.oddpool_key}"}
        async with aiohttp.ClientSession() as session:
            await self.login_kalshi(session)
            print("[*] Starting Oddpool polling loop...")
            while True:
                try:
                    async with session.get(ODDPOOL_URL, headers=headers) as resp:
                        if resp.status != 200:
                            await asyncio.sleep(1)
                            continue
                        
                        data = await resp.json()
                        for market in data.get("live_opportunities", []):
                            if "sports" not in market.get("category", "").lower():
                                continue
                                
                            total_cost = market["kalshi_price"] + market["poly_price"]
                            if total_cost < 0.96:
                                print(f"[!] Arbitrage opportunity found: {market['title']}")
                                
                                # Concurrent Execution Guardrail
                                k_task = self.place_kalshi_order(session, market["kalshi_ticker"], "yes", 10, market["kalshi_price"])
                                p_task = self.place_poly_order(session, market["poly_token_id"], "BUY", 10, market["poly_price"])
                                
                                results = await asyncio.gather(k_task, p_task, return_exceptions=True)
                                print(f"[*] Execution results: {results}")
                                await asyncio.sleep(30)
                except Exception as e:
                    print(f"[-] Loop error: {e}")
                await asyncio.sleep(0.5)

if __name__ == "__main__":
    bot = ArbitrageBot()
    asyncio.run(bot.run())