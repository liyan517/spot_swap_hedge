from json import dumps
from coinbase.rest import RESTClient
from config import API_KEY, API_SECRET

# Use env by default; passing explicitly shown for clarity
client = RESTClient(api_key=API_KEY, api_secret=API_SECRET, rate_limit_headers=True)

def main():
    # Public data (no auth needed, but SDK handles both)
    prods = client.get_public_products()
    print("First 3 products:\n", dumps(prods.to_dict()["products"][:3], indent=2))

    # A specific product
    btcusd = client.get_product("BTC-USD")
    print("\nBTC-USD snapshot price:", btcusd.price)

    # Recent market trades for BTC-USD (public)
    trades = client.get_market_trades(product_id="BTC-USD", limit=5)
    print("\nRecent trades:\n", dumps(trades.to_dict(), indent=2))

    # Your accounts (private, requires JWT behind the scenes)
    accounts = client.get_accounts()
    print("\nAccounts:\n", dumps(accounts.to_dict(), indent=2))

if __name__ == "__main__":
    main()
