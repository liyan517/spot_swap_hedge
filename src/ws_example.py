import time
from coinbase.websocket import WSClient  # public feed
# For user-specific channels you'd use: from coinbase.websocket import WSUserClient

def on_msg(msg: str):
    print(msg)

def main():
    ws = WSClient(on_message=on_msg)  # public channels don't need keys
    ws.open()
    ws.ticker(product_ids=["BTC-USD"])  # subscribe to ticker channel
    time.sleep(10)
    ws.ticker_unsubscribe(product_ids=["BTC-USD"])
    ws.close()

if __name__ == "__main__":
    main()
