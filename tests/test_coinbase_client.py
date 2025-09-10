import asyncio as aio
import os
import sys
import types

import pytest

# Ensure the project root (containing the ``src`` package) is on the import
# path so ``src.coinbase_client`` can be imported.
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# Provide stub ``dotenv`` and environment variables expected by ``config``.
dotenv = types.ModuleType("dotenv")
dotenv.load_dotenv = lambda *args, **kwargs: None
sys.modules["dotenv"] = dotenv
os.environ.setdefault("COINBASE_API_KEY", "k")
os.environ.setdefault("COINBASE_API_SECRET", "s")

# Stub out the external ``coinbase-advanced-py`` dependency so that the module
# under test can be imported without the real package installed.
coinbase = types.ModuleType("coinbase")
rest_mod = types.ModuleType("coinbase.rest")
ws_mod = types.ModuleType("coinbase.websocket")


class DummyRESTClient:
    def __init__(self, *args, **kwargs):
        pass


rest_mod.RESTClient = DummyRESTClient


_events = {"ticker_called": False}


class DummyWSClient:
    def __init__(self, on_message=None, api_key=None, api_secret=None):
        self.on_message = on_message
        self.open_complete = aio.Event()

    async def open(self):
        # Yield control to ensure the coroutine actually runs
        await aio.sleep(0.01)
        self.open_complete.set()

    async def ticker(self, product_ids):
        # ``ticker`` should only run after ``open`` has completed
        assert self.open_complete.is_set(), "open must complete before subscribing"
        _events["ticker_called"] = True

    async def ticker_unsubscribe(self, product_ids):
        pass

    async def close(self):
        pass


ws_mod.WSClient = DummyWSClient

sys.modules.setdefault("coinbase", coinbase)
sys.modules["coinbase.rest"] = rest_mod
sys.modules["coinbase.websocket"] = ws_mod

from src.coinbase_client import CoinbaseClient


def test_ws_open_completed_before_subscribe():
    async def runner():
        client = CoinbaseClient("k", "s")
        async with client.top_of_book_feed(["BTC-USD"]):
            # No messages are fed; entering the context triggers ``open`` then
            # ``ticker`` which our stubs validate for correct sequencing.
            await aio.sleep(0)

    aio.run(runner())
    assert _events["ticker_called"]
