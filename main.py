# This is a sample Python script.

# Press ⌃R to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.

import os
import asyncio
import src.config as config

# run_hedge.py
import asyncio
from src.spot_swap_hedge import (
    spot_swaps_hedge, Instrument, ExecConfig,
    CoinbaseClient,  # or SimClient for a local simulator
)
# import pytest

missing_creds = not (config.API_KEY and config.API_SECRET)


def test_coinbase_client_real_api():
    """Connect to Coinbase Advanced Trade and fetch live data."""

    if missing_creds:
        print("requires Coinbase API credentials")

    from src.coinbase_client import CoinbaseClient
    from src.models import Position

    client = CoinbaseClient()
    # Fetch balance for BTC to ensure REST connectivity
    pos = asyncio.get_event_loop().run_until_complete(client.get_position("BTC-USD"))
    assert isinstance(pos, Position)

    async def _get_tob():
        async with client.top_of_book_feed(["BTC-USD"]) as feed:
            return await asyncio.wait_for(feed.__anext__(), timeout=10)

    tob = asyncio.get_event_loop().run_until_complete(_get_tob())
    print(tob)
    assert tob.symbol == "BTC-USD"
    assert tob.bid > 0
    assert tob.ask > 0
# Press the green button in the gutter to run the script.


async def runner():
    client = CoinbaseClient()  # real exchange
    spot = Instrument("BTC-USD", 0.01, 0.0001)
    swap = Instrument("BTC-PERP", 0.1, 0.001)
    cfg = ExecConfig(price_offset_bps=0)  # adjust as needed
    await spot_swaps_hedge(
        client,
        spot, swap,
        target_spot_qty=0.01,
        target_swap_qty=-0.01,
        lot_size=0.001,
        cfg=cfg,
    )


if __name__ == '__main__':
    # test_coinbase_client_real_api()
    asyncio.run(runner())

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
