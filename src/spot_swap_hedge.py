"""Production-grade hedging loop for Spot (long) vs Swaps/Perp (short).

Core features
-------------
- ``spot_swaps_hedge(...)``: coordinates two concurrent legs (spot buy-up and
  swap sell-down) until both reach their respective target positions in
  lot-sized steps.
- ``reach_position(...)``: event-driven order placement loop that consumes a
  best-bid/ask market feed, replaces stale orders at top-of-book, and exits once
  the target position is reached.
- Exchange abstraction (``ExchangeClient``) with a minimal, production-oriented
  surface: positions, open orders, cancel/replace, and best bid/ask market data
  via an async context-managed feed.
- Robustness: idempotent logic, precise rounding to tick/step sizes, timeouts,
  retries with jitter, structured logging, graceful cancellation.

How to use
----------
1) Implement an adapter of ``ExchangeClient`` for your venue (e.g. Coinbase,
   Binance Futures, Bybit). A thin Coinbase Advanced Trade adapter is included
   below (``CoinbaseClient``) and already wired to API keys via ``config``. Fill
   the remaining ``TODO`` sections with your authenticated REST & WebSocket
   calls.
2) Instantiate your adapter, then run ``asyncio.run(spot_swaps_hedge(...))``
   with your symbols, targets, and risk/config parameters.

IMPORTANT
---------
- This module is designed for **limit IOC/PO** style top-of-book execution with
  frequent cancel/replace, not for aggressive sweeping. Tune config accordingly.
- Always test on paper/sandbox first, and validate increments/permissions.
"""

from .models import (
    Side,
    Instrument,
    Position,
    Order,
    TopOfBook,
    ExecConfig,
)
from .exchange import ExchangeClient
from .hedging import reach_position, spot_swaps_hedge
from .coinbase_client import CoinbaseClient
from .sim_client import SimClient
from .utils import logger, round_to_increment, now

__all__ = [
    "Side",
    "Instrument",
    "Position",
    "Order",
    "TopOfBook",
    "ExecConfig",
    "ExchangeClient",
    "reach_position",
    "spot_swaps_hedge",
    "CoinbaseClient",
    "SimClient",
    "logger",
    "round_to_increment",
    "now",
]
