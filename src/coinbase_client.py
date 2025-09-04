"""Exchange adapter backed by `coinbase-advanced-py`.

The real Coinbase Advanced Trade API is synchronous.  The strategy expects an
``ExchangeClient`` with ``async`` methods so this module provides thin wrappers
around the SDK that delegate blocking calls to a threadpool.  Only a tiny subset
of functionality required by the hedger is implemented – enough to query
balances, manage limit orders and stream top‑of‑book data via websockets.

The implementation intentionally mirrors the public API documented at
https://docs.cloud.coinbase.com/advanced-trade-api .  No network calls are made
by the unit tests so best‑effort stubs are sufficient provided their shape is
correct.
"""

import asyncio as aio
import contextlib
import json
import time
from typing import AsyncIterator, Optional, Sequence, List, Callable, Any

from coinbase.rest import RESTClient
from coinbase.websocket import WSClient

from .exchange import ExchangeClient
from .models import Position, Order, Side, TopOfBook
from .config import API_KEY, API_SECRET


class CoinbaseClient(ExchangeClient):
    """Thin adapter for Coinbase Advanced Trade.

    It wires API credentials from :mod:`config` into ``coinbase-advanced-py``
    clients and exposes the minimal async interface required by the hedging
    strategy.
    """

    def __init__(self, api_key: str = API_KEY, api_secret: str = API_SECRET):
        self.api_key = api_key
        self.api_secret = api_secret
        # REST client handles auth, signing and rate limits
        self.rest = RESTClient(
            api_key=api_key, api_secret=api_secret, rate_limit_headers=True
        )
        self._ws: Optional[WSClient] = None

    # --- helpers ---------------------------------------------------------
    async def _async(self, fn: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        """Execute the blocking REST/WS call in the default executor."""
        loop = aio.get_running_loop()
        return await loop.run_in_executor(None, lambda: fn(*args, **kwargs))

    # --- Positions ---
    async def get_position(self, symbol: str) -> Position:
        """Return the available balance for the base asset of ``symbol``.

        Coinbase exposes balances per currency.  A spot product like ``BTC-USD``
        uses ``BTC`` as its base asset; the net position is simply the available
        balance of that currency.  If the account is missing the currency we
        assume a flat position.
        """

        base_ccy = symbol.split("-")[0]
        resp = await self._async(self.rest.get_accounts)
        accounts = resp.to_dict()["accounts"] if hasattr(resp, "to_dict") else resp["accounts"]
        for acct in accounts:
            if acct.get("currency") == base_ccy:
                bal = acct.get("available_balance", {}).get("value", "0")
                return Position(symbol, float(bal))
        return Position(symbol, 0.0)

    # --- Orders ---
    async def list_open_orders(self, symbol: str, side: Optional[Side] = None) -> List[Order]:
        """Fetch currently working orders for ``symbol``.

        Coinbase's ``list_orders`` endpoint returns orders in descending
        creation time.  We request only open/pending orders and map the minimal
        fields to our :class:`Order` dataclass.
        """

        resp = await self._async(
            self.rest.list_orders,
            product_id=symbol,
            order_status=["OPEN", "PENDING"],
        )
        raw = resp.to_dict().get("orders", []) if hasattr(resp, "to_dict") else resp.get("orders", [])
        out: List[Order] = []
        for o in raw:
            if side and o.get("side", "").upper() != side.value:
                continue
            price = o.get("price") or o.get("limit_price")
            size = o.get("size") or o.get("base_size")
            filled = o.get("filled_size") or o.get("filled", 0)
            out.append(
                Order(
                    id=o.get("order_id") or o.get("id"),
                    symbol=o.get("product_id", symbol),
                    side=Side(o.get("side", "BUY").upper()),
                    price=float(price),
                    qty=float(size),
                    filled_qty=float(filled),
                    status=o.get("status", "UNKNOWN"),
                )
            )
        return out

    async def cancel_order(self, order_id: str) -> None:
        """Cancel an order by id.  Coinbase ignores unknown order ids."""

        await self._async(self.rest.cancel_orders, order_ids=[order_id])

    async def place_limit_order(
        self,
        symbol: str,
        side: Side,
        qty: float,
        price: float,
        time_in_force: str = "IOC",
        post_only: bool = False,
        reduce_only: bool = False,
        client_order_id: Optional[str] = None,
    ) -> Order:
        """Submit a limit order and return the resulting :class:`Order` object."""

        params = dict(
            product_id=symbol,
            base_size=str(qty),
            limit_price=str(price),
            time_in_force=time_in_force,
            post_only=post_only,
            reduce_only=reduce_only,
            client_order_id=client_order_id,
        )

        if side is Side.BUY:
            resp = await self._async(self.rest.limit_order_buy, **params)
        else:
            resp = await self._async(self.rest.limit_order_sell, **params)

        data = resp.to_dict() if hasattr(resp, "to_dict") else resp
        o = data.get("success_response", {}).get("order", data.get("order", data))
        return Order(
            id=o.get("order_id") or o.get("id"),
            symbol=o.get("product_id", symbol),
            side=side,
            price=float(o.get("price") or o.get("limit_price")),
            qty=float(o.get("size") or o.get("base_size", qty)),
            filled_qty=float(o.get("filled_size") or 0),
            status=o.get("status", "NEW"),
        )

    # --- Market data ---
    @contextlib.asynccontextmanager
    async def top_of_book_feed(self, symbols: Sequence[str]) -> AsyncIterator[TopOfBook]:
        queue: aio.Queue[TopOfBook] = aio.Queue(maxsize=1000)

        def _on_msg(raw: str) -> None:
            """WS callback parsing ticker messages into ``TopOfBook`` objects."""
            try:
                msg = json.loads(raw)
            except Exception:
                return

            if msg.get("channel") != "ticker":
                return
            events = msg.get("events", [])
            for ev in events:
                if ev.get("product_id") not in symbols:
                    continue
                bid = ev.get("best_bid") or ev.get("bid_price")
                ask = ev.get("best_ask") or ev.get("ask_price")
                ts = ev.get("time")
                tob = TopOfBook(
                    symbol=ev.get("product_id"),
                    bid=float(bid),
                    ask=float(ask),
                    ts=float(ts) if ts is not None else time.time(),
                )
                try:
                    queue.put_nowait(tob)
                except aio.QueueFull:
                    pass

        self._ws = WSClient(on_message=_on_msg, api_key=self.api_key, api_secret=self.api_secret)
        await self._async(self._ws.open)
        await self._async(self._ws.ticker, product_ids=list(symbols))

        class _Iter:
            async def __anext__(self) -> TopOfBook:
                return await queue.get()

        try:
            yield _Iter()
        finally:
            if self._ws:
                with contextlib.suppress(Exception):
                    await self._async(self._ws.ticker_unsubscribe, product_ids=list(symbols))
                    await self._async(self._ws.close)
                self._ws = None
