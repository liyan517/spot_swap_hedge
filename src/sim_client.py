import asyncio as aio
import contextlib
import random
import time
from typing import AsyncIterator, Sequence, Optional, List

from .exchange import ExchangeClient
from .models import Order, Position, Side, TopOfBook


class SimClient(ExchangeClient):
    """A deterministic simulator for local tests.

    - Price walks in a narrow band; orders at bid/ask fill immediately with IOC.
    - Positions update on each fill.
    - Useful to unit test core logic without a live venue.
    """

    def __init__(self):
        self._pos = {}
        self._orders: dict[str, Order] = {}
        self._price = {"BTC-USD": 60_000.0, "BTC-PERP": 60_000.0}

    # --- helper ---
    async def _tick(self, symbol: str) -> TopOfBook:
        """Random walk to simulate market."""
        base = self._price.get(symbol, 100.0)
        base *= (1 + random.uniform(-0.0005, 0.0005))
        self._price[symbol] = base
        spread = max(0.5, base * 0.0002)
        return TopOfBook(symbol=symbol, bid=base - spread / 2, ask=base + spread / 2, ts=time.time())

    # --- positions ---
    async def get_position(self, symbol: str) -> Position:
        return Position(symbol, self._pos.get(symbol, 0.0))

    # --- orders ---
    async def list_open_orders(self, symbol: str, side: Optional[Side] = None) -> List[Order]:
        out = [o for o in self._orders.values() if o.symbol == symbol and o.is_active]
        if side:
            out = [o for o in out if o.side == side]
        return out

    async def cancel_order(self, order_id: str) -> None:
        if order_id in self._orders:
            o = self._orders[order_id]
            o.status = "CANCELED"

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
        oid = client_order_id or f"sim-{len(self._orders)+1}"
        order = Order(id=oid, symbol=symbol, side=side, price=price, qty=qty, filled_qty=0.0, status="NEW")
        self._orders[oid] = order
        # Immediate-or-cancel at top-of-book fills instantly in this sim
        tob = await self._tick(symbol)
        if (side is Side.BUY and price >= tob.ask) or (side is Side.SELL and price <= tob.bid):
            order.filled_qty = qty
            order.status = "FILLED"
            self._pos[symbol] = self._pos.get(symbol, 0.0) + (qty if side is Side.BUY else -qty)
        else:
            # IOC not crossing -> canceled
            order.status = "CANCELED"
        return order

    @contextlib.asynccontextmanager
    async def top_of_book_feed(self, symbols: Sequence[str]) -> AsyncIterator[TopOfBook]:
        queue: aio.Queue[TopOfBook] = aio.Queue(maxsize=100)

        async def _producer():
            try:
                while True:
                    for s in symbols:
                        await queue.put(await self._tick(s))
                    await aio.sleep(0.05)
            except aio.CancelledError:
                pass

        task = aio.create_task(_producer())

        class _Iter:
            async def __anext__(self) -> TopOfBook:
                return await queue.get()

        try:
            yield _Iter()
        finally:
            task.cancel()
            with contextlib.suppress(Exception):
                await task
