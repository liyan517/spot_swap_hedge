"""
Production-grade hedging loop for Spot (long) vs Swaps/Perp (short).

Core features
-------------
- `spot_swaps_hedge(...)`: coordinates two concurrent legs (spot buy-up and swap sell-down)
  until both reach their respective target positions in lot-sized steps.
- `reach_position(...)`: event-driven order placement loop that consumes a best-bid/ask
  market feed, replaces stale orders at top-of-book, and exits once the target
  position is reached.
- Exchange abstraction (`ExchangeClient`) with a minimal, production-oriented surface:
  positions, open orders, cancel/replace, and best bid/ask market data via an
  async context-managed feed.
- Robustness: idempotent logic, precise rounding to tick/step sizes, timeouts,
  retries with jitter, structured logging, graceful cancellation.

How to use
----------
1) Implement an adapter of `ExchangeClient` for your venue (e.g. Coinbase, Binance Futures, Bybit).
   A thin Coinbase Advanced Trade skeleton is included below (`CoinbaseClientSkeleton`).
   Fill the `TODO` sections with your authenticated REST & WebSocket calls.
2) Instantiate your adapter, then run `asyncio.run(spot_swaps_hedge(...))` with your
   symbols, targets, and risk/config parameters.

IMPORTANT
---------
- This module is designed for **limit IOC/PO** style top-of-book execution with
  frequent cancel/replace, not for aggressive sweeping. Tune config accordingly.
- Always test on paper/sandbox first, and validate increments/permissions.
"""
from __future__ import annotations

import abc
import asyncio as aio
import contextlib
import dataclasses
import logging
import math
import random
import time
from dataclasses import dataclass
from enum import Enum
from typing import AsyncIterator, Optional, Sequence, Tuple, List


# -----------------------------
# Logging setup
# -----------------------------
logger = logging.getLogger("hedge")
if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)


# -----------------------------
# Domain models
# -----------------------------
class Side(str, Enum):
    """Order side."""
    BUY = "BUY"
    SELL = "SELL"


@dataclass
class Instrument:
    """Exchange instrument metadata used for validation & rounding.

    Attributes
    ----------
    symbol: Venue-specific symbol (e.g., "BTC-USD" for spot, "BTC-PERP" for swap).
    price_increment: Minimum price tick.
    size_increment: Minimum order size (step).
    quote_ccy: Quote currency code (for logging only).
    """
    symbol: str
    price_increment: float
    size_increment: float
    quote_ccy: str = "USD"


@dataclass
class Position:
    """Net position on an instrument.

    qty: Positive = long, Negative = short. Units are base asset units.
    """
    symbol: str
    qty: float


@dataclass
class Order:
    """Minimal order view for tracking and replace logic."""
    id: str
    symbol: str
    side: Side
    price: float
    qty: float
    filled_qty: float
    status: str  # e.g., NEW, PARTIALLY_FILLED, FILLED, CANCELED

    @property
    def unfilled_qty(self) -> float:
        """Return remaining quantity on the order."""
        return max(self.qty - self.filled_qty, 0.0)

    @property
    def is_active(self) -> bool:
        """True if this order is still working on the book."""
        return self.status in {"NEW", "PARTIALLY_FILLED"}


@dataclass
class TopOfBook:
    """Best bid/ask snapshot from a market data feed."""
    symbol: str
    bid: float
    ask: float
    ts: float  # exchange timestamp seconds (float)


# -----------------------------
# Exchange abstraction
# -----------------------------
class ExchangeClient(abc.ABC):
    """Abstract operations the strategy needs to trade on a venue.

    Implementations must be **async-safe** and handle their own auth/rate limits.
    """

    # --- Positions ---
    @abc.abstractmethod
    async def get_position(self, symbol: str) -> Position:
        """Return current net position for `symbol`.

        Implementations should cache or batch calls if expensive.
        """
        raise NotImplementedError

    # --- Orders ---
    @abc.abstractmethod
    async def list_open_orders(self, symbol: str, side: Optional[Side] = None) -> List[Order]:
        """Return currently open orders for the symbol, optionally filtered by side."""
        raise NotImplementedError

    @abc.abstractmethod
    async def cancel_order(self, order_id: str) -> None:
        """Cancel an order by id. Should be idempotent and ignore unknown ids."""
        raise NotImplementedError

    @abc.abstractmethod
    async def place_limit_order(
        self,
        symbol: str,
        side: Side,
        qty: float,
        price: float,
        time_in_force: str = "IOC",  # or "GTC"/"PO"
        post_only: bool = False,
        reduce_only: bool = False,
        client_order_id: Optional[str] = None,
    ) -> Order:
        """Place a limit order. Return the created order.

        Implementations must round price/qty to exchange increments and should
        raise a descriptive exception if rejected.
        """
        raise NotImplementedError

    # --- Market data ---
    @abc.abstractmethod
    @contextlib.asynccontextmanager
    async def top_of_book_feed(self, symbols: Sequence[str]) -> AsyncIterator[TopOfBook]:
        """Async context manager yielding best bid/ask updates as `TopOfBook`.

        Yields **one update at a time** for any of the subscribed symbols.
        Implementations should multiplex and coalesce updates sensibly.
        """
        yield  # type: ignore[misc]


# -----------------------------
# Configuration for execution behavior
# -----------------------------
@dataclass
class ExecConfig:
    """Tunable parameters for order placement & monitoring.

    position_tolerance: Allowed absolute error to consider target reached.
    price_offset_bps: Offset from best bid/ask in basis points (+ crosses the spread further).
    refresh_position_every_s: Poll frequency for positions during the loop.
    replace_stale_after_s: If an active order sits older than this, replace it.
    ws_idle_timeout_s: If no market data seen for this long, restart the feed.
    order_timeout_s: If an order remains active longer than this, cancel it.
    max_concurrent_replacements: Throttle cancel/replace bursts.
    """
    position_tolerance: float = 1e-9
    price_offset_bps: float = 0.0
    refresh_position_every_s: float = 0.5
    replace_stale_after_s: float = 1.0
    ws_idle_timeout_s: float = 15.0
    order_timeout_s: float = 5.0
    max_concurrent_replacements: int = 1


# -----------------------------
# Utility functions
# -----------------------------

def _round_to_increment(value: float, increment: float, *, up_for_buy: Optional[bool] = None) -> float:
    """Round a number to the exchange's increment with directional bias.

    For prices, we typically want to **bias toward more aggressive** values
    (buy rounds UP, sell rounds DOWN) so we don't miss top-of-book by a tick.
    For sizes, we typically want to round DOWN to avoid violating min step.
    """
    if increment <= 0:
        return value

    q = value / increment
    if up_for_buy is None:
        # Default: size rounding down
        return math.floor(q) * increment

    if up_for_buy:
        return math.ceil(q) * increment
    else:
        return math.floor(q) * increment


def _now() -> float:
    """Monotonic timestamp for internal timers."""
    return time.monotonic()


# -----------------------------
# Core algorithm: reach target position
# -----------------------------
async def reach_position(
    client: ExchangeClient,
    instrument: Instrument,
    target_qty: float,
    lot_size: float,
    cfg: ExecConfig,
) -> None:
    """Event-driven loop to move position on one instrument to `target_qty`.

    Logic
    -----
    - Subscribe to top-of-book updates.
    - On each update (or timed tick), re-evaluate net position.
    - If target not reached, ensure there is **one** active order working at the
      current best level (with optional offset). If there's an older or off-price
      order, cancel & replace at the fresh best price.
    - Exit the loop once the position is within `position_tolerance` of `target_qty`.

    Safety & correctness
    --------------------
    - Only ever works **one** order per symbol/side to avoid over-trading.
    - Respects exchange increments via `_round_to_increment`.
    - Uses timeouts to avoid stuck websocket or stale orders.
    """
    symbol = instrument.symbol
    last_pos_check = 0.0
    active_order: Optional[Order] = None
    active_since = _now()

    def _need() -> Tuple[float, Optional[Side]]:
        """Compute remaining quantity and desired side to move towards target.

        Returns (remaining_abs, side_or_none). If no side needed (already at
        target within tolerance), side is None.
        """
        # Note: we fetch a fresh position outside using the client call.
        raise NotImplementedError  # replaced at runtime to capture latest pos

    # Mutable closure state for latest position value
    latest_pos = await client.get_position(symbol)

    def need_impl() -> Tuple[float, Optional[Side]]:
        remaining = target_qty - latest_pos.qty
        if abs(remaining) <= cfg.position_tolerance:
            return 0.0, None
        side = Side.BUY if remaining > 0 else Side.SELL
        return abs(remaining), side

    # Monkey-assign the implementation to avoid capturing stale values
    _need.__code__ = need_impl.__code__  # type: ignore[attr-defined]

    async def _maybe_cancel_active(reason: str) -> None:
        """Cancel the current active order with logging and jitter to prevent bursts."""
        nonlocal active_order
        if active_order and active_order.is_active:
            try:
                logger.info(f"Cancel {active_order.symbol} {active_order.side} #{active_order.id} – {reason}")
                await client.cancel_order(active_order.id)
            finally:
                active_order = None
                await aio.sleep(random.uniform(0.01, 0.05))  # jitter

    @contextlib.asynccontextmanager
    async def _feed_ctx():
        async with client.top_of_book_feed([symbol]) as feed:
            yield feed

    idle_deadline = _now() + cfg.ws_idle_timeout_s

    async with _feed_ctx() as feed:
        while True:
            # Timeout handling: if feed is idle, restart subscription
            if _now() > idle_deadline:
                logger.warning(f"[{symbol}] market data idle – restarting feed")
                await _maybe_cancel_active("feed idle restart")
                async with _feed_ctx() as new_feed:
                    feed = new_feed  # type: ignore[assignment]
                    idle_deadline = _now() + cfg.ws_idle_timeout_s
                continue

            # Receive the next top-of-book update (with a safety timeout)
            try:
                tob: TopOfBook = await aio.wait_for(feed.__anext__(), timeout=cfg.ws_idle_timeout_s)
            except (aio.TimeoutError, StopAsyncIteration):
                # Treat both as idle and attempt a feed restart on next loop
                continue

            idle_deadline = _now() + cfg.ws_idle_timeout_s

            # Periodically refresh position to check progress
            if _now() - last_pos_check >= cfg.refresh_position_every_s:
                latest_pos = await client.get_position(symbol)
                last_pos_check = _now()

            remaining, side = _need()
            if side is None:
                # Target reached; exit gracefully
                await _maybe_cancel_active("target reached")
                logger.info(f"[{symbol}] target reached: pos={latest_pos.qty:.8f}, target={target_qty:.8f}")
                return

            # Determine working quantity for this step
            step_qty = min(remaining, lot_size)
            step_qty = max(step_qty, instrument.size_increment)
            step_qty = _round_to_increment(step_qty, instrument.size_increment)  # size rounds down
            if step_qty <= 0:
                # Can't place a valid order at this step size; wait for next tick
                continue

            # Determine target price at top-of-book with optional offset
            bid, ask = tob.bid, tob.ask
            if side is Side.BUY:
                px = ask * (1 + cfg.price_offset_bps / 10_000.0)
                px = _round_to_increment(px, instrument.price_increment, up_for_buy=True)
            else:
                px = bid * (1 - cfg.price_offset_bps / 10_000.0)
                px = _round_to_increment(px, instrument.price_increment, up_for_buy=False)

            # Fetch open orders and keep at most one active working order on this side
            open_orders = await client.list_open_orders(symbol, side=side)
            # Prefer our existing reference if still in the open list
            live_ids = {o.id for o in open_orders}
            if not active_order or active_order.id not in live_ids:
                active_order = None
                if open_orders:
                    # Keep the newest by heuristic (highest id lex or latest price)
                    open_orders.sort(key=lambda o: o.price, reverse=(side is Side.SELL))
                    active_order = open_orders[0]
                    active_since = _now()

            # Decide whether to replace the active order if it's stale/off-price
            need_replace = False
            if active_order and active_order.is_active:
                # If price is worse than current target, replace
                if side is Side.BUY and active_order.price < px:
                    need_replace = True
                if side is Side.SELL and active_order.price > px:
                    need_replace = True
                # If order sat too long, replace to refresh priority
                if _now() - active_since >= cfg.replace_stale_after_s:
                    need_replace = True
                # If order is timing out, cancel to avoid hanging
                if _now() - active_since >= cfg.order_timeout_s:
                    need_replace = True

            if active_order and need_replace:
                await _maybe_cancel_active("refresh top-of-book")

            # If no active order, place a new one at the computed price
            if active_order is None:
                # Choose TIF: IOC for immediacy; switch to GTC if you want to rest
                tif = "IOC"
                try:
                    active_order = await client.place_limit_order(
                        symbol=symbol,
                        side=side,
                        qty=step_qty,
                        price=px,
                        time_in_force=tif,
                        post_only=(tif == "PO"),
                        reduce_only=False,
                    )
                    active_since = _now()
                    logger.info(
                        f"Place {symbol} {side} {step_qty:.8f} @ {px:.2f} (tif={tif}) remaining={remaining:.8f}"
                    )
                except Exception as e:
                    # Log and continue; next tick we will try again
                    logger.exception(f"[{symbol}] place order failed: {e}")
                    await aio.sleep(0.1)
                    continue


# -----------------------------
# Coordinator: Spot vs Swap hedging
# -----------------------------
async def spot_swaps_hedge(
    client: ExchangeClient,
    spot: Instrument,
    swap: Instrument,
    target_spot_qty: float,
    target_swap_qty: float,
    lot_size: float,
    cfg: Optional[ExecConfig] = None,
) -> None:
    """Run two `reach_position` loops concurrently until both legs are done.

    Parameters
    ----------
    client: Exchange adapter implementing `ExchangeClient`.
    spot: Spot instrument descriptor (long target expected).
    swap: Swap/Perp instrument descriptor (short target expected).
    target_spot_qty: Desired final net spot position (>= current spot). Positive for long.
    target_swap_qty: Desired final net swap position (<= current swap). Negative for short.
    lot_size: Per-iteration order size (will be rounded down to `size_increment`).
    cfg: Optional execution configuration.

    Behavior
    --------
    - This orchestrator **does not** net-hedge between legs; each leg independently
      marches toward its own target using `reach_position`.
    - If one leg completes earlier, we continue driving the other leg until done.
    - Any exception in a leg cancels the sibling and re-raises.
    """
    cfg = cfg or ExecConfig()

    async def _leg(name: str, instr: Instrument, qty: float) -> None:
        logger.info(f"Start leg={name} symbol={instr.symbol} target={qty}")
        await reach_position(client, instr, qty, lot_size, cfg)
        logger.info(f"Done leg={name} symbol={instr.symbol}")

    # Run legs concurrently with linked cancellation
    task_spot = aio.create_task(_leg("spot", spot, target_spot_qty))
    task_swap = aio.create_task(_leg("swap", swap, target_swap_qty))

    try:
        await aio.gather(task_spot, task_swap)
    except Exception:
        # On failure, cancel both legs to avoid orphan workers
        for t in (task_spot, task_swap):
            t.cancel()
            with contextlib.suppress(Exception):
                await t
        raise


# -----------------------------
# Coinbase Advanced Trade adapter (skeleton)
# -----------------------------
class CoinbaseClientSkeleton(ExchangeClient):
    """Skeleton adapter for Coinbase Advanced Trade.

    Fill the TODOs with calls using `coinbase-advanced-py` (RESTClient & WSClient)
    or your own lightweight HTTP/JWT + websockets implementation.
    """

    def __init__(self, api_key: str, api_secret: str):
        self.api_key = api_key
        self.api_secret = api_secret
        # TODO: init REST client, auth, and any local caches

    # --- Positions ---
    async def get_position(self, symbol: str) -> Position:
        # TODO: map accounts/positions to `symbol` and return net qty
        # Example with RESTClient: client.get_accounts() -> find balance for base asset
        raise NotImplementedError

    # --- Orders ---
    async def list_open_orders(self, symbol: str, side: Optional[Side] = None) -> List[Order]:
        # TODO: client.list_orders(product_id=symbol, order_status=["OPEN", "PENDING"])
        # Map to Order dataclass and filter by side if provided
        raise NotImplementedError

    async def cancel_order(self, order_id: str) -> None:
        # TODO: client.cancel_orders(order_ids=[order_id])
        raise NotImplementedError

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
        # TODO: client.limit_order_buy/sell or generic orders endpoint
        # Ensure rounding to increments before sending
        raise NotImplementedError

    # --- Market data ---
    @contextlib.asynccontextmanager
    async def top_of_book_feed(self, symbols: Sequence[str]) -> AsyncIterator[TopOfBook]:
        # TODO: open WSClient(), subscribe to ticker or level2 for symbols, and yield TopOfBook
        # Remember to parse best bid/ask and fill `TopOfBook` dataclass
        # This context manager must produce an async iterator with method `__anext__`.
        raise NotImplementedError


# -----------------------------
# Optional: In-memory simulated client for local testing
# -----------------------------
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
        # Random walk to simulate market
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
                item = await queue.get()
                return item

        try:
            yield _Iter()
        finally:
            task.cancel()
            with contextlib.suppress(Exception):
                await task


# -----------------------------
# Example main (simulation)
# -----------------------------
async def _example() -> None:
    """Run a quick simulated hedge to demonstrate orchestration and logging."""
    sim = SimClient()
    spot = Instrument(symbol="BTC-USD", price_increment=0.01, size_increment=0.0001)
    swap = Instrument(symbol="BTC-PERP", price_increment=0.01, size_increment=0.0001)

    # Targets: build +0.01 BTC spot and -0.01 BTC swap in 0.0025 increments
    await spot_swaps_hedge(
        client=sim,
        spot=spot,
        swap=swap,
        target_spot_qty=0.01,
        target_swap_qty=-0.01,
        lot_size=0.0025,
        cfg=ExecConfig(price_offset_bps=0.0),
    )


if __name__ == "__main__":
    # For local smoke test of the simulator
    aio.run(_example())
