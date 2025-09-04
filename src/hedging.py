from __future__ import annotations

import asyncio as aio
import contextlib
import random
from typing import Optional, Tuple

from .exchange import ExchangeClient
from .models import Instrument, ExecConfig, Side, Order, TopOfBook
from .utils import logger, round_to_increment, now


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
    """Event-driven loop to move position on one instrument to ``target_qty``.

    Logic
    -----
    - Subscribe to top-of-book updates.
    - On each update (or timed tick), re-evaluate net position.
    - If target not reached, ensure there is **one** active order working at the
      current best level (with optional offset). If there's an older or off-price
      order, cancel & replace at the fresh best price.
    - Exit the loop once the position is within ``position_tolerance`` of
      ``target_qty``.

    Safety & correctness
    --------------------
    - Only ever works **one** order per symbol/side to avoid over-trading.
    - Respects exchange increments via ``round_to_increment``.
    - Uses timeouts to avoid stuck websocket or stale orders.
    """
    symbol = instrument.symbol
    last_pos_check = 0.0
    active_order: Optional[Order] = None
    active_since = now()

    latest_pos = await client.get_position(symbol)

    def need() -> Tuple[float, Optional[Side]]:
        """Compute remaining quantity and desired side to move towards target.

        Returns ``(remaining_abs, side_or_none)``. If no side needed (already at
        target within tolerance), side is ``None``.
        """
        remaining = target_qty - latest_pos.qty
        if abs(remaining) <= cfg.position_tolerance:
            return 0.0, None
        side = Side.BUY if remaining > 0 else Side.SELL
        return abs(remaining), side

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

    idle_deadline = now() + cfg.ws_idle_timeout_s

    async with _feed_ctx() as feed:
        while True:
            # Timeout handling: if feed is idle, restart subscription
            if now() > idle_deadline:
                logger.warning(f"[{symbol}] market data idle – restarting feed")
                await _maybe_cancel_active("feed idle restart")
                async with _feed_ctx() as new_feed:
                    feed = new_feed  # type: ignore[assignment]
                    idle_deadline = now() + cfg.ws_idle_timeout_s
                continue

            # Receive the next top-of-book update (with a safety timeout)
            try:
                tob: TopOfBook = await aio.wait_for(feed.__anext__(), timeout=cfg.ws_idle_timeout_s)
            except (aio.TimeoutError, StopAsyncIteration):
                # Treat both as idle and attempt a feed restart on next loop
                continue

            idle_deadline = now() + cfg.ws_idle_timeout_s

            # Periodically refresh position to check progress
            if now() - last_pos_check >= cfg.refresh_position_every_s:
                latest_pos = await client.get_position(symbol)
                last_pos_check = now()

            remaining, side = need()
            if side is None:
                await _maybe_cancel_active("target reached")
                logger.info(
                    f"[{symbol}] target reached: pos={latest_pos.qty:.8f}, target={target_qty:.8f}"
                )
                return

            # Determine working quantity for this step
            step_qty = min(remaining, lot_size)
            step_qty = max(step_qty, instrument.size_increment)
            step_qty = round_to_increment(step_qty, instrument.size_increment)  # size rounds down
            if step_qty <= 0:
                # Can't place a valid order at this step size; wait for next tick
                continue

            # Determine target price at top-of-book with optional offset
            bid, ask = tob.bid, tob.ask
            if side is Side.BUY:
                px = ask * (1 + cfg.price_offset_bps / 10_000.0)
                px = round_to_increment(px, instrument.price_increment, up_for_buy=True)
            else:
                px = bid * (1 - cfg.price_offset_bps / 10_000.0)
                px = round_to_increment(px, instrument.price_increment, up_for_buy=False)

            # Fetch open orders and keep at most one active working order on this side
            open_orders = await client.list_open_orders(symbol, side=side)
            live_ids = {o.id for o in open_orders}
            if not active_order or active_order.id not in live_ids:
                active_order = None
                if open_orders:
                    open_orders.sort(key=lambda o: o.price, reverse=(side is Side.SELL))
                    active_order = open_orders[0]
                    active_since = now()

            # Decide whether to replace the active order if it's stale/off-price
            need_replace = False
            if active_order and active_order.is_active:
                # If price is worse than current target, replace
                if side is Side.BUY and active_order.price < px:
                    need_replace = True
                if side is Side.SELL and active_order.price > px:
                    need_replace = True
                # If order sat too long, replace to refresh priority
                if now() - active_since >= cfg.replace_stale_after_s:
                    need_replace = True
                # If order is timing out, cancel to avoid hanging
                if now() - active_since >= cfg.order_timeout_s:
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
                    active_since = now()
                    logger.info(
                        f"Place {symbol} {side} {step_qty:.8f} @ {px:.2f} (tif={tif}) remaining={remaining:.8f}"
                    )
                except Exception as e:
                    # Log and continue; next tick we will try again
                    logger.exception(f"[{symbol}] place order failed: {e}")
                    await aio.sleep(0.1)
                    continue


async def spot_swaps_hedge(
    client: ExchangeClient,
    spot: Instrument,
    swap: Instrument,
    target_spot_qty: float,
    target_swap_qty: float,
    lot_size: float,
    cfg: Optional[ExecConfig] = None,
) -> None:
    """Run two ``reach_position`` loops concurrently until both legs are done.

    Parameters
    ----------
    client:
        Exchange adapter implementing ``ExchangeClient``.
    spot:
        Spot instrument descriptor (long target expected).
    swap:
        Swap/Perp instrument descriptor (short target expected).
    target_spot_qty:
        Desired final net spot position (>= current spot). Positive for long.
    target_swap_qty:
        Desired final net swap position (<= current swap). Negative for short.
    lot_size:
        Per-iteration order size (will be rounded down to ``size_increment``).
    cfg:
        Optional execution configuration.

    Behavior
    --------
    - This orchestrator **does not** net-hedge between legs; each leg
      independently marches toward its own target using ``reach_position``.
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
