from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class Side(str, Enum):
    """Order side."""
    BUY = "BUY"
    SELL = "SELL"


@dataclass
class Instrument:
    """Exchange instrument metadata used for validation & rounding.

    Attributes
    ----------
    symbol:
        Venue-specific symbol (e.g., ``"BTC-USD"`` for spot,
        ``"BTC-PERP"`` for swap).
    price_increment:
        Minimum price tick allowed by the venue.
    size_increment:
        Minimum order size (step).
    quote_ccy:
        Quote currency code (for logging only).
    """
    symbol: str
    price_increment: float
    size_increment: float
    quote_ccy: str = "USD"


@dataclass
class Position:
    """Net position on an instrument.

    qty:
        Positive = long, Negative = short. Units are base asset units.
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


@dataclass
class ExecConfig:
    """Tunable parameters for order placement & monitoring.

    position_tolerance:
        Allowed absolute error to consider target reached.
    price_offset_bps:
        Offset from best bid/ask in basis points (+ crosses the spread further).
    refresh_position_every_s:
        Poll frequency for positions during the loop.
    replace_stale_after_s:
        If an active order sits older than this, replace it.
    ws_idle_timeout_s:
        If no market data seen for this long, restart the feed.
    order_timeout_s:
        If an order remains active longer than this, cancel it.
    max_concurrent_replacements:
        Throttle cancel/replace bursts.
    """
    position_tolerance: float = 1e-9
    price_offset_bps: float = 0.0
    refresh_position_every_s: float = 0.5
    replace_stale_after_s: float = 1.0
    ws_idle_timeout_s: float = 15.0
    order_timeout_s: float = 5.0
    max_concurrent_replacements: int = 1
