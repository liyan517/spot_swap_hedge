import abc
import contextlib
from typing import AsyncIterator, Optional, Sequence, List

from .models import Position, Order, Side, TopOfBook


class ExchangeClient(abc.ABC):
    """Abstract operations the strategy needs to trade on a venue.

    Implementations must be **async-safe** and handle their own auth/rate limits.
    """

    # --- Positions ---
    @abc.abstractmethod
    async def get_position(self, symbol: str) -> Position:
        """Return current net position for ``symbol``.

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
        """Async context manager yielding best bid/ask updates as ``TopOfBook``.

        Yields **one update at a time** for any of the subscribed symbols.
        Implementations should multiplex and coalesce updates sensibly.
        """
        yield  # type: ignore[misc]
