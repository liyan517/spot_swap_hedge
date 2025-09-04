import logging
import math
import time
from typing import Optional


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
# Utility functions
# -----------------------------
def round_to_increment(value: float, increment: float, *, up_for_buy: Optional[bool] = None) -> float:
    """Round a number to the exchange's increment with directional bias.

    For prices, we typically want to **bias toward more aggressive** values
    (buy rounds UP, sell rounds DOWN) so we don't miss top-of-book by a tick.
    For sizes, we typically want to round DOWN to avoid violating min step.
    """
    if increment <= 0:
        return value

    q = value / increment
    if up_for_buy is None:
        return math.floor(q) * increment

    if up_for_buy:
        return math.ceil(q) * increment
    return math.floor(q) * increment


def now() -> float:
    """Monotonic timestamp for internal timers."""
    return time.monotonic()
