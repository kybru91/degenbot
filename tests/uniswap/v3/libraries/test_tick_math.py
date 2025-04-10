from decimal import Decimal, getcontext
from math import floor, log

import pytest
from pydantic import ValidationError

from degenbot.constants import MAX_UINT160, MIN_UINT160
from degenbot.uniswap.v3_libraries.tick_math import (
    MAX_SQRT_RATIO,
    MAX_TICK,
    MIN_SQRT_RATIO,
    MIN_TICK,
    get_sqrt_ratio_at_tick,
    get_tick_at_sqrt_ratio,
)

# Tests adapted from Typescript tests on Uniswap V3 Github repo
# ref: https://github.com/Uniswap/v3-core/blob/main/test/TickMath.spec.ts

# Change the rounding method to match the BigNumber unit test at https://github.com/Uniswap/v3-core/blob/main/test/shared/utilities.ts
# which specifies .integerValue(3), the 'ROUND_FLOOR' rounding method per https://mikemcl.github.io/bignumber.js/#bignumber
getcontext().prec = 256
getcontext().rounding = "ROUND_FLOOR"


def encode_price_sqrt(reserve1: int, reserve0: int) -> int:
    """
    Returns the sqrt price as a Q64.96 value
    """
    return round((Decimal(reserve1) / Decimal(reserve0)).sqrt() * Decimal(2**96))


def test_get_sqrt_ratio_at_tick() -> None:
    with pytest.raises(ValidationError):
        get_sqrt_ratio_at_tick(MIN_TICK - 1)

    with pytest.raises(ValidationError):
        get_sqrt_ratio_at_tick(MAX_TICK + 1)

    assert get_sqrt_ratio_at_tick(MIN_TICK) == 4295128739

    assert get_sqrt_ratio_at_tick(MIN_TICK + 1) == 4295343490

    assert get_sqrt_ratio_at_tick(MAX_TICK - 1) == 1461373636630004318706518188784493106690254656249

    assert get_sqrt_ratio_at_tick(MIN_TICK) < (encode_price_sqrt(1, 2**127))

    assert get_sqrt_ratio_at_tick(MAX_TICK) > encode_price_sqrt(2**127, 1)

    assert get_sqrt_ratio_at_tick(MAX_TICK) == 1461446703485210103287273052203988822378723970342


def test_min_sqrt_ratio() -> None:
    assert get_sqrt_ratio_at_tick(MIN_TICK) == MIN_SQRT_RATIO


def test_max_sqrt_ratio() -> None:
    assert get_sqrt_ratio_at_tick(MAX_TICK) == MAX_SQRT_RATIO


def test_get_tick_at_sqrt_ratio() -> None:
    with pytest.raises(ValidationError):
        get_tick_at_sqrt_ratio(MIN_UINT160 - 1)

    with pytest.raises(ValidationError):
        get_tick_at_sqrt_ratio(MAX_UINT160 + 1)

    with pytest.raises(ValidationError):
        get_tick_at_sqrt_ratio(MIN_SQRT_RATIO - 1)

    with pytest.raises(ValidationError):
        get_tick_at_sqrt_ratio(MAX_SQRT_RATIO)

    assert (get_tick_at_sqrt_ratio(MIN_SQRT_RATIO)) == (MIN_TICK)
    assert (get_tick_at_sqrt_ratio(4295343490)) == (MIN_TICK + 1)

    assert (get_tick_at_sqrt_ratio(1461373636630004318706518188784493106690254656249)) == (
        MAX_TICK - 1
    )
    assert (get_tick_at_sqrt_ratio(MAX_SQRT_RATIO - 1)) == MAX_TICK - 1

    for ratio in [
        MIN_SQRT_RATIO,
        encode_price_sqrt((10) ** (12), 1),
        encode_price_sqrt((10) ** (6), 1),
        encode_price_sqrt(1, 64),
        encode_price_sqrt(1, 8),
        encode_price_sqrt(1, 2),
        encode_price_sqrt(1, 1),
        encode_price_sqrt(2, 1),
        encode_price_sqrt(8, 1),
        encode_price_sqrt(64, 1),
        encode_price_sqrt(1, (10) ** (6)),
        encode_price_sqrt(1, (10) ** (12)),
        MAX_SQRT_RATIO - 1,
    ]:
        math_result = floor(log(((ratio / 2**96) ** 2), 1.0001))
        result = get_tick_at_sqrt_ratio(ratio)
        abs_diff = abs(result - math_result)
        assert abs_diff <= 1

        tick = get_tick_at_sqrt_ratio(ratio)
        ratio_of_tick = get_sqrt_ratio_at_tick(tick)
        ratio_of_tick_plus_one = get_sqrt_ratio_at_tick(tick + 1)
        assert ratio >= ratio_of_tick
        assert ratio < ratio_of_tick_plus_one
