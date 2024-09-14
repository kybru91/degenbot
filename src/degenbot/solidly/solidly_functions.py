from collections.abc import Sequence
from fractions import Fraction
from typing import Literal

import eth_abi.packed
from eth_typing import ChecksumAddress
from eth_utils.crypto import keccak
from hexbytes import HexBytes

from degenbot.constants import MAX_UINT256, MIN_UINT256

from ..exceptions import EVMRevertError
from ..functions import eip_1167_clone_address


def raise_if_invalid_uint256(number: int) -> None:
    if (MIN_UINT256 <= number <= MAX_UINT256) is False:
        raise EVMRevertError(f"underflow/overflow for {number}")


def _d(
    x0: int,
    y: int,
) -> int:
    return (3 * x0 * ((y * y) // 10**18)) // 10**18 + ((((x0 * x0) // 10**18) * x0) // 10**18)


def _f_aerodrome(
    x0: int,
    y: int,
) -> int:
    _a = (x0 * y) // 10**18
    _b = (x0 * x0) // 10**18 + (y * y) // 10**18
    return (_a * _b) // 10**18


def _f_camelot(x0: int, y: int) -> int:
    return (
        x0 * (y * y // 10**18 * y // 10**18) // 10**18
        + (x0 * x0 // 10**18 * x0 // 10**18) * y // 10**18
    )


def _get_y_aerodrome(
    x0: int,
    xy: int,
    y: int,
    decimals0: int,
    decimals1: int,
) -> int:
    """
    Calculate the minimum reserves for the withdrawn token that satisfy the pool invariant.

    Reference: https://github.com/aerodrome-finance/contracts/blob/main/contracts/Pool.sol
    """

    for _ in range(255):
        k = _f_aerodrome(x0, y)
        if k < xy:
            dy = ((xy - k) * 10**18) // _d(x0, y)
            if dy == 0:
                if k == xy:
                    return y
                if (
                    _k_aerodrome(
                        balance_0=x0, balance_1=y + 1, decimals_0=decimals0, decimals_1=decimals1
                    )
                    > xy
                ):
                    return y + 1
                dy = 1
            y = y + dy
        else:
            dy = ((k - xy) * 10**18) // _d(x0, y)
            if dy == 0:
                if k == xy or _f_aerodrome(x0, y - 1) < xy:
                    return y
                dy = 1
            y = y - dy
    raise EVMRevertError("Failed to converge on a value for y")


def _get_y_camelot(
    x_0: int,
    xy: int,
    y: int,
) -> int:
    for _ in range(255):
        y_prev = y
        k = _f_camelot(x_0, y)
        if k < xy:
            dy = (xy - k) * 10**18 // _d(x_0, y)

            y = y + dy
        else:
            dy = (k - xy) * 10**18 // _d(x_0, y)
            y = y - dy

        if y > y_prev:
            if y - y_prev <= 1:
                return y
        elif y_prev - y <= 1:
            return y
    return y


def _k_aerodrome(
    balance_0: int,
    balance_1: int,
    decimals_0: int,
    decimals_1: int,
) -> int:
    _x = balance_0 * 10**18 // decimals_0
    _y = balance_1 * 10**18 // decimals_1
    _a = _x * _y // 10**18
    _b = (_x * _x // 10**18) + (_y * _y // 10**18)

    raise_if_invalid_uint256(_a * _b)
    return _a * _b // 10**18  # x^3*y + y^3*x >= k


def generate_aerodrome_v2_pool_address(
    deployer_address: str | bytes,
    token_addresses: Sequence[str | bytes],
    implementation_address: str | bytes,
    stable: bool,
) -> ChecksumAddress:
    """
    Get the deterministic V2 pool address generated by CREATE2. Uses the token address to generate
    the salt. The token addresses can be passed in any order.

    Adapted from https://github.com/aerodrome-finance/contracts/blob/main/contracts/factories/PoolFactory.sol
    and https://github.com/OpenZeppelin/openzeppelin-contracts/blob/master/contracts/proxy/Clones.sol
    """

    sorted_token_addresses = sorted([HexBytes(address) for address in token_addresses])

    salt = keccak(
        eth_abi.packed.encode_packed(
            ("address", "address", "bool"),
            [*sorted_token_addresses, stable],
        )
    )

    return eip_1167_clone_address(
        deployer=deployer_address,
        implementation_contract=implementation_address,
        salt=salt,
    )


def solidly_calc_exact_in_stable(
    amount_in: int,
    token_in: Literal[0, 1],
    reserves0: int,
    reserves1: int,
    decimals0: int,
    decimals1: int,
    fee: Fraction,
) -> int:
    """
    Calculate the amount out for an exact input to a Solidly stable pool (invariant
    y*x^3 + x*y^3 >= k).
    """

    try:
        amount_in_after_fee = amount_in - amount_in * fee.numerator // fee.denominator

        xy = _k_aerodrome(
            balance_0=reserves0, balance_1=reserves1, decimals_0=decimals0, decimals_1=decimals1
        )

        scaled_reserves_0 = (reserves0 * 10**18) // decimals0
        scaled_reserves_1 = (reserves1 * 10**18) // decimals1

        if token_in == 0:
            reserves_a, reserves_b = scaled_reserves_0, scaled_reserves_1
            amount_in_after_fee = (amount_in_after_fee * 10**18) // decimals0
        elif token_in == 1:
            reserves_a, reserves_b = scaled_reserves_1, scaled_reserves_0
            amount_in_after_fee = (amount_in_after_fee * 10**18) // decimals1
        else:
            raise ValueError("Invalid token_in identifier")

        y = reserves_b - _get_y_aerodrome(
            amount_in_after_fee + reserves_a, xy, reserves_b, decimals0, decimals1
        )
        return (y * (decimals1 if token_in == 0 else decimals0)) // 10**18
    except ZeroDivisionError:
        # Pools with very low reserves can throw division by zero errors because _d() returns 0
        raise EVMRevertError("Division by zero") from None


def solidly_calc_exact_in_volatile(
    amount_in: int,
    token_in: Literal[0, 1],
    reserves0: int,
    reserves1: int,
    fee: Fraction,
) -> int:
    """
    Calculate the amount out for an exact input to a Solidly volatile pool (invariant x*y>=k).
    """

    amount_in_after_fee = amount_in - amount_in * fee.numerator // fee.denominator

    if token_in == 0:
        reserves_a, reserves_b = reserves0, reserves1
    elif token_in == 1:
        reserves_a, reserves_b = reserves1, reserves0
    else:
        raise ValueError("Invalid token_in identifier")

    return (amount_in_after_fee * reserves_b) // (reserves_a + amount_in_after_fee)