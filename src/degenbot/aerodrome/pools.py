from fractions import Fraction
from threading import Lock
from typing import Any, Literal, TypeAlias, cast

from eth_typing import ChecksumAddress
from eth_utils.address import to_checksum_address
from typing_extensions import override
from web3 import Web3
from web3.types import BlockIdentifier

from .. import config
from ..erc20_token import Erc20Token
from ..exceptions import AddressMismatch, DegenbotValueError, ZeroSwapError
from ..functions import encode_function_calldata, get_number_for_block_identifier, raw_call
from ..logging import logger
from ..managers.erc20_token_manager import Erc20TokenManager
from ..registry.all_pools import AllPools
from ..solidly.solidly_functions import general_calc_exact_in_volatile
from ..types import AbstractLiquidityPool
from ..uniswap.v3_liquidity_pool import UniswapV3Pool
from .functions import (
    calc_exact_in_stable,
    generate_aerodrome_v2_pool_address,
    generate_aerodrome_v3_pool_address,
)
from .types import AerodromeV2PoolState


class AerodromeV2Pool(AbstractLiquidityPool):
    FEE_DENOMINATOR = 10_000

    def __init__(
        self,
        address: ChecksumAddress | str,
        tokens: list[Erc20Token] | None = None,
        factory_address: str | None = None,
        deployer_address: str | None = None,
        fee: Fraction | None = None,
        silent: bool = False,
        state_block: int | None = None,
        archive_states: bool = True,
        verify_address: bool = True,
    ) -> None:
        self.address = to_checksum_address(address)

        self._state_lock = Lock()
        self.state = AerodromeV2PoolState(
            pool=self.address,
            reserves_token0=0,
            reserves_token1=0,
        )

        w3 = config.get_web3()
        self.update_block = state_block if state_block is not None else w3.eth.block_number
        chain_id = w3.eth.chain_id

        self.factory = (
            to_checksum_address(factory_address)
            if factory_address is not None
            else to_checksum_address(self.get_factory(w3=w3, block_identifier=self.update_block))
        )
        self.deployer_address = (
            to_checksum_address(deployer_address) if deployer_address is not None else self.factory
        )

        self.stable: bool = self.get_stable(w3=w3, block_identifier=self.update_block)
        self.fee = (
            fee
            if fee is not None
            else Fraction(
                self.get_fee(w3=w3, block_identifier=self.update_block),
                self.FEE_DENOMINATOR,
            )
        )

        token_manager = Erc20TokenManager(chain_id)

        self.token0, self.token1 = (
            sorted(tokens)
            if tokens is not None
            else (
                token_manager.get_erc20token(
                    address=self.get_token0(w3=w3, block_identifier=self.update_block),
                    silent=silent,
                ),
                token_manager.get_erc20token(
                    address=self.get_token1(w3=w3, block_identifier=self.update_block),
                    silent=silent,
                ),
            )
        )

        if verify_address and self.address != self._verified_address():  # pragma: no branch
            raise AddressMismatch("Pool address verification failed.")

        self.name = f"{self.token0}-{self.token1} (AerodromeV2, {100*self.fee.numerator/self.fee.denominator:.2f}%)"  # noqa:E501
        self.reserves_token0, self.reserves_token1 = self.get_reserves(
            w3=w3, block_identifier=self.update_block
        )

        self._pool_state_archive = {self.update_block: self.state} if archive_states else None

        AllPools(chain_id)[self.address] = self

        self._subscribers = set()

        if not silent:  # pragma: no cover
            logger.info(self.name)
            logger.info(f"• Token 0: {self.token0} - Reserves: {self.reserves_token0}")
            logger.info(f"• Token 1: {self.token1} - Reserves: {self.reserves_token1}")

    def __getstate__(self) -> dict[str, Any]:
        # Remove objects that either cannot be pickled or are unnecessary to perform the calculation
        copied_attributes = ()
        dropped_attributes = (
            "_state_lock",
            "_subscribers",
            "_pool_state_archive",
        )

        with self._state_lock:
            return {
                k: (v.copy() if k in copied_attributes else v)
                for k, v in self.__dict__.items()
                if k not in dropped_attributes
            }

    def _verified_address(self) -> ChecksumAddress:
        # The implementation address is hard-coded into the contract
        implementation_address = to_checksum_address(
            config.get_web3().eth.get_code(self.address)[10:30]
        )

        return generate_aerodrome_v2_pool_address(
            deployer_address=self.deployer_address,
            token_addresses=(self.token0.address, self.token1.address),
            implementation_address=to_checksum_address(implementation_address),
            stable=self.stable,
        )

    @property
    def reserves_token0(self) -> int:
        return self.state.reserves_token0

    @reserves_token0.setter
    def reserves_token0(self, new_reserves: int) -> None:
        current_state = self.state
        self.state = AerodromeV2PoolState(
            pool=current_state.pool,
            reserves_token0=new_reserves,
            reserves_token1=current_state.reserves_token1,
        )

    @property
    def reserves_token1(self) -> int:
        return self.state.reserves_token1

    @reserves_token1.setter
    def reserves_token1(self, new_reserves: int) -> None:
        current_state = self.state
        self.state = AerodromeV2PoolState(
            pool=current_state.pool,
            reserves_token0=current_state.reserves_token0,
            reserves_token1=new_reserves,
        )

    @property
    def state(self) -> AerodromeV2PoolState:
        return self._state

    @state.setter
    @override
    def state(self, new_state: AerodromeV2PoolState) -> None:
        self._state = new_state

    @property
    def tokens(self) -> tuple[Erc20Token, Erc20Token]:
        return self.token0, self.token1

    def calculate_tokens_out_from_tokens_in(
        self,
        token_in: Erc20Token,
        token_in_quantity: int,
        override_state: AerodromeV2PoolState | None = None,
    ) -> int:
        """
        Calculates the expected token OUTPUT for a target INPUT at current pool reserves.
        """

        if token_in not in self.tokens:  # pragma: no cover
            raise DegenbotValueError("token_in not recognized.")

        TOKEN_IN: Literal[0, 1] = 0 if token_in == self.token0 else 1

        if token_in_quantity <= 0:  # pragma: no cover
            raise ZeroSwapError("token_in_quantity must be positive")

        if override_state:  # pragma: no cover
            logger.debug(f"State overrides applied: {override_state}")

        reserves_0 = (
            override_state.reserves_token0 if override_state is not None else self.reserves_token0
        )
        reserves_1 = (
            override_state.reserves_token1 if override_state is not None else self.reserves_token1
        )

        if self.stable:
            return calc_exact_in_stable(
                amount_in=token_in_quantity,
                token_in=TOKEN_IN,
                reserves0=reserves_0,
                reserves1=reserves_1,
                decimals0=10**self.token0.decimals,
                decimals1=10**self.token1.decimals,
                fee=self.fee,
            )
        else:
            return general_calc_exact_in_volatile(
                amount_in=token_in_quantity,
                token_in=TOKEN_IN,
                reserves0=reserves_0,
                reserves1=reserves_1,
                fee=self.fee,
            )

    def get_factory(self, w3: Web3, block_identifier: BlockIdentifier | None = None) -> str:
        factory_address, *_ = raw_call(
            w3=w3,
            address=self.address,
            block_identifier=get_number_for_block_identifier(block_identifier),
            calldata=encode_function_calldata(
                function_prototype="factory()",
                function_arguments=None,
            ),
            return_types=["address"],
        )
        return cast(str, factory_address)

    def get_fee(self, w3: Web3, block_identifier: BlockIdentifier | None = None) -> int:
        result, *_ = raw_call(
            w3=w3,
            address=self.factory,
            calldata=encode_function_calldata(
                function_prototype="getFee(address,bool)",
                function_arguments=[self.address, self.stable],
            ),
            return_types=["uint256"],
            block_identifier=get_number_for_block_identifier(block_identifier),
        )
        return cast(int, result)

    def get_reserves(
        self, w3: Web3, block_identifier: BlockIdentifier | None = None
    ) -> tuple[int, int]:
        reserves_token0, reserves_token1, *_ = raw_call(
            w3=w3,
            address=self.address,
            block_identifier=get_number_for_block_identifier(block_identifier),
            calldata=encode_function_calldata(
                function_prototype="getReserves()",
                function_arguments=None,
            ),
            return_types=["uint256", "uint256"],
        )

        return cast(int, reserves_token0), cast(int, reserves_token1)

    def get_stable(self, w3: Web3, block_identifier: BlockIdentifier | None = None) -> bool:
        stable, *_ = raw_call(
            w3=w3,
            address=self.address,
            block_identifier=get_number_for_block_identifier(block_identifier),
            calldata=encode_function_calldata(
                function_prototype="stable()",
                function_arguments=None,
            ),
            return_types=["bool"],
        )
        return cast(bool, stable)

    def get_token0(self, w3: Web3, block_identifier: BlockIdentifier | None = None) -> str:
        result, *_ = raw_call(
            w3=w3,
            address=self.address,
            calldata=encode_function_calldata(
                function_prototype="token0()",
                function_arguments=None,
            ),
            return_types=["address"],
            block_identifier=get_number_for_block_identifier(block_identifier),
        )
        return cast(str, result)

    def get_token1(self, w3: Web3, block_identifier: BlockIdentifier | None = None) -> str:
        result, *_ = raw_call(
            w3=w3,
            address=self.address,
            calldata=encode_function_calldata(
                function_prototype="token1()",
                function_arguments=None,
            ),
            return_types=["address"],
            block_identifier=get_number_for_block_identifier(block_identifier),
        )
        return cast(str, result)


class AerodromeV3Pool(UniswapV3Pool):
    from .types import AerodromeV3PoolState as state_constructor

    PoolStateType: TypeAlias = state_constructor

    TICK_STRUCT_TYPES = [
        "uint128",
        "int128",
        "int128",
        "uint256",
        "uint256",
        "uint256",
        "int56",
        "uint160",
        "uint32",
        "bool",
    ]
    SLOT0_STRUCT_TYPES = [
        "uint160",
        "int24",
        "uint16",
        "uint16",
        "uint16",
        "bool",
    ]

    def _verified_address(self) -> ChecksumAddress:
        # The implementation address is hard-coded into the contract
        implementation_address = to_checksum_address(
            config.get_web3().eth.get_code(self.address)[10:30]
        )

        return generate_aerodrome_v3_pool_address(
            deployer_address=self.deployer_address,
            token_addresses=(self.token0.address, self.token1.address),
            implementation_address=to_checksum_address(implementation_address),
            tick_spacing=self.tick_spacing,
        )
