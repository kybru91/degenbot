import contextlib
import pathlib
import shutil
import socket
import subprocess
from collections.abc import AsyncIterator, Iterable
from typing import TYPE_CHECKING, Any, Literal, cast

import tenacity
from eth_typing import HexAddress, HexStr
from hexbytes import HexBytes
from web3 import AsyncIPCProvider, AsyncWeb3, IPCProvider, Web3
from web3.middleware import Middleware
from web3.types import RPCEndpoint

from degenbot.constants import MAX_UINT256
from degenbot.exceptions import DegenbotError, DegenbotValueError, InvalidUint256
from degenbot.logging import logger
from degenbot.types import BlockNumber, ChainId


class AnvilNotFound(Exception):
    def __init__(self) -> None:  # pragma: no cover
        super().__init__("Anvil path could not be located.")


class AnvilFork:
    """
    Launch an Anvil fork as a separate process and expose methods for commonly-used RPC calls.

    Provides a `Web3` connector to Anvil's IPC socket endpoint at the `.w3` attribute.
    """

    def __init__(
        self,
        fork_url: str,
        fork_block: BlockNumber | None = None,
        fork_transaction_hash: str | None = None,
        hardfork: str = "latest",
        chain_id: ChainId | None = None,
        mining_mode: Literal["auto", "interval", "none"] = "auto",
        mining_interval: int | None = None,
        storage_caching: bool = True,
        base_fee: int | None = None,
        ipc_path: pathlib.Path = pathlib.Path("/tmp/"),
        mnemonic: str = (
            # Default mnemonic used by Brownie for Ganache forks
            "patient rude simple dog close planet oval animal hunt sketch suspect slim"
        ),
        coinbase: HexAddress | None = None,
        middlewares: list[tuple[Middleware, int]] | None = None,
        balance_overrides: Iterable[tuple[HexAddress, int]] | None = None,
        bytecode_overrides: Iterable[tuple[HexAddress, bytes]] | None = None,
        nonce_overrides: Iterable[tuple[HexAddress, int]] | None = None,
        ipc_provider_kwargs: dict[str, Any] | None = None,
        prune_history: bool = False,
        anvil_opts: list[str] | None = None,  # Additional options passed to the Anvil command
    ):
        def build_anvil_command(path_to_anvil: pathlib.Path) -> list[str]:  # pragma: no cover
            command = [
                str(path_to_anvil),
                "--silent",
                "--auto-impersonate",
                "--no-rate-limit",
                f"--fork-url={fork_url}",
                f"--hardfork={hardfork}",
                f"--port={self.port}",
                f"--ipc={self.ipc_filename}",
                f"--mnemonic={mnemonic}",
            ]
            if fork_block:
                command.append(f"--fork-block-number={fork_block}")
            if fork_transaction_hash:
                command.append(f"--fork-transaction-hash={fork_transaction_hash}")
            if chain_id:
                command.append(f"--chain-id={chain_id}")
            if base_fee:
                command.append(f"--base-fee={base_fee}")
            if storage_caching is False:
                command.append("--no-storage-caching")
            if prune_history:
                command.append("--prune-history")
            if anvil_opts:
                command.extend(anvil_opts)
            match mining_mode:
                case "auto":
                    pass
                case "interval":
                    if mining_interval is None:
                        raise DegenbotValueError(
                            message="Interval mining mode was specified without an interval value."
                        )
                    command.append(f"--block-time={mining_interval}")
                case "none":
                    command.append("--no-mining")
                    command.append("--order=fifo")
                case _:
                    raise DegenbotValueError(message=f"Unknown mining mode '{mining_mode}'.")

            return command

        _path_to_anvil = shutil.which("anvil")
        if _path_to_anvil is None:  # pragma: no cover
            raise AnvilNotFound
        path_to_anvil = pathlib.Path(_path_to_anvil)

        self.port = self._get_free_port_number()
        self.ipc_path = ipc_path
        if ipc_provider_kwargs is not None:
            self.ipc_provider_kwargs = ipc_provider_kwargs
        else:
            self.ipc_provider_kwargs = {}

        self._anvil_command = build_anvil_command(path_to_anvil=path_to_anvil)
        self._setup_process_and_w3(anvil_command=self._anvil_command)

        self._block_number = (
            fork_block if fork_block is not None else self.w3.eth.get_block_number()
        )
        self._fork_url = fork_url

        if middlewares is not None:
            for middleware, layer in middlewares:
                self.w3.middleware_onion.inject(middleware, layer=layer)

        if balance_overrides is not None:
            for account, balance in balance_overrides:
                self.set_balance(account, balance)

        if bytecode_overrides is not None:
            for account, bytecode in bytecode_overrides:
                self.set_code(account, bytecode)

        if nonce_overrides is not None:
            for account, nonce in nonce_overrides:
                self.set_nonce(account, nonce)

        if coinbase is not None:
            self.set_coinbase(coinbase)

        if mining_interval:
            self.set_block_timestamp_interval(mining_interval)

    @property
    def block_number(self) -> int:
        return self._block_number

    @property
    def fork_url(self) -> str:
        return self._fork_url

    @property
    def http_url(self) -> str:
        return f"http://localhost:{self.port}"

    @property
    def ipc_filename(self) -> pathlib.Path:
        return self.ipc_path / f"anvil-{self.port}.ipc"

    @property
    def ws_url(self) -> str:
        return f"ws://localhost:{self.port}"

    @staticmethod
    def _get_free_port_number() -> int:
        with socket.socket() as sock:
            sock.bind(("", 0))
            _, _port = sock.getsockname()
            return cast("int", _port)

    def _setup_process_and_w3(self, anvil_command: list[str]) -> None:
        """
        Launch an Anvil subprocess and wait for a `Web3` connection to be established with its
        IPC socket.
        """

        process = subprocess.Popen(anvil_command)

        try:
            # Storage I/O should be fast, so use a low fixed wait time
            filename_check_with_retry = tenacity.Retrying(
                stop=tenacity.stop_after_delay(10),
                wait=tenacity.wait_fixed(0.01),
                retry=tenacity.retry_if_result(lambda result: result is False),
            )
            filename_check_with_retry(fn=self.ipc_filename.exists)
        except tenacity.RetryError as exc:
            raise DegenbotError(message="Timed out waiting for IPC socket to be created.") from exc

        try:
            # network I/O is less reliable, so wait with an exponential delay and jitter
            w3 = Web3(IPCProvider(ipc_path=self.ipc_filename, **self.ipc_provider_kwargs))
            w3_connected_check_with_retry = tenacity.Retrying(
                stop=tenacity.stop_after_delay(10),
                wait=tenacity.wait_exponential_jitter(),
                retry=tenacity.retry_if_result(lambda result: result is False),
            )
            w3_connected_check_with_retry(fn=w3.is_connected)
        except tenacity.RetryError as exc:
            raise DegenbotError(message="Timed out waiting for Web3 connection.") from exc

        self.w3 = w3
        self._process = process

    def __del__(self) -> None:
        if hasattr(self, "_process"):
            self._process.terminate()
            self._process.wait(timeout=10)
        self.ipc_filename.unlink(missing_ok=True)

    def mine(self) -> None:
        self.w3.provider.make_request(
            method=RPCEndpoint("evm_mine"),
            params=[],
        )

    @property
    @contextlib.asynccontextmanager
    async def async_w3(self) -> AsyncIterator[AsyncWeb3]:
        # TODO: investigate why cache_allowed_requests causes sequential is_connected calls to fail
        async with AsyncWeb3(AsyncIPCProvider(self.ipc_filename)) as async_w3:
            if TYPE_CHECKING:
                assert isinstance(async_w3, AsyncWeb3)
            yield async_w3

    def reset(
        self,
        fork_url: str | None = None,
        block_number: BlockNumber | None = None,
        transaction_hash: str | None = None,
    ) -> None:
        """
        Fork from a new block number or transaction hash.
        """

        # Terminate the old fork process
        self._process.terminate()
        self._process.wait(timeout=10)

        if block_number is not None and transaction_hash is not None:
            block_number = None
            logger.warning(
                f"Forking from transaction hash {transaction_hash}, ignoring provided block number."
            )

        # Sanitize the command by stripping options that may conflict
        self._anvil_command = [
            option
            for option in self._anvil_command.copy()
            if all(
                (
                    "--fork-url" not in option,
                    "--fork-block-number" not in option,
                    "--fork-transaction-hash" not in option,
                )
            )
        ]

        if fork_url is not None:
            self._fork_url = fork_url
        self._anvil_command.append(f"--fork-url={self._fork_url}")

        if block_number is not None:
            self._anvil_command.append(f"--fork-block-number={block_number}")

        if transaction_hash is not None:
            self._anvil_command.append(f"--fork-transaction-hash={transaction_hash}")

        self._setup_process_and_w3(anvil_command=self._anvil_command)

    def return_to_snapshot(self, snapshot_id: int) -> bool:
        if snapshot_id < 0:
            raise DegenbotValueError(message="ID cannot be negative")
        return bool(
            self.w3.provider.make_request(
                method=RPCEndpoint("evm_revert"),
                params=[snapshot_id],
            )["result"]
        )

    def set_balance(self, address: str, balance: int) -> None:
        if not (0 <= balance <= MAX_UINT256):
            raise InvalidUint256

        self.w3.provider.make_request(
            method=RPCEndpoint("anvil_setBalance"),
            params=[address, hex(balance)],
        )

    def set_code(self, address: str, bytecode: bytes) -> None:
        self.w3.provider.make_request(
            method=RPCEndpoint("anvil_setCode"),
            params=[address, bytecode],
        )

    def set_coinbase(self, address: str) -> None:
        self.w3.provider.make_request(
            method=RPCEndpoint("anvil_setCoinbase"),
            params=[address],
        )

    def set_block_timestamp_interval(self, interval: int) -> None:
        self.w3.provider.make_request(
            method=RPCEndpoint("anvil_setBlockTimestampInterval"),
            params=[interval],
        )

    def set_next_base_fee(self, fee: int) -> None:
        if not (0 <= fee <= MAX_UINT256):
            raise InvalidUint256
        self.w3.provider.make_request(
            method=RPCEndpoint("anvil_setNextBlockBaseFeePerGas"),
            params=[fee],
        )

    def set_nonce(self, address: str, nonce: int) -> None:
        self.w3.provider.make_request(
            method=RPCEndpoint("anvil_setNonce"),
            params=[address, nonce],
        )

    def set_snapshot(self) -> int:
        return int(
            self.w3.provider.make_request(
                method=RPCEndpoint("evm_snapshot"),
                params=[],
            )["result"],
            16,
        )

    def set_storage(self, address: HexStr | bytes, position: int, value: HexStr | bytes) -> None:
        self.w3.provider.make_request(
            method=RPCEndpoint("anvil_setStorageAt"),
            params=[
                address,
                position,
                (
                    # Storage value must be padded to 32 bytes
                    HexBytes(value).hex().zfill(64)
                ),
            ],
        )
