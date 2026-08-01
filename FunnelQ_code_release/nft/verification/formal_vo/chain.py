from __future__ import annotations

import json
import re
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any, Iterable, Mapping

from .protocol import hash_obj

_MASK64 = (1 << 64) - 1
_ROUND_CONSTANTS = (
    0x0000000000000001,
    0x0000000000008082,
    0x800000000000808A,
    0x8000000080008000,
    0x000000000000808B,
    0x0000000080000001,
    0x8000000080008081,
    0x8000000000008009,
    0x000000000000008A,
    0x0000000000000088,
    0x0000000080008009,
    0x000000008000000A,
    0x000000008000808B,
    0x800000000000008B,
    0x8000000000008089,
    0x8000000000008003,
    0x8000000000008002,
    0x8000000000000080,
    0x000000000000800A,
    0x800000008000000A,
    0x8000000080008081,
    0x8000000000008080,
    0x0000000080000001,
    0x8000000080008008,
)
_ROTATION = (
    (0, 36, 3, 41, 18),
    (1, 44, 10, 45, 2),
    (62, 6, 43, 15, 61),
    (28, 55, 25, 21, 56),
    (27, 20, 39, 8, 14),
)
_SIGNATURE_RE = re.compile(r"^([A-Za-z_][A-Za-z0-9_]*)\((.*)\)$")
_HEX_RE = re.compile(r"^[0-9a-fA-F]+$")
_ZERO_ROOT = "0" * 64


class JsonRpcError(RuntimeError):
    pass


def _rotl64(value: int, amount: int) -> int:
    if amount == 0:
        return value & _MASK64
    return ((value << amount) | (value >> (64 - amount))) & _MASK64


def _keccak_f1600(state: list[int]) -> None:
    for round_constant in _ROUND_CONSTANTS:
        c = [
            state[x]
            ^ state[x + 5]
            ^ state[x + 10]
            ^ state[x + 15]
            ^ state[x + 20]
            for x in range(5)
        ]
        d = [c[(x - 1) % 5] ^ _rotl64(c[(x + 1) % 5], 1) for x in range(5)]
        for x in range(5):
            for y in range(5):
                state[x + 5 * y] ^= d[x]

        b = [0] * 25
        for x in range(5):
            for y in range(5):
                b[y + 5 * ((2 * x + 3 * y) % 5)] = _rotl64(
                    state[x + 5 * y], _ROTATION[x][y]
                )

        for x in range(5):
            for y in range(5):
                state[x + 5 * y] = (
                    b[x + 5 * y]
                    ^ ((~b[((x + 1) % 5) + 5 * y]) & b[((x + 2) % 5) + 5 * y])
                ) & _MASK64
        state[0] ^= round_constant


def keccak_256(payload: bytes) -> bytes:
    """Ethereum Keccak-256 (legacy Keccak padding, not NIST SHA3-256)."""
    rate = 136
    state = [0] * 25
    offset = 0
    while offset + rate <= len(payload):
        block = payload[offset : offset + rate]
        for lane in range(rate // 8):
            start = lane * 8
            state[lane] ^= int.from_bytes(block[start : start + 8], "little")
        _keccak_f1600(state)
        offset += rate

    tail = bytearray(rate)
    remainder = payload[offset:]
    tail[: len(remainder)] = remainder
    tail[len(remainder)] ^= 0x01
    tail[-1] ^= 0x80
    for lane in range(rate // 8):
        start = lane * 8
        state[lane] ^= int.from_bytes(tail[start : start + 8], "little")
    _keccak_f1600(state)

    output = bytearray()
    while len(output) < 32:
        for lane in range(rate // 8):
            output.extend(state[lane].to_bytes(8, "little"))
            if len(output) >= 32:
                break
        if len(output) < 32:
            _keccak_f1600(state)
    return bytes(output[:32])


def function_selector(signature: str) -> bytes:
    _parse_signature(signature)
    return keccak_256(signature.encode("ascii"))[:4]


def _parse_signature(signature: str) -> tuple[str, tuple[str, ...]]:
    text = str(signature).strip()
    match = _SIGNATURE_RE.fullmatch(text)
    if not match:
        raise ValueError(f"invalid Solidity function signature: {signature!r}")
    raw = match.group(2).strip()
    types = () if not raw else tuple(item.strip() for item in raw.split(","))
    supported = {"bytes32", "uint256", "address"}
    unknown = [item for item in types if item not in supported]
    if unknown:
        raise ValueError(f"unsupported ABI argument types: {unknown}")
    return match.group(1), types


def normalize_bytes32(value: str, *, name: str = "bytes32") -> str:
    text = str(value).strip().lower()
    if text.startswith("0x"):
        text = text[2:]
    if len(text) != 64 or not _HEX_RE.fullmatch(text):
        raise ValueError(f"{name} must be exactly 32 bytes of hexadecimal data")
    return text


def normalize_address(value: str) -> str:
    text = str(value).strip().lower()
    if text.startswith("0x"):
        text = text[2:]
    if len(text) != 40 or not _HEX_RE.fullmatch(text):
        raise ValueError("contract/from address must be exactly 20 bytes")
    return "0x" + text


def _encode_word(type_name: str, value: str | int) -> bytes:
    if type_name == "bytes32":
        return bytes.fromhex(normalize_bytes32(str(value)))
    if type_name == "address":
        return bytes.fromhex(normalize_address(str(value))[2:]).rjust(32, b"\x00")
    if type_name == "uint256":
        if isinstance(value, str):
            number = int(value, 0)
        else:
            number = int(value)
        if number < 0 or number >= 1 << 256:
            raise ValueError("uint256 value is out of range")
        return number.to_bytes(32, "big")
    raise ValueError(f"unsupported ABI argument type: {type_name}")


def encode_call(signature: str, arguments: Iterable[str | int]) -> str:
    _name, types = _parse_signature(signature)
    values = tuple(arguments)
    if len(types) != len(values):
        raise ValueError(
            f"signature {signature!r} expects {len(types)} arguments, got {len(values)}"
        )
    encoded = function_selector(signature) + b"".join(
        _encode_word(type_name, value) for type_name, value in zip(types, values)
    )
    return "0x" + encoded.hex()


def derive_epoch_id(manifest: Mapping[str, Any]) -> str:
    contract = manifest["crossStructureContract"]
    return hash_obj(
        "epoch-registry-key-v1",
        {
            "protocolVersion": manifest["protocolVersion"],
            "schemaVersion": manifest["schemaVersion"],
            "dataRange": manifest["dataRange"],
            "sourceMainDbSha256": contract["sourceMainDbSha256"],
            "sourceTxDbSha256": contract["sourceTxDbSha256"],
        },
    )


def resolve_epoch_id(value: str | None, manifest: Mapping[str, Any] | None = None) -> str:
    if value is None or str(value).strip().lower() in {"", "auto"}:
        if manifest is None:
            raise ValueError("epoch id 'auto' requires an Epoch manifest")
        return derive_epoch_id(manifest)
    return normalize_bytes32(str(value), name="epoch id")


def _redact_rpc_url(url: str) -> str:
    parsed = urllib.parse.urlsplit(url)
    host = parsed.hostname or ""
    if parsed.port is not None:
        host = f"{host}:{parsed.port}"
    return urllib.parse.urlunsplit((parsed.scheme, host, parsed.path, "", ""))


@dataclass
class JsonRpcClient:
    url: str
    timeout_seconds: float = 15.0

    def __post_init__(self) -> None:
        parsed = urllib.parse.urlsplit(self.url)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            raise ValueError("RPC URL must be an absolute http(s) URL")
        if self.timeout_seconds <= 0:
            raise ValueError("RPC timeout must be positive")
        self._request_id = 0

    @property
    def display_url(self) -> str:
        return _redact_rpc_url(self.url)

    def call(self, method: str, params: list[Any]) -> Any:
        self._request_id += 1
        payload = json.dumps(
            {
                "jsonrpc": "2.0",
                "id": self._request_id,
                "method": method,
                "params": params,
            },
            separators=(",", ":"),
        ).encode("utf-8")
        request = urllib.request.Request(
            self.url,
            data=payload,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=self.timeout_seconds) as response:
                body = response.read()
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            raise JsonRpcError(f"JSON-RPC request failed for {method}: {exc}") from exc
        try:
            message = json.loads(body)
        except json.JSONDecodeError as exc:
            raise JsonRpcError(f"JSON-RPC returned invalid JSON for {method}") from exc
        if not isinstance(message, Mapping) or message.get("jsonrpc") != "2.0":
            raise JsonRpcError(f"JSON-RPC returned an invalid envelope for {method}")
        if message.get("id") != self._request_id:
            raise JsonRpcError(f"JSON-RPC response id mismatch for {method}")
        if "error" in message:
            error = message["error"]
            raise JsonRpcError(f"JSON-RPC {method} error: {error}")
        if "result" not in message:
            raise JsonRpcError(f"JSON-RPC response has no result for {method}")
        return message["result"]


def _hex_quantity(value: str | int, *, name: str) -> int:
    if isinstance(value, int):
        number = value
    else:
        text = str(value)
        if not text.startswith("0x"):
            raise JsonRpcError(f"{name} is not an Ethereum hex quantity")
        number = int(text, 16)
    if number < 0:
        raise JsonRpcError(f"{name} is negative")
    return number


def _normalize_block_tag(value: str | int) -> str:
    if isinstance(value, int):
        if value < 0:
            raise ValueError("block number cannot be negative")
        return hex(value)
    text = str(value).strip().lower()
    if text in {"latest", "safe", "finalized", "pending", "earliest"}:
        return text
    if text.startswith("0x") and _HEX_RE.fullmatch(text[2:] or "0"):
        return hex(int(text, 16))
    if text.isdigit():
        return hex(int(text))
    raise ValueError(f"unsupported block tag: {value!r}")


def _read_arguments(signature: str, epoch_id: str) -> list[str | int]:
    _name, types = _parse_signature(signature)
    if types == ():
        return []
    if types == ("bytes32",):
        return [epoch_id]
    if types == ("uint256",):
        return [int(epoch_id, 16)]
    raise ValueError(
        "read signature must have no arguments or exactly one bytes32/uint256 epoch key"
    )


def _write_arguments(signature: str, epoch_id: str, epoch_root: str) -> list[str | int]:
    _name, types = _parse_signature(signature)
    if types == ("bytes32", "bytes32"):
        return [epoch_id, epoch_root]
    if types == ("uint256", "bytes32"):
        return [int(epoch_id, 16), epoch_root]
    raise ValueError(
        "write signature must be (bytes32,bytes32) or (uint256,bytes32)"
    )


def read_epoch_root(
    *,
    rpc_url: str,
    contract_address: str,
    epoch_id: str,
    read_signature: str = "getEpochRoot(bytes32)",
    block_tag: str | int = "latest",
    expected_chain_id: int | None = None,
    timeout_seconds: float = 15.0,
) -> dict[str, Any]:
    started = time.perf_counter()
    client = JsonRpcClient(rpc_url, timeout_seconds=timeout_seconds)
    contract = normalize_address(contract_address)
    epoch_key = normalize_bytes32(epoch_id, name="epoch id")

    chain_context_started = time.perf_counter()
    chain_id = _hex_quantity(client.call("eth_chainId", []), name="chain id")
    if expected_chain_id is not None and chain_id != int(expected_chain_id):
        raise ValueError(
            f"RPC chain id {chain_id} differs from expected chain id {expected_chain_id}"
        )
    requested_tag = _normalize_block_tag(block_tag)
    if requested_tag in {"latest", "safe", "finalized"}:
        pinned_tag = hex(_hex_quantity(client.call("eth_blockNumber", []), name="block number"))
    else:
        pinned_tag = requested_tag
    block = client.call("eth_getBlockByNumber", [pinned_tag, False])
    if not isinstance(block, Mapping) or block.get("number") is None or block.get("hash") is None:
        raise JsonRpcError("RPC did not return the pinned block")
    block_number = _hex_quantity(block["number"], name="block number")
    block_hash = normalize_bytes32(str(block["hash"]), name="block hash")
    code = str(client.call("eth_getCode", [contract, pinned_tag])).lower()
    if code in {"0x", "0x0", "0x00", ""}:
        raise ValueError(f"no contract bytecode exists at {contract} in block {block_number}")
    code_hex = code[2:] if code.startswith("0x") else code
    if len(code_hex) % 2 or (code_hex and not _HEX_RE.fullmatch(code_hex)):
        raise JsonRpcError("eth_getCode returned invalid hexadecimal data")
    chain_context_ms = (time.perf_counter() - chain_context_started) * 1000.0

    call_data = encode_call(read_signature, _read_arguments(read_signature, epoch_key))
    eth_call_started = time.perf_counter()
    raw_result = client.call(
        "eth_call", [{"to": contract, "data": call_data}, pinned_tag]
    )
    eth_call_ms = (time.perf_counter() - eth_call_started) * 1000.0
    raw = str(raw_result).lower()
    if raw.startswith("0x"):
        raw = raw[2:]
    if len(raw) < 64 or len(raw) % 64 != 0 or not _HEX_RE.fullmatch(raw):
        raise JsonRpcError("eth_call did not return ABI-encoded bytes32 data")
    root = normalize_bytes32(raw[:64], name="on-chain EpochRoot")
    total_ms = (time.perf_counter() - started) * 1000.0
    return {
        "mode": "ethereum_json_rpc_eth_call",
        "rpcUrl": client.display_url,
        "chainId": chain_id,
        "contractAddress": contract,
        "epochId": epoch_key,
        "readSignature": read_signature,
        "requestedBlockTag": requested_tag,
        "blockNumber": block_number,
        "blockHash": block_hash,
        "contractCodeBytes": len(code_hex) // 2,
        "epochRoot": root,
        "chainContextMs": chain_context_ms,
        "ethCallMs": eth_call_ms,
        "chainReadMs": total_ms,
    }


def commit_epoch_root(
    *,
    rpc_url: str,
    contract_address: str,
    epoch_id: str,
    epoch_root: str,
    read_signature: str = "getEpochRoot(bytes32)",
    write_signature: str = "commitEpochRoot(bytes32,bytes32)",
    from_address: str | None = None,
    expected_chain_id: int | None = None,
    gas: int | None = None,
    timeout_seconds: float = 60.0,
    poll_interval_seconds: float = 0.25,
) -> dict[str, Any]:
    if timeout_seconds <= 0 or poll_interval_seconds <= 0:
        raise ValueError("transaction timeout and poll interval must be positive")
    contract = normalize_address(contract_address)
    epoch_key = normalize_bytes32(epoch_id, name="epoch id")
    root = normalize_bytes32(epoch_root, name="EpochRoot")

    before = read_epoch_root(
        rpc_url=rpc_url,
        contract_address=contract,
        epoch_id=epoch_key,
        read_signature=read_signature,
        expected_chain_id=expected_chain_id,
        timeout_seconds=min(timeout_seconds, 15.0),
    )
    if before["epochRoot"] == root:
        return {
            "status": "ALREADY_COMMITTED",
            "epochRoot": root,
            "epochId": epoch_key,
            "transactionHash": None,
            "receipt": None,
            "readback": before,
        }
    if before["epochRoot"] != _ZERO_ROOT:
        raise ValueError(
            "registry already contains a different nonzero root for this epoch id"
        )

    client = JsonRpcClient(rpc_url, timeout_seconds=min(timeout_seconds, 15.0))
    if from_address is None or not str(from_address).strip():
        accounts = client.call("eth_accounts", [])
        if not isinstance(accounts, list) or not accounts:
            raise ValueError("RPC node exposes no unlocked account; pass --from-address")
        sender = normalize_address(str(accounts[0]))
    else:
        sender = normalize_address(from_address)

    transaction: dict[str, Any] = {
        "from": sender,
        "to": contract,
        "data": encode_call(
            write_signature, _write_arguments(write_signature, epoch_key, root)
        ),
    }
    if gas is not None:
        if int(gas) <= 0:
            raise ValueError("gas must be positive")
        transaction["gas"] = hex(int(gas))
    else:
        estimated = client.call("eth_estimateGas", [transaction])
        estimated_int = _hex_quantity(estimated, name="gas estimate")
        transaction["gas"] = hex(max(estimated_int + estimated_int // 5, estimated_int + 20_000))

    tx_started = time.perf_counter()
    tx_hash_raw = str(client.call("eth_sendTransaction", [transaction])).lower()
    tx_hash = normalize_bytes32(tx_hash_raw, name="transaction hash")
    deadline = time.monotonic() + timeout_seconds
    receipt: Mapping[str, Any] | None = None
    while time.monotonic() < deadline:
        candidate = client.call("eth_getTransactionReceipt", ["0x" + tx_hash])
        if candidate is not None:
            if not isinstance(candidate, Mapping):
                raise JsonRpcError("transaction receipt has invalid shape")
            receipt = candidate
            break
        time.sleep(poll_interval_seconds)
    if receipt is None:
        raise TimeoutError("timed out waiting for the EpochRoot commit transaction")
    status = _hex_quantity(receipt.get("status", "0x0"), name="receipt status")
    if status != 1:
        raise RuntimeError(f"EpochRoot commit transaction reverted: 0x{tx_hash}")
    receipt_block = _hex_quantity(receipt["blockNumber"], name="receipt block number")
    gas_used = _hex_quantity(receipt.get("gasUsed", "0x0"), name="gas used")
    transaction_ms = (time.perf_counter() - tx_started) * 1000.0

    readback = read_epoch_root(
        rpc_url=rpc_url,
        contract_address=contract,
        epoch_id=epoch_key,
        read_signature=read_signature,
        block_tag=receipt_block,
        expected_chain_id=expected_chain_id,
        timeout_seconds=min(timeout_seconds, 15.0),
    )
    if readback["epochRoot"] != root:
        raise RuntimeError("on-chain readback differs from the committed EpochRoot")
    return {
        "status": "COMMITTED",
        "epochRoot": root,
        "epochId": epoch_key,
        "fromAddress": sender,
        "writeSignature": write_signature,
        "transactionHash": "0x" + tx_hash,
        "transactionMs": transaction_ms,
        "gasUsed": gas_used,
        "receipt": {
            "blockNumber": receipt_block,
            "blockHash": str(receipt.get("blockHash", "")),
            "status": status,
        },
        "readback": readback,
    }
