"""
One-time on-chain allowance setup for EOA live trading on Polymarket (Polygon).

An EOA (signature_type 0) must approve the Polymarket exchange contracts to move
its USDC (ERC-20) and outcome tokens (ERC-1155 CTF) before the CLOB will accept
its orders. This module checks the current allowances and only sends the missing
approvals. Proxy wallets (signature_type 1/2) have allowances managed by
Polymarket and are skipped.

Requires the trading wallet to hold a little POL/MATIC for gas.
"""

from typing import Dict, Any, List
from web3 import AsyncWeb3
from .config import settings
from .chainlink import chainlink_fetcher  # reuse its ordered Polygon RPC list

MAX_UINT256 = (2 ** 256) - 1
ALLOWANCE_THRESHOLD = 2 ** 255  # an existing allowance above this counts as "set"

ERC20_ABI = [
    {"constant": False, "inputs": [{"name": "spender", "type": "address"}, {"name": "value", "type": "uint256"}], "name": "approve", "outputs": [{"name": "", "type": "bool"}], "stateMutability": "nonpayable", "type": "function"},
    {"constant": True, "inputs": [{"name": "owner", "type": "address"}, {"name": "spender", "type": "address"}], "name": "allowance", "outputs": [{"name": "", "type": "uint256"}], "stateMutability": "view", "type": "function"},
]

ERC1155_ABI = [
    {"inputs": [{"name": "operator", "type": "address"}, {"name": "approved", "type": "bool"}], "name": "setApprovalForAll", "outputs": [], "stateMutability": "nonpayable", "type": "function"},
    {"inputs": [{"name": "account", "type": "address"}, {"name": "operator", "type": "address"}], "name": "isApprovedForAll", "outputs": [{"name": "", "type": "bool"}], "stateMutability": "view", "type": "function"},
]


async def _get_w3() -> AsyncWeb3:
    last_err = None
    for rpc in chainlink_fetcher.get_ordered_rpcs():
        try:
            w3 = AsyncWeb3(AsyncWeb3.AsyncHTTPProvider(rpc, request_kwargs={"timeout": 10.0}))
            if await w3.is_connected():
                return w3
        except Exception as e:
            last_err = e
            continue
    raise RuntimeError(f"no_rpc_available: {last_err}")


def _raw_tx(signed) -> bytes:
    # web3 v6.5+ uses raw_transaction; older uses rawTransaction
    return getattr(signed, "raw_transaction", None) or getattr(signed, "rawTransaction")


async def ensure_allowances() -> Dict[str, Any]:
    if settings.CLOB_SIGNATURE_TYPE != 0:
        return {"ok": True, "skipped": True, "reason": "proxy_wallet_allowances_managed_by_polymarket"}
    if not settings.PRIVATE_KEY:
        return {"ok": False, "error": "missing_private_key"}

    w3 = await _get_w3()
    acct = w3.eth.account.from_key(settings.PRIVATE_KEY)
    owner = acct.address

    usdc = w3.eth.contract(address=AsyncWeb3.to_checksum_address(settings.USDC_ADDRESS), abi=ERC20_ABI)
    ctf = w3.eth.contract(address=AsyncWeb3.to_checksum_address(settings.CTF_ADDRESS), abi=ERC1155_ABI)

    spenders = [
        ("exchange", settings.CLOB_EXCHANGE_ADDRESS),
        ("neg_risk_exchange", settings.CLOB_NEG_RISK_EXCHANGE_ADDRESS),
        ("neg_risk_adapter", settings.CLOB_NEG_RISK_ADAPTER_ADDRESS),
    ]

    nonce = await w3.eth.get_transaction_count(owner)
    try:
        gas_price = int(await w3.eth.gas_price)
    except Exception:
        gas_price = w3.to_wei(50, "gwei")
    gas_price = int(gas_price * 1.25)

    actions: List[Dict[str, Any]] = []

    async def _send(func) -> Dict[str, Any]:
        nonlocal nonce
        tx = await func.build_transaction({
            "from": owner,
            "nonce": nonce,
            "chainId": 137,
            "gas": 120000,
            "gasPrice": gas_price,
        })
        signed = acct.sign_transaction(tx)
        tx_hash = await w3.eth.send_raw_transaction(_raw_tx(signed))
        nonce += 1
        receipt = await w3.eth.wait_for_transaction_receipt(tx_hash, timeout=180)
        return {"tx": tx_hash.hex(), "status": int(receipt.get("status", 0))}

    for name, raw_addr in spenders:
        if not raw_addr:
            continue
        spender = AsyncWeb3.to_checksum_address(raw_addr)

        current = await usdc.functions.allowance(owner, spender).call()
        if current < ALLOWANCE_THRESHOLD:
            res = await _send(usdc.functions.approve(spender, MAX_UINT256))
            actions.append({"type": "usdc_approve", "spender": name, **res})

        approved = await ctf.functions.isApprovedForAll(owner, spender).call()
        if not approved:
            res = await _send(ctf.functions.setApprovalForAll(spender, True))
            actions.append({"type": "ctf_approve", "spender": name, **res})

    return {"ok": True, "owner": owner, "actions": actions, "already_set": len(actions) == 0}
