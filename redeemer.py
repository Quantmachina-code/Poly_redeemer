#!/usr/bin/env python3
"""
redeemer.py — Automatically redeem resolved Polymarket positions every 5 minutes.

Setup:
    cp env.example .env          # fill in POLY_PRIVATE_KEY, POLY_FUNDER_ADDRESS, etc.
    pip install -r requirements.txt
    python redeemer.py

How it works:
    1. Every 5 minutes, query the Polymarket Data API for the wallet's positions.
    2. For each position, determine the CTF conditionId and outcome indexSet.
    3. Check on-chain whether the condition has been resolved (payoutDenominator > 0).
    4. Check the wallet's ERC-1155 CTF token balance for that position.
    5. If both resolved and non-zero balance, call redeemPositions() on the CTF contract.

Requirements:
    • POLY_PRIVATE_KEY must be set in .env (0x-prefixed private key).
    • A small MATIC balance for gas is required (~$0.001 per redemption).
    • Works with standard binary YES/NO Polymarket markets.
"""

from __future__ import annotations

import os
import sys
import time
import logging
from typing import Optional

import requests
from web3 import Web3
from eth_account import Account
from dotenv import load_dotenv

load_dotenv()

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Configuration (from .env) ─────────────────────────────────────────────────
POLYGON_RPC_URL     = os.getenv("POLYGON_RPC_URL", "https://polygon-rpc.com")
POLY_PRIVATE_KEY    = os.getenv("POLY_PRIVATE_KEY", "").strip()
POLY_FUNDER_ADDRESS = os.getenv("POLY_FUNDER_ADDRESS", "").strip()

# Native USDC on Polygon (post-2024 Polymarket markets).
# For older markets use USDC.e: 0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174
_DEFAULT_USDC     = "0x3c499c542cEF5E3811e1192ce70d8cC03d5c3359"
POLY_USDC_ADDRESS = Web3.to_checksum_address(
    os.getenv("POLY_USDC_ADDRESS") or _DEFAULT_USDC
)

# ── Contract addresses (Polygon mainnet) ──────────────────────────────────────
# Gnosis Conditional Token Framework — holds all Polymarket outcome tokens
CTF_ADDRESS = Web3.to_checksum_address("0x4D97DCd97eC945f40cF65F87097ACe5EA0476045")

# ── API endpoints ─────────────────────────────────────────────────────────────
DATA_API  = "https://data-api.polymarket.com"   # positions for a wallet
GAMMA_API = "https://gamma-api.polymarket.com"  # market metadata

# ── Timing ────────────────────────────────────────────────────────────────────
REDEEM_INTERVAL_S = int(os.getenv("REDEEM_INTERVAL_S", str(5 * 60)))  # 5 min default
HTTP_TIMEOUT      = 20  # seconds per API call

# ── CTF ABI (only the functions we need) ──────────────────────────────────────
CTF_ABI = [
    # redeemPositions(collateralToken, parentCollectionId, conditionId, indexSets)
    {
        "inputs": [
            {"internalType": "address",   "name": "collateralToken",    "type": "address"},
            {"internalType": "bytes32",   "name": "parentCollectionId", "type": "bytes32"},
            {"internalType": "bytes32",   "name": "conditionId",        "type": "bytes32"},
            {"internalType": "uint256[]", "name": "indexSets",          "type": "uint256[]"},
        ],
        "name": "redeemPositions",
        "outputs": [],
        "stateMutability": "nonpayable",
        "type": "function",
    },
    # balanceOf(account, id) → uint256
    {
        "inputs": [
            {"internalType": "address", "name": "account", "type": "address"},
            {"internalType": "uint256", "name": "id",      "type": "uint256"},
        ],
        "name": "balanceOf",
        "outputs": [{"internalType": "uint256", "name": "", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function",
    },
    # payoutDenominator(conditionId) → uint256  (non-zero once resolved)
    {
        "inputs": [{"internalType": "bytes32", "name": "conditionId", "type": "bytes32"}],
        "name": "payoutDenominator",
        "outputs": [{"internalType": "uint256", "name": "", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function",
    },
]

# ── Utility helpers ───────────────────────────────────────────────────────────

def hex_to_bytes32(hex_str: str) -> bytes:
    """Convert a 0x-prefixed (or bare) hex string to exactly 32 bytes."""
    return bytes.fromhex(hex_str.removeprefix("0x").zfill(64))


def _get(url: str, params: dict | None = None) -> Optional[list | dict]:
    """HTTP GET with basic error handling. Returns parsed JSON or None."""
    try:
        resp = requests.get(url, params=params, timeout=HTTP_TIMEOUT)
        resp.raise_for_status()
        return resp.json()
    except Exception as exc:
        log.warning("GET %s failed: %s", url, exc)
        return None


# ── Polymarket API helpers ────────────────────────────────────────────────────

def fetch_user_positions(address: str) -> list[dict]:
    """
    Fetch all positions for *address* from the Polymarket Data API.

    Returns a list of position dicts. Each dict may contain:
        asset_id / tokenId / token_id  — ERC-1155 token ID (as string)
        conditionId / condition_id     — CTF condition ID (0x-prefixed hex)
        outcome                        — "Yes" / "No"
        size                           — position size in shares
    """
    data = _get(f"{DATA_API}/positions", {
        "user":           address,
        "sizeThreshold":  "0",
        "limit":          500,
    })
    if isinstance(data, list):
        return data
    # Some API versions wrap the list in a 'data' key
    if isinstance(data, dict):
        return data.get("data") or data.get("positions") or []
    return []


def fetch_markets_by_tokens(token_ids: list[str]) -> list[dict]:
    """
    Batch-fetch market metadata from Gamma API for the given CLOB token IDs.
    Returns a list of market dicts.
    """
    if not token_ids:
        return []
    # Gamma API accepts a comma-separated list for clob_token_ids
    data = _get(f"{GAMMA_API}/markets", {"clob_token_ids": ",".join(token_ids)})
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        return data.get("data") or []
    return []


def build_token_to_market_map(positions: list[dict]) -> dict[str, dict]:
    """
    Given a flat list of position dicts, batch-look up their markets
    on the Gamma API and return a mapping: token_id_str → market_dict.
    """
    # Collect all token IDs that lack a conditionId in the position record
    unknown_tokens = []
    for pos in positions:
        cond = pos.get("conditionId") or pos.get("condition_id")
        if not cond:
            tok = str(
                pos.get("asset_id")
                or pos.get("tokenId")
                or pos.get("token_id")
                or ""
            )
            if tok:
                unknown_tokens.append(tok)

    if not unknown_tokens:
        return {}

    markets = fetch_markets_by_tokens(list(set(unknown_tokens)))
    mapping: dict[str, dict] = {}
    for mkt in markets:
        tokens = mkt.get("tokens") or []
        for tok in tokens:
            tid = str(tok.get("token_id", "") if isinstance(tok, dict) else tok)
            if tid:
                mapping[tid] = mkt
    return mapping


def index_set_for_token(market: dict, token_id: str) -> Optional[int]:
    """
    Return the CTF indexSet (1 or 2) for *token_id* within *market*.

    In a binary Polymarket market:
        tokens[0]  →  indexSet 1  (YES, first outcome)
        tokens[1]  →  indexSet 2  (NO,  second outcome)
    """
    tokens = market.get("tokens") or []
    for i, tok in enumerate(tokens):
        tid = str(tok.get("token_id", tok) if isinstance(tok, dict) else tok)
        if tid == str(token_id):
            return 1 << i  # 1 for i=0, 2 for i=1
    return None


# ── Core redemption logic ─────────────────────────────────────────────────────

def build_condition_map(
    positions: list[dict],
    token_to_market: dict[str, dict],
) -> dict[str, dict[int, int]]:
    """
    Convert a flat list of position records into:
        { conditionId_hex: { token_id_int: index_set_int, ... }, ... }

    This groups positions by their CTF condition so we can redeem all
    outcomes of the same market in a single transaction.
    """
    by_condition: dict[str, dict[int, int]] = {}

    for pos in positions:
        # --- resolve token ID ---
        tok_raw = (
            pos.get("asset_id")
            or pos.get("tokenId")
            or pos.get("token_id")
        )
        if tok_raw is None:
            continue
        token_id = int(tok_raw)
        tok_str  = str(token_id)

        # --- resolve conditionId ---
        cond_id = pos.get("conditionId") or pos.get("condition_id")
        if not cond_id:
            mkt = token_to_market.get(tok_str)
            if not mkt:
                log.debug("No market found for token %s; skipping.", tok_str)
                continue
            cond_id = mkt.get("conditionId")
        if not cond_id:
            continue

        # --- resolve indexSet ---
        idx = pos.get("indexSet") or pos.get("index_set")
        if idx is None:
            outcome = str(pos.get("outcome") or "").strip().lower()
            if outcome in ("yes", "1"):
                idx = 1
            elif outcome in ("no", "2"):
                idx = 2
            else:
                # Derive from Gamma market token ordering
                mkt = token_to_market.get(tok_str)
                if not mkt:
                    mkt = next(
                        (m for m in token_to_market.values()
                         if m.get("conditionId") == cond_id),
                        None,
                    )
                idx = index_set_for_token(mkt, tok_str) if mkt else None

        if idx is None:
            log.debug("Cannot determine indexSet for token %s; skipping.", tok_str)
            continue

        by_condition.setdefault(cond_id, {})[token_id] = int(idx)

    return by_condition


def redeem_condition(
    w3:       Web3,
    ctf,
    account:  Account,
    cond_id:  str,
    tok_map:  dict[int, int],   # token_id → indexSet
    nonce:    int,
) -> tuple[Optional[str], int]:
    """
    Attempt to redeem all positions in *cond_id* that have a non-zero balance.
    Returns (tx_hash_hex_or_None, next_nonce).
    """
    wallet = account.address
    cond_b = hex_to_bytes32(cond_id)

    # Check on-chain resolution
    try:
        denom = ctf.functions.payoutDenominator(cond_b).call()
    except Exception as exc:
        log.warning("payoutDenominator(%s…): %s", cond_id[:16], exc)
        return None, nonce

    if denom == 0:
        log.debug("Condition %s… not resolved yet.", cond_id[:16])
        return None, nonce

    log.info("Condition %s… RESOLVED (denom=%s).", cond_id[:20], denom)

    # Find positions with non-zero CTF balance
    redeemable: list[int] = []
    for tok_id, idx_set in tok_map.items():
        try:
            bal = ctf.functions.balanceOf(wallet, tok_id).call()
        except Exception as exc:
            log.warning("balanceOf(token=%s): %s", tok_id, exc)
            continue
        if bal > 0:
            log.info("  token %-42s  indexSet=%s  balance=%s", tok_id, idx_set, bal)
            redeemable.append(idx_set)

    if not redeemable:
        log.info("  → No redeemable balance (already claimed or zero position).")
        return None, nonce

    # Build redeemPositions transaction
    parent = b"\x00" * 32
    fn     = ctf.functions.redeemPositions(
        POLY_USDC_ADDRESS, parent, cond_b, redeemable
    )
    try:
        gas = fn.estimate_gas({"from": wallet}) + 60_000
    except Exception:
        gas = 350_000

    tx = fn.build_transaction({
        "from":     wallet,
        "gas":      gas,
        "gasPrice": w3.eth.gas_price,
        "nonce":    nonce,
    })
    signed = account.sign_transaction(tx)

    # web3.py ≥6 uses .raw_transaction; fall back to .rawTransaction for older
    raw_tx = getattr(signed, "raw_transaction", None) or signed.rawTransaction

    try:
        tx_hash = w3.eth.send_raw_transaction(raw_tx)
        log.info("  ✓ tx sent: %s", tx_hash.hex())
        return tx_hash.hex(), nonce + 1
    except Exception as exc:
        log.error("  send_raw_transaction failed: %s", exc)
        return None, nonce


# ── One full redemption cycle ─────────────────────────────────────────────────

def run_once(w3: Web3, ctf, account: Account) -> None:
    wallet = account.address
    log.info("── Scanning wallet %s ──", wallet)

    # 1. Fetch positions
    positions = fetch_user_positions(wallet)
    log.info("Data API returned %d position record(s).", len(positions))
    if not positions:
        return

    # 2. Resolve any missing market metadata in a single batch request
    token_to_market = build_token_to_market_map(positions)

    # 3. Group by conditionId
    by_condition = build_condition_map(positions, token_to_market)
    if not by_condition:
        log.info("No positions with identifiable conditions.")
        return

    log.info("Checking %d condition(s) …", len(by_condition))

    # 4. Iterate; maintain a running nonce for any txs sent this cycle
    nonce    = w3.eth.get_transaction_count(wallet, "pending")
    redeemed = 0

    for cond_id, tok_map in by_condition.items():
        tx_hash, nonce = redeem_condition(w3, ctf, account, cond_id, tok_map, nonce)
        if tx_hash:
            redeemed += 1

    log.info("Cycle complete — %d redemption(s) submitted.", redeemed)


# ── Entry point ───────────────────────────────────────────────────────────────

def main() -> None:
    if not POLY_PRIVATE_KEY:
        log.error(
            "POLY_PRIVATE_KEY is not set.\n"
            "Copy env.example to .env and fill in your credentials."
        )
        sys.exit(1)

    account        = Account.from_key(POLY_PRIVATE_KEY)
    wallet_address = Web3.to_checksum_address(POLY_FUNDER_ADDRESS or account.address)

    log.info("═" * 55)
    log.info("  Poly Redeemer — started")
    log.info("  Wallet   : %s", wallet_address)
    log.info("  RPC      : %s", POLYGON_RPC_URL)
    log.info("  USDC     : %s", POLY_USDC_ADDRESS)
    log.info("  CTF      : %s", CTF_ADDRESS)
    log.info("  Interval : %ds (%dm)", REDEEM_INTERVAL_S, REDEEM_INTERVAL_S // 60)
    log.info("═" * 55)

    w3 = Web3(Web3.HTTPProvider(POLYGON_RPC_URL))
    if not w3.is_connected():
        log.error("Cannot connect to Polygon RPC: %s", POLYGON_RPC_URL)
        sys.exit(1)

    log.info("Connected to Polygon (chain_id=%s)", w3.eth.chain_id)

    ctf = w3.eth.contract(address=CTF_ADDRESS, abi=CTF_ABI)

    while True:
        try:
            run_once(w3, ctf, account)
        except KeyboardInterrupt:
            log.info("Interrupted — exiting.")
            sys.exit(0)
        except Exception as exc:
            log.exception("Unexpected error in redemption cycle: %s", exc)

        log.info("Sleeping %ds until next scan …\n", REDEEM_INTERVAL_S)
        try:
            time.sleep(REDEEM_INTERVAL_S)
        except KeyboardInterrupt:
            log.info("Interrupted — exiting.")
            sys.exit(0)


if __name__ == "__main__":
    main()
