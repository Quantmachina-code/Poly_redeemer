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

import argparse
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
        if data:
            log.debug("Position record sample keys: %s", list(data[0].keys()))
        return data
    # Some API versions wrap the list in a 'data' key
    if isinstance(data, dict):
        inner = data.get("data") or data.get("positions") or []
        if inner:
            log.debug("Position record sample keys: %s", list(inner[0].keys()))
        return inner
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

    log.debug("Gamma lookup: %d unique token IDs", len(set(unknown_tokens)))
    markets = fetch_markets_by_tokens(list(set(unknown_tokens)))
    log.debug("Gamma returned %d market(s)", len(markets))
    if markets:
        log.debug("Market sample keys: %s", list(markets[0].keys()))
    mapping: dict[str, dict] = {}
    for mkt in markets:
        tokens = mkt.get("tokens") or mkt.get("clobTokenIds") or []
        for tok in tokens:
            tid = str(tok.get("token_id", "") if isinstance(tok, dict) else tok)
            if tid:
                mapping[tid] = mkt
    log.debug("token→market map: %d entries", len(mapping))
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
            cond_id = mkt.get("conditionId") or mkt.get("condition_id")
        if not cond_id:
            log.debug("No conditionId for token %s; skipping.", tok_str)
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
    w3:              Web3,
    ctf,
    account:         Account,
    cond_id:         str,
    tok_map:         dict[int, int],   # token_id → indexSet
    nonce:           int,
    token_to_market: dict[str, dict] | None = None,
    wallet:          str | None = None,
) -> tuple[Optional[str], int]:
    """
    Attempt to redeem all positions in *cond_id* that have a non-zero balance.
    Returns (tx_hash_hex_or_None, next_nonce).
    """
    wallet = wallet or account.address
    cond_b = hex_to_bytes32(cond_id)

    # Resolve a human-readable market label for this condition
    mkt_label = cond_id[:20] + "…"
    if token_to_market:
        for tok_id in tok_map:
            mkt = token_to_market.get(str(tok_id))
            if mkt:
                name = mkt.get("question") or mkt.get("title") or mkt.get("slug")
                if name:
                    mkt_label = f'"{name}"'
                break

    log.info("  Condition %s  %s", cond_id[:20] + "…", mkt_label if mkt_label != cond_id[:20] + "…" else "")

    # Check on-chain resolution
    try:
        denom = ctf.functions.payoutDenominator(cond_b).call()
    except Exception as exc:
        log.warning("    payoutDenominator failed: %s", exc)
        return None, nonce

    if denom == 0:
        log.info("    status : PENDING  (not resolved on-chain yet)")
        return None, nonce

    log.info("    status : RESOLVED  (payoutDenominator=%s)", denom)

    # Find positions with non-zero CTF balance
    redeemable: list[int] = []
    for tok_id, idx_set in tok_map.items():
        try:
            bal = ctf.functions.balanceOf(wallet, tok_id).call()
        except Exception as exc:
            log.warning("    balanceOf(token=%s): %s", tok_id, exc)
            continue
        outcome_label = "YES" if idx_set == 1 else ("NO" if idx_set == 2 else f"idx={idx_set}")
        if bal > 0:
            log.info("    token %s  %-3s  balance=%s  → REDEEMABLE", tok_id, outcome_label, bal)
            redeemable.append(idx_set)
        else:
            log.info("    token %s  %-3s  balance=0   → already claimed / no position", tok_id, outcome_label)

    if not redeemable:
        log.info("    result : nothing to redeem.")
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
        log.info("    result : REDEEMED  tx=%s", tx_hash.hex())
        return tx_hash.hex(), nonce + 1
    except Exception as exc:
        log.error("    result : send_raw_transaction failed: %s", exc)
        return None, nonce


# ── One full redemption cycle ─────────────────────────────────────────────────

def run_once(w3: Web3, ctf, account: Account, wallet: str | None = None) -> None:
    wallet = wallet or account.address
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

    # ── Summary of found positions ────────────────────────────────────────────
    log.info("Found %d condition(s) with positions:", len(by_condition))
    for cond_id, tok_map in by_condition.items():
        mkt_name = None
        for tok_id in tok_map:
            mkt = token_to_market.get(str(tok_id))
            if mkt:
                mkt_name = mkt.get("question") or mkt.get("title") or mkt.get("slug")
                if mkt_name:
                    break
        label = f'"{mkt_name}"' if mkt_name else "(market unknown)"
        log.info("  • %s  %s  (%d token(s))", cond_id[:20] + "…", label, len(tok_map))

    log.info("Checking resolution status …")

    # 4. Iterate; maintain a running nonce for any txs sent this cycle
    nonce    = w3.eth.get_transaction_count(account.address, "pending")
    redeemed = 0

    for cond_id, tok_map in by_condition.items():
        tx_hash, nonce = redeem_condition(
            w3, ctf, account, cond_id, tok_map, nonce, token_to_market, wallet
        )
        if tx_hash:
            redeemed += 1

    log.info(
        "Cycle complete — %d/%d condition(s) redeemed.",
        redeemed, len(by_condition),
    )


# ── Entry point ───────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Polymarket auto-redeemer")
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        default=os.getenv("VERBOSE", "").lower() in ("1", "true", "yes"),
        help="Enable debug logging (also via VERBOSE=true in .env)",
    )
    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
        log.debug("Debug logging enabled.")

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
            run_once(w3, ctf, account, wallet_address)
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
