#!/usr/bin/env python3
"""
diagnose_proxy.py — Figure out what type of proxy 0x092146388... is
and who its owner is.

Run: python diagnose_proxy.py
"""
import os, re
from web3 import Web3
from dotenv import load_dotenv

load_dotenv()

RPC = os.getenv("POLYGON_RPC_URL", "https://polygon-bor-rpc.publicnode.com")
PROXY = Web3.to_checksum_address("0x092146388ac74e1A97681B1E343f5985072729D1")

w3 = Web3(Web3.HTTPProvider(RPC))
assert w3.is_connected(), "Cannot connect to RPC"

print(f"Proxy : {PROXY}\n")

# ── 1. Bytecode & proxy type ───────────────────────────────────────────────
code = w3.eth.get_code(PROXY).hex()
print(f"Bytecode : {len(code)//2} bytes")

impl_to_inspect = code  # bytecode we'll scan for selectors

# EIP-1167 minimal proxy: 363d3d373d3d3d363d73<20-byte-impl>5af4...
if "363d3d373d3d3d363d73" in code:
    idx = code.index("363d3d373d3d3d363d73")
    impl_addr = Web3.to_checksum_address("0x" + code[idx+20 : idx+60])
    print(f"Type     : EIP-1167 minimal proxy")
    print(f"Impl     : {impl_addr}")
    impl_to_inspect = w3.eth.get_code(impl_addr).hex()
else:
    print(f"Type     : not a minimal proxy (checking other patterns)")

# EIP-1967 slots
EIP1967_IMPL  = "0x360894a13ba1a3210667c828492db98dca3e2076cc3735a920a3ca505d382bbc"
EIP1967_ADMIN = "0xb53127684a568b3173ae13b9f8a6016e243e63b6e8ee1178d6a717850b5d6103"
imp = w3.eth.get_storage_at(PROXY, EIP1967_IMPL).hex()
adm = w3.eth.get_storage_at(PROXY, EIP1967_ADMIN).hex()
if imp != "0" * 64:
    print(f"EIP-1967 impl  : 0x{imp[-40:]}")
if adm != "0" * 64:
    print(f"EIP-1967 admin : 0x{adm[-40:]}")

# ── 2. Storage slots 0-5 ──────────────────────────────────────────────────
print("\nStorage slots:")
for i in range(6):
    v = w3.eth.get_storage_at(PROXY, i).hex()
    print(f"  slot {i} : {v}  → as addr: 0x{v[-40:]}")

# ── 3. Function selectors in bytecode ─────────────────────────────────────
# PUSH4 (0x63) followed by 4 bytes followed by EQ (0x14) is how the
# EVM dispatcher checks function selectors.
sels = set(re.findall(r'63([0-9a-f]{8})14', impl_to_inspect))
known = {
    '8da5cb5b': 'owner()',
    '2f54bf6e': 'isOwner(address)',
    'a0e67e2b': 'getOwners()',
    'e75235b8': 'getThreshold()',
    '893d20e8': 'getOwner()',
    '468721a7': 'execute(address,uint256,bytes,uint8)',
    '6a761202': 'execTransaction(address,uint256,bytes,uint8,...)',
    'affed0e0': 'nonce()',
    'f698da25': 'domainSeparator()',
    'd8d11f78': 'getTransactionHash(...)',
    '7ecebe00': 'nonces(address)',
    'b61d27f6': 'execute(address,uint256,bytes)',     # 3-arg execute
    '0a1028c4': 'execute(bytes,bytes)',
}
print(f"\nSelectors in bytecode ({len(sels)}):")
for s in sorted(sels):
    print(f"  0x{s}  {known.get(s, '(unknown)')}")

# ── 4. Try raw eth_call with every candidate owner selector ───────────────
candidates = {
    '8da5cb5b': 'owner()',
    '893d20e8': 'getOwner()',
    'a0e67e2b': 'getOwners()',
}
print("\nRaw eth_call owner probes:")
for sel, name in candidates.items():
    try:
        result = w3.eth.call({"to": PROXY, "data": "0x" + sel})
        addr = "0x" + result.hex()[-40:] if len(result) >= 20 else result.hex()
        print(f"  {name:30s} → {addr}")
    except Exception as e:
        print(f"  {name:30s} → REVERT: {e}")

# ── 5. Check what private key is configured ───────────────────────────────
from eth_account import Account
pk = os.getenv("POLY_PRIVATE_KEY", "").strip()
if pk:
    eoa = Account.from_key(pk).address
    print(f"\nConfigured EOA (from POLY_PRIVATE_KEY): {eoa}")
    print(f"Proxy address                          : {PROXY}")
    print(f"Same? {eoa.lower() == PROXY.lower()}")
