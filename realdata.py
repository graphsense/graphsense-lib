"""Old vs new THORChain memo check on real on-chain memos.

Sources:
  Midgard (THORChain's indexer): recent actions, memo from metadata.<type>.memo
  mempool.space: txs to the Bridgers BTC deposit address seen in the PR fixture
Usage: python3 -I realdata.py <path-to-gslib-src> <midgard_pages> <mempool_pages>
"""
import json
import sys
import time
import urllib.request
from collections import Counter

sys.path.insert(0, sys.argv[1])
from graphsenselib.defi.bridging.thorchain import (  # noqa: E402
    _memo_function,
    is_thorchain_memo as new,
    parse_op_return_memo,
)


def old(m):
    return bool(m) and len(m) >= 2 and m[0] in "=sS+-~"


def get(url):
    for _ in range(4):
        try:
            return json.load(urllib.request.urlopen(url, timeout=30))
        except Exception as e:  # noqa: BLE001
            err = e
            time.sleep(2)
    print('skip', url, err, file=sys.stderr)
    return None


MIDGARD = "https://gateway.liquify.com/chain/thorchain_midgard/v2/actions?limit=50"
BRIDGERS = "bc1q7t4vyehjsexdme84qhdgd4dawcn54djh0m78fz"

out = {"midgard": [], "bridgers": []}
dates = []
HEIGHTS = [None] + [h * 1_000_000 for h in (4, 6, 8, 10, 12, 14, 16, 18, 20, 22, 24, 26)]
pages = [(h, p) for h in HEIGHTS for p in range(int(sys.argv[2]))]
token = None
for h, p in pages:
    if p == 0:
        token = None
    base = MIDGARD + (f"&height={h}" if h else "")
    d = get(base + (f"&nextPageToken={token}" if token else ""))
    if d is None:
        out.setdefault("failed_pages", 0)
        out["failed_pages"] += 1
        token = None
        continue
    for a in d["actions"]:
        dates.append(int(a["date"]))
        for v in a["metadata"].values():
            m = v.get("memo") if isinstance(v, dict) else None
            if m:
                out["midgard"].append({"height": int(a["height"]), "type": a["type"], "status": a["status"], "memo": m})
    token = d["meta"].get("nextPageToken")
out["midgard_range"] = [min(dates), max(dates)]

last, heights = None, []
for _ in range(int(sys.argv[3])):
    url = f"https://mempool.space/api/address/{BRIDGERS}/txs/chain" + (f"/{last}" if last else "")
    txs = get(url)
    if not txs:
        break
    for t in txs:
        heights.append(t["status"].get("block_height", 0))
        incoming = any(o.get("scriptpubkey_address") == BRIDGERS for o in t["vout"])
        memos = [parse_op_return_memo(o["scriptpubkey"]) for o in t["vout"] if o["scriptpubkey"].startswith("6a")]
        memos = [m for m in memos if m]
        if incoming and memos:
            out["bridgers"].append({"txid": t["txid"], "memo": memos[0]})
    last = txs[-1]["txid"]
    time.sleep(0.4)
out["bridgers_range"] = [min(heights), max(heights)]

summary = {}
for name in ("midgard", "bridgers"):
    rows = out[name]
    for r in rows:
        r["old"], r["new"] = old(r["memo"]), new(r["memo"])
    c = Counter((r["old"], r["new"]) for r in rows)
    summary[name] = {"n": len(rows), "both": c[(True, True)], "lost": c[(True, False)],
                     "gained": c[(False, True)], "neither": c[(False, False)]}
# Of the THORChain memos the new check drops: which were real, successful actions?
lost = [r for r in out["midgard"] if r["old"] and not r["new"]]
summary["midgard_lost_by_status"] = Counter(f'{r["type"]}/{r["status"]}' for r in lost)
summary["midgard_lost_examples"] = Counter(_memo_function(r["memo"]) for r in lost).most_common(15)
summary["midgard_lost_success"] = [r["memo"][:70] for r in lost if r["status"] == "success" and r["type"] != "refund"][:20]
summary["midgard_neither_functions"] = Counter(_memo_function(r["memo"]) for r in out["midgard"] if not r["old"]).most_common(12)
summary["bridgers_tags"] = Counter(r["memo"].split(":")[0] for r in out["bridgers"]).most_common(12)
summary["failed_pages"] = out.get("failed_pages", 0)
summary["heights_seen"] = sorted({r["height"] // 1_000_000 for r in out["midgard"]})
summary["ranges"] = {"midgard_ns": out["midgard_range"], "bridgers_blocks": out["bridgers_range"]}
json.dump(out, open("/tmp/pr217/realdata.json", "w"))
print(json.dumps(summary, indent=1, default=str))
