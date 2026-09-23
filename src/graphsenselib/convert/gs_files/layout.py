"""Direction-aware layered layout for pathfinder specs, plus layout metrics.

:func:`graphsenselib.convert.gs_files.encoder.apply_hierarchical_layout`
dispatches here when the spec says which addresses send and which receive
each tx (``senders`` / ``receivers`` on a tx entry, e.g. filled in from
the backend by :func:`graphsenselib.pathfinder.annotate_tx_flows`). The
``.gs`` format itself carries no direction — an agg edge's ``a``/``b`` are
just its two ends — so without that information the undirected BFS layout
is all we can do, and it is what runs.

The layout is a small Sugiyama-style pipeline:

1. **Columns** — multi-source BFS from the starting points, as before,
   except that a step *against* the money flow goes one column left
   instead of right. Senders therefore sit left of their tx and receivers
   right of it; inflows to a starting point land at negative x. A step
   whose direction is unknown goes right, matching the undirected layout.
   Where the graph has cycles, a tx then moves to the column where the
   fewest of its links point backwards.
2. **Order within a column** — seeded from a pre-order walk of the BFS
   spanning tree (so subtrees stay together), then barycenter sweeps
   left→right and right→left; the ordering with the fewest crossings
   between adjacent columns wins.
3. **Vertical position** — minimise the total squared vertical length of
   all edges (direct address links count less — they summarise, not
   show, the flow), subject to each column keeping its order and a minimum
   node spacing. Solved exactly one column at a time (weighted isotonic
   regression, pool-adjacent-violators) and iterated to convergence.
   This keeps edges straight, centres a node between its neighbours, and
   spreads a pile of txs between the same address pair symmetrically.

This module is pure stdlib — it is vendored into the python client.
"""

from __future__ import annotations

from collections import deque
from typing import Optional

from .encoder import (
    _HIER_X_STEP,
    _HIER_Y_STEP,
    _LABEL_LINE_HEIGHT,
    _label_line_count,
    _spec_item_to_dict,
    normalize_address_id,
    normalize_tx_id,
)

Key = tuple[str, str]

# Barycenter sweeps: each round is one left→right and one right→left pass.
# Small graphs settle in two or three; the best ordering seen is kept.
_ORDER_ROUNDS = 12
# Coordinate relaxation: stop once no node moves more than this.
_RELAX_TOL = 1e-9
_RELAX_MAX_ITER = 2000
# Weight for a node with no neighbour in another column: it still takes
# part in spacing but has no pull of its own, so it stays where it is.
_ISOLATED_WEIGHT = 1e-3
# Pull of a direct address—address link relative to an address—tx link.
_ADDRESS_LINK_WEIGHT = 0.1
# Pull of a link to a swap/bridge leg tx, whose addresses the UI moves
# onto the tx's row.
_LEG_LINK_WEIGHT = 20.0
# Weight that keeps a snapped node in place while its column is re-spaced.
_PINNED_WEIGHT = 1e9


def has_flow_info(spec: dict) -> bool:
    """True when at least one tx in ``spec`` names its senders or receivers."""
    for t in spec.get("txs", []):
        if isinstance(t, dict) and (t.get("senders") or t.get("receivers")):
            return True
    return False


class _Graph:
    """Nodes, undirected adjacency and known flow direction of a spec."""

    def __init__(self, spec: dict) -> None:
        self.addresses = [_spec_item_to_dict(a) for a in spec.get("addresses", [])]
        self.txs = [_spec_item_to_dict(t) for t in spec.get("txs", [])]
        self.edges = list(spec.get("agg_edges", []))
        self.nodes: list[Key] = []
        self.adj: dict[Key, set[Key]] = {}
        # Links as drawn: address—tx for every tx on an agg edge, and a
        # direct address—address link for an agg edge without txs.
        self.links: set[tuple[Key, Key]] = set()

        for a in self.addresses:
            self._register(("addr", normalize_address_id(a["id"])))
        for t in self.txs:
            self._register(("tx", normalize_tx_id(t["id"])))
        # Only txs listed in ``txs`` are drawn. An edge may reference more
        # — the Pathfinder UI saves an account-model edge with both the
        # base tx hash and its ``_I``/``_T`` sub-payment, but draws only
        # the listed one — and an unlisted tx must not take up a row. An
        # edge with no drawn tx is drawn as a direct address link.
        drawn = {("tx", normalize_tx_id(t["id"])) for t in self.txs}
        for e in self.edges:
            a_key = ("addr", normalize_address_id(e["a"]))
            b_key = ("addr", normalize_address_id(e["b"]))
            self._register(a_key)
            self._register(b_key)
            tx_ids = [
                tid
                for tid in e.get("tx_ids") or []
                if ("tx", normalize_tx_id(tid)) in drawn
            ]
            if not tx_ids:
                self._link(a_key, b_key)
            for tid in tx_ids:
                t_key = ("tx", normalize_tx_id(tid))
                self._link(a_key, t_key)
                self._link(b_key, t_key)

        self.position = {key: i for i, key in enumerate(self.nodes)}

        # (upstream, downstream) pairs. An address that is a sender of a
        # tx is upstream of it — also when it receives change back, which
        # keeps a change address on the spending side.
        self.flow: set[tuple[Key, Key]] = set()
        for t in self.txs:
            senders = {normalize_address_id(a) for a in t.get("senders") or []}
            receivers = {normalize_address_id(a) for a in t.get("receivers") or []}
            if not senders and not receivers:
                continue
            t_key = ("tx", normalize_tx_id(t["id"]))
            for n in self.adj.get(t_key, ()):
                if n[0] != "addr":
                    continue
                if n[1] in senders:
                    self.flow.add((n, t_key))
                elif n[1] in receivers:
                    self.flow.add((t_key, n))

        starts: list[Key] = []
        for a in self.addresses:
            if a.get("starting_point"):
                starts.append(("addr", normalize_address_id(a["id"])))
        for t in self.txs:
            if t.get("starting_point"):
                starts.append(("tx", normalize_tx_id(t["id"])))
        self.starts = list(dict.fromkeys(starts))

        # Swaps and bridges the Pathfinder UI will draw (``conversions``,
        # see graphsenselib.pathfinder.annotate_conversions). The UI links
        # the input leg's first output address to the output leg's first
        # input address and, on load, moves the addresses of both legs
        # into a U-turn: the output leg sits in the input leg's column,
        # below it, and runs right to left. The layout reproduces that
        # arrangement so the UI's move is a no-op. Only the output leg
        # is mirrored; flow beyond its addresses runs right again. The
        # bridge link joins the two chains for the layout but is not a
        # drawn link.
        self.bridges: set[frozenset[Key]] = set()
        # (input-leg tx, output-leg tx, edge_from, edge_to)
        self.conversions: list[tuple[Key, Key, Key, Key]] = []
        for c in spec.get("conversions") or []:
            i = ("tx", normalize_tx_id(c["input_tx"]))
            o = ("tx", normalize_tx_id(c["output_tx"]))
            a = ("addr", normalize_address_id(c["edge_from"]))
            b = ("addr", normalize_address_id(c["edge_to"]))
            if i not in drawn or o not in drawn or a == b:
                continue
            if a not in self.adj or b not in self.adj:
                continue
            self.adj[a].add(b)
            self.adj[b].add(a)
            self.bridges.add(frozenset((a, b)))
            self.conversions.append((i, o, a, b))
        self.mirrored = {o for _i, o, _a, _b in self.conversions}

    def orient(self, u: Key, v: Key) -> int:
        """Drawn direction of the link u—v: -1 for a link of a conversion's
        output leg, which the UI draws right to left, else +1."""
        return -1 if u in self.mirrored or v in self.mirrored else 1

    def _register(self, key: Key) -> None:
        if key not in self.adj:
            self.adj[key] = set()
            self.nodes.append(key)

    def _link(self, u: Key, v: Key) -> None:
        self.adj[u].add(v)
        self.adj[v].add(u)
        self.links.add((u, v) if u <= v else (v, u))

    def label_lines(self) -> dict[Key, int]:
        lines: dict[Key, int] = {}
        for kind, items, normalize in (
            ("addr", self.addresses, normalize_address_id),
            ("tx", self.txs, normalize_tx_id),
        ):
            for item in items:
                key = (kind, normalize(item["id"]))
                lines[key] = max(
                    lines.get(key, 1), _label_line_count(item.get("label"))
                )
        return lines


def _assign_columns(
    g: _Graph,
) -> tuple[dict[Key, int], dict[Key, Optional[Key]], list[Key]]:
    """Signed BFS levels, the spanning-tree parent of every node, and the
    tree roots (the starting points, then one anchor per unconnected part).

    A step along the money flow goes one column right, against it one
    column left — mirrored on a conversion's output leg (see
    :meth:`_Graph.orient`). A bridge link keeps the column.

    A part of the graph that no starting point reaches — typically the
    far side of a cross-chain swap, which no agg edge links — is laid out
    the same way from an anchor of its own, and shifted to begin one
    blank column right of everything placed so far. Its anchor is its
    first address that receives nothing within the part (where the money
    enters it), else its first node.
    """
    level: dict[Key, int] = {}
    parent: dict[Key, Optional[Key]] = {}

    def bridge(u: Key, v: Key) -> bool:
        return frozenset((u, v)) in g.bridges

    def directed(u: Key, v: Key) -> bool:
        return (u, v) in g.flow or (v, u) in g.flow or bridge(u, v)

    def grow(roots: list[Key]) -> list[Key]:
        for r in roots:
            level[r] = 0
            parent[r] = None
        reached = list(roots)
        # Two passes: first only along links whose direction is known,
        # then along the rest from everything reached. A link without
        # direction (an edge without a drawn tx, or a tx the lookup
        # couldn't resolve) is a shortcut that would otherwise place a
        # node before the flow gets there, e.g. an exchange deposit
        # linked straight to the victim, which would pull the hop paying
        # into it to the wrong side.
        for follow in (directed, lambda _u, _v: True):
            queue: deque[Key] = deque(reached)
            while queue:
                n = queue.popleft()
                for m in sorted(g.adj[n], key=lambda k: g.position[k]):
                    if m in level or not follow(n, m):
                        continue
                    if bridge(n, m):
                        level[m] = level[n]
                    else:
                        step = -1 if (m, n) in g.flow else 1
                        level[m] = level[n] + g.orient(n, m) * step
                    parent[m] = n
                    reached.append(m)
                    queue.append(m)
        return reached

    grow(g.starts)
    roots = list(g.starts)
    receives = {v for _u, v in g.flow}
    for node in g.nodes:
        if node in level:
            continue
        part = _reachable(g, node)
        anchor = next(
            (k for k in part if k[0] == "addr" and k not in receives),
            node,
        )
        base = max(level.values()) + 2 if level else 0
        reached = grow([anchor])
        shift = base - min(level[k] for k in reached)
        for k in reached:
            level[k] += shift
        roots.append(anchor)
    return level, parent, roots


def _reachable(g: _Graph, start: Key) -> list[Key]:
    """Nodes connected to ``start``, in spec order."""
    seen = {start}
    stack = [start]
    while stack:
        for m in g.adj[stack.pop()]:
            if m not in seen:
                seen.add(m)
                stack.append(m)
    return sorted(seen, key=lambda k: g.position[k])


def _straighten_txs(
    g: _Graph,
    level: dict[Key, int],
    parent: dict[Key, Optional[Key]],
) -> None:
    """Move a tx to the column where fewest of its links point backwards.

    BFS puts a tx next to whichever endpoint reached it first. When the
    graph has more than one path (a round trip, a second start), its
    other endpoints can end up on the wrong side of it. Addresses stay
    put; only the tx moves, keeping the parity of its column so txs and
    addresses keep alternating, and only when that strictly reduces the
    backward links. Its spanning-tree children, if any, move with it.
    """
    for t in g.nodes:
        if t[0] != "tx" or t not in level or parent.get(t) is None:
            continue
        if any(t in (i, o) for i, o, _a, _b in g.conversions):
            continue  # the UI fixes where conversion legs go
        if any(parent.get(m) == t for m in g.adj[t]):
            continue  # a tree parent: moving it would strand its subtree
        ups = [level[u] for u in g.adj[t] if (u, t) in g.flow and u in level]
        downs = [level[d] for d in g.adj[t] if (t, d) in g.flow and d in level]
        if not ups and not downs:
            continue

        o = g.orient(t, t)

        def backwards(lv: int) -> int:
            return sum(o * u >= o * lv for u in ups) + sum(
                o * d <= o * lv for d in downs
            )

        here = level[t]
        lo = min(ups + downs) - 1
        hi = max(ups + downs) + 1
        candidates = [lv for lv in range(lo, hi + 1) if (lv - here) % 2 == 0]
        best = min(candidates, key=lambda lv: (backwards(lv), abs(lv - here)))
        if backwards(best) < backwards(here):
            level[t] = best


def _initial_order(
    g: _Graph,
    level: dict[Key, int],
    parent: dict[Key, Optional[Key]],
    roots: list[Key],
) -> dict[int, list[Key]]:
    """Columns in spanning-tree pre-order.

    Pre-order keeps every subtree contiguous in each column it spans,
    which is what the old tidy-tree layout achieved and a good seed for
    the crossing reduction. Iterative so a long peel chain can't hit the
    recursion limit.
    """
    children: dict[Key, list[Key]] = {k: [] for k in level}
    for child, par in parent.items():
        if par is not None:
            children[par].append(child)
    for kids in children.values():
        kids.sort(key=lambda k: g.position[k])

    columns: dict[int, list[Key]] = {}
    for root in roots:
        stack = [root]
        while stack:
            node = stack.pop()
            columns.setdefault(level[node], []).append(node)
            stack.extend(reversed(children[node]))
    return columns


def _components(g: _Graph) -> dict[Key, int]:
    """Connected-component index of every node."""
    component: dict[Key, int] = {}
    for i, root in enumerate(g.nodes):
        if root in component:
            continue
        stack = [root]
        component[root] = i
        while stack:
            for m in g.adj[stack.pop()]:
                if m not in component:
                    component[m] = i
                    stack.append(m)
    return component


def _crossings_between(
    left: list[Key], right: list[Key], adj: dict[Key, set[Key]]
) -> int:
    pos_r = {k: i for i, k in enumerate(right)}
    segs = []
    for i, u in enumerate(left):
        for v in adj[u]:
            j = pos_r.get(v)
            if j is not None:
                segs.append((i, j))
    segs.sort()
    count = 0
    for x in range(len(segs)):
        for y in range(x + 1, len(segs)):
            if segs[x][0] < segs[y][0] and segs[x][1] > segs[y][1]:
                count += 1
    return count


def _total_crossings(
    columns: dict[int, list[Key]], cols: list[int], adj: dict[Key, set[Key]]
) -> int:
    return sum(
        _crossings_between(columns[a], columns[b], adj)
        for a, b in zip(cols, cols[1:])
        if b == a + 1
    )


def _reorder(
    column: list[Key], fixed: list[Key], adj: dict[Key, set[Key]]
) -> list[Key]:
    """Sort ``column`` by the mean position of its neighbours in ``fixed``.

    A node without neighbours there keeps its current index as its key,
    so it stays roughly in place. The sort is stable, so equal keys — the
    txs of one multi-tx edge — keep their relative order.
    """
    pos = {k: i for i, k in enumerate(fixed)}
    keys = {}
    for i, v in enumerate(column):
        ps = [pos[u] for u in adj[v] if u in pos]
        keys[v] = sum(ps) / len(ps) if ps else float(i)
    return sorted(column, key=lambda v: keys[v])


def _glue(column: list[Key], pairs: list[tuple[Key, Key]]) -> list[Key]:
    """Put each ``lower`` directly below its ``upper`` where both are in
    ``column`` — the stacking the Pathfinder UI uses for a conversion."""
    out = list(column)
    for upper, lower in pairs:
        if upper != lower and upper in out and lower in out:
            out.remove(lower)
            out.insert(out.index(upper) + 1, lower)
    return out


def _minimise_crossings(
    columns: dict[int, list[Key]],
    adj: dict[Key, set[Key]],
    glue=lambda col: col,
) -> dict[int, list[Key]]:
    cols = sorted(columns)
    best = {c: list(v) for c, v in columns.items()}
    best_count = _total_crossings(best, cols, adj)
    current = {c: list(v) for c, v in columns.items()}
    for _ in range(_ORDER_ROUNDS):
        if best_count == 0:
            break
        for a, b in zip(cols, cols[1:]):
            current[b] = glue(_reorder(current[b], current[a], adj))
        for a, b in zip(reversed(cols[1:]), reversed(cols[:-1])):
            current[b] = glue(_reorder(current[b], current[a], adj))
        count = _total_crossings(current, cols, adj)
        if count < best_count:
            best = {c: list(v) for c, v in current.items()}
            best_count = count
    return best


def _pack(desired: list[float], weights: list[float], gaps: list[float]) -> list[float]:
    """Closest positions to ``desired`` that keep order and minimum gaps.

    Minimises ``sum(w_i * (y_i - desired_i)**2)`` subject to
    ``y_{i+1} - y_i >= gaps[i]``. Substituting ``z_i = y_i - offset_i``
    (``offset`` = running sum of gaps) turns the gaps into plain
    monotonicity, which is weighted isotonic regression, solved exactly
    by pool-adjacent-violators.
    """
    offsets = [0.0]
    for gap in gaps:
        offsets.append(offsets[-1] + gap)
    # Each block: [total weight, weighted sum, member count].
    blocks: list[list[float]] = []
    for d, w, off in zip(desired, weights, offsets):
        blocks.append([w, w * (d - off), 1])
        while len(blocks) > 1 and (
            blocks[-2][1] / blocks[-2][0] > blocks[-1][1] / blocks[-1][0]
        ):
            w2, s2, c2 = blocks.pop()
            blocks[-1][0] += w2
            blocks[-1][1] += s2
            blocks[-1][2] += c2
    z: list[float] = []
    for w, s, c in blocks:
        z.extend([s / w] * int(c))
    return [zi + off for zi, off in zip(z, offsets)]


def _assign_rows(
    columns: dict[int, list[Key]],
    adj: dict[Key, set[Key]],
    level: dict[Key, int],
    gap_between,
    link_weight,
) -> dict[Key, float]:
    """Vertical positions: minimum total squared edge height, per-column
    order and spacing kept (see the module docstring)."""
    y: dict[Key, float] = {}
    for col in columns.values():
        acc = 0.0
        for i, v in enumerate(col):
            if i:
                acc += gap_between(col[i - 1], v)
            y[v] = acc

    cols = sorted(columns)
    sweep = cols + cols[-2:0:-1] if len(cols) > 1 else cols
    for _ in range(_RELAX_MAX_ITER):
        moved = 0.0
        for c in sweep:
            col = columns[c]
            desired, weights = [], []
            for v in col:
                ns = [
                    (y[u], link_weight(u, v))
                    for u in adj[v]
                    if level.get(u, c) != c and u in y
                ]
                if ns:
                    w = sum(wt for _y, wt in ns)
                    desired.append(sum(yu * wt for yu, wt in ns) / w)
                    weights.append(w)
                else:
                    desired.append(y[v])
                    weights.append(_ISOLATED_WEIGHT)
            gaps = [gap_between(a, b) for a, b in zip(col, col[1:])]
            for v, new in zip(col, _pack(desired, weights, gaps)):
                moved = max(moved, abs(new - y[v]))
                y[v] = new
        if moved < _RELAX_TOL:
            break
    return y


def directed_layout(spec: dict) -> dict:
    """Return a copy of ``spec`` with ``x``/``y`` on every node, laid out
    by money flow. See the module docstring for the algorithm.

    Caller-provided ``x``/``y`` on an item are preserved verbatim. A
    part of the graph no starting point reaches is laid out from its own
    anchor, right of the rest (see :func:`_assign_columns`).
    """
    g = _Graph(spec)
    level, parent, roots = _assign_columns(g)
    _straighten_txs(g, level, parent)

    # Conversion legs: the output leg below the input leg, the bridge's
    # far end below its near end, and — for single-address sides — the
    # output leg's receiver below the input leg's sender.
    glued: list[tuple[Key, Key]] = []
    legs: set[Key] = set()
    leg_order: list[Key] = []
    for i, o, a, b in g.conversions:
        glued += [(i, o), (a, b)]
        legs |= {i, o}
        leg_order += [i, o]
        senders = [u for u in g.adj[i] if (u, i) in g.flow]
        receivers = [v for v in g.adj[o] if (o, v) in g.flow and v != b]
        if len(senders) == 1 and len(receivers) == 1:
            glued.append((senders[0], receivers[0]))

    def glue(col: list[Key]) -> list[Key]:
        return _glue(col, glued)

    columns = {c: glue(v) for c, v in _initial_order(g, level, parent, roots).items()}
    columns = _minimise_crossings(columns, g.adj, glue)

    lines = g.label_lines()
    component = _components(g)

    def gap_between(a: Key, b: Key) -> float:
        extra = max(lines.get(a, 1), lines.get(b, 1)) - 1
        gap = _HIER_Y_STEP + extra * _LABEL_LINE_HEIGHT
        # A blank row between graphs that share no edge. Starting points
        # joined through the graph are one drawing and get no gap.
        return gap * 2 if component[a] != component[b] else gap

    # Addresses of conversion legs, with the legs each one is on.
    on_legs: dict[Key, list[Key]] = {}
    for t in leg_order:
        for a in sorted(g.adj[t], key=lambda k: g.position[k]):
            if a[0] == "addr":
                on_legs.setdefault(a, []).append(t)

    def link_weight(u: Key, v: Key) -> float:
        # The UI puts a conversion leg's addresses on the leg's row; pull
        # them there hard — except the swapper on both legs of a swap,
        # which would drag its whole path towards the swap. A direct
        # address link (an edge without a drawn tx) is a summary, not a
        # step of the flow, and often spans many columns: at full weight
        # it would bend the path.
        if (u in legs and len(on_legs.get(v, ())) == 1) or (
            v in legs and len(on_legs.get(u, ())) == 1
        ):
            return _LEG_LINK_WEIGHT
        return _ADDRESS_LINK_WEIGHT if u[0] == v[0] == "addr" else 1.0

    rows = _assign_rows(columns, g.adj, level, gap_between, link_weight)

    # The UI puts a conversion leg's addresses exactly on the leg's row.
    # Snap them there, then re-space each column around the snapped
    # nodes: they stay put, the others move as little as possible. An
    # address on both legs — the swapper of a DEX swap, which pays in on
    # one leg and gets paid on the other — has two rows to be on and
    # stays between them.
    snapped: set[Key] = set()
    for a, ts in on_legs.items():
        if len(ts) == 1:
            rows[a] = rows[ts[0]]
            snapped.add(a)
    if snapped:
        for col in columns.values():
            if not snapped & set(col):
                continue
            weights = [_PINNED_WEIGHT if v in snapped else 1.0 for v in col]
            gaps = [gap_between(a, b) for a, b in zip(col, col[1:])]
            for v, new in zip(col, _pack([rows[v] for v in col], weights, gaps)):
                rows[v] = new

    shift = rows[roots[0]] if roots else 0.0
    coords: dict[Key, tuple[float, float]] = {}
    for key, lvl in level.items():
        coords[key] = (float(lvl) * _HIER_X_STEP, _tidy(rows[key] - shift))

    def _apply(items: list[dict], kind: str) -> list[dict]:
        normalize = normalize_address_id if kind == "addr" else normalize_tx_id
        out: list[dict] = []
        for item in items:
            laid = coords.get((kind, normalize(item["id"])), (0.0, 0.0))
            new = dict(item)
            if new.get("x") is None:
                new["x"] = laid[0]
            if new.get("y") is None:
                new["y"] = laid[1]
            out.append(new)
        return out

    return {
        **spec,
        "addresses": _apply(g.addresses, "addr"),
        "txs": _apply(g.txs, "tx"),
    }


def _tidy(v: float) -> float:
    """Round away float noise from the relaxation (and -0.0). A thousandth
    of a graph unit is far below what the UI can show."""
    return round(v, 3) + 0.0


# ---------------------------------------------------------------------------
# Metrics
# ---------------------------------------------------------------------------


def _segments_cross(
    p1: tuple[float, float],
    p2: tuple[float, float],
    q1: tuple[float, float],
    q2: tuple[float, float],
) -> bool:
    """Proper intersection of two segments (touching doesn't count)."""

    def orient(a, b, c) -> float:
        return (b[0] - a[0]) * (c[1] - a[1]) - (b[1] - a[1]) * (c[0] - a[0])

    d1, d2 = orient(q1, q2, p1), orient(q1, q2, p2)
    d3, d4 = orient(p1, p2, q1), orient(p1, p2, q2)
    eps = 1e-9
    return ((d1 > eps and d2 < -eps) or (d1 < -eps and d2 > eps)) and (
        (d3 > eps and d4 < -eps) or (d3 < -eps and d4 > eps)
    )


def layout_metrics(spec: dict) -> dict:
    """Quality numbers for a laid-out spec (every node needs ``x``/``y``).

    - ``nodes`` / ``links``: size of the drawing.
    - ``overlaps``: node pairs in the same column less than half a row
      apart — they collide on screen. ``cramped``: pairs less than a
      full row apart — they don't collide, but labels may.
    - ``crossings``: pairs of links that properly intersect.
    - ``flow_links``: address—tx links whose direction is known from
      ``senders`` / ``receivers``; ``flow_backwards``: how many of those
      point right-to-left (a sender drawn right of its tx, or a receiver
      left of it). Zero when the spec carries no flow information.
    - ``mean_link_dy``: mean vertical extent of a link, in rows — how far
      edges are from straight.
    """
    g = _Graph(spec)
    pos: dict[Key, tuple[float, float]] = {}
    for kind, items, normalize in (
        ("addr", g.addresses, normalize_address_id),
        ("tx", g.txs, normalize_tx_id),
    ):
        for item in items:
            if item.get("x") is None or item.get("y") is None:
                continue
            pos.setdefault((kind, normalize(item["id"])), (item["x"], item["y"]))

    placed = [k for k in g.nodes if k in pos]
    by_col: dict[float, list[float]] = {}
    for k in placed:
        by_col.setdefault(round(pos[k][0], 6), []).append(pos[k][1])
    overlaps = cramped = 0
    for ys in by_col.values():
        ys.sort()
        for i in range(len(ys)):
            for j in range(i + 1, len(ys)):
                dy = ys[j] - ys[i]
                if dy >= _HIER_Y_STEP - 1e-9:
                    break
                cramped += 1
                if dy < _HIER_Y_STEP / 2 - 1e-9:
                    overlaps += 1

    links = sorted(lk for lk in g.links if lk[0] in pos and lk[1] in pos)
    crossings = 0
    for i in range(len(links)):
        u1, v1 = links[i]
        for j in range(i + 1, len(links)):
            u2, v2 = links[j]
            if {u1, v1} & {u2, v2}:
                continue
            if _segments_cross(pos[u1], pos[v1], pos[u2], pos[v2]):
                crossings += 1

    flow = [(u, v) for u, v in g.flow if u in pos and v in pos]

    # A conversion's output leg is drawn right to left by the UI.
    def is_backwards(u: Key, v: Key) -> bool:
        return g.orient(u, v) * (pos[v][0] - pos[u][0]) <= 0

    backwards = sum(1 for u, v in flow if is_backwards(u, v))

    dys = [abs(pos[u][1] - pos[v][1]) / _HIER_Y_STEP for u, v in links]
    return {
        "nodes": len(placed),
        "links": len(links),
        "overlaps": overlaps,
        "cramped": cramped,
        "crossings": crossings,
        "flow_links": len(flow),
        "flow_backwards": backwards,
        "mean_link_dy": round(sum(dys) / len(dys), 3) if dys else 0.0,
    }
