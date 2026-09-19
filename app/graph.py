"""Pure graph algorithms used at seal time.

All functions are deterministic and depend only on the *sets* of tasks and
edges, never on insertion order, so different submission orders of the same
graph yield identical results. Complexity is O(V + E) (plus sorting), with no
path enumeration and no per-edge full-graph rescans.
"""

from __future__ import annotations

import hashlib
import heapq


def canonical_digest(tasks: list[str], edges: list[tuple[str, str]]) -> str:
    """SHA-256 over the canonical (sorted) task set and edge set."""
    h = hashlib.sha256()
    for t in sorted(tasks):
        h.update(b"T:")
        h.update(t.encode("utf-8"))
        h.update(b"\n")
    for src, dst in sorted(edges):
        h.update(b"E:")
        h.update(src.encode("utf-8"))
        h.update(b"->")
        h.update(dst.encode("utf-8"))
        h.update(b"\n")
    return h.hexdigest()


def topo_order(tasks: list[str], edges: list[tuple[str, str]]) -> list[str] | None:
    """Unique topological order with lexicographic tie-breaking (min task id
    first). Returns None if the graph has a cycle."""
    indeg: dict[str, int] = {t: 0 for t in tasks}
    adj: dict[str, list[str]] = {t: [] for t in tasks}
    for src, dst in edges:
        adj[src].append(dst)
        indeg[dst] += 1
    heap = [t for t, d in indeg.items() if d == 0]
    heapq.heapify(heap)
    order: list[str] = []
    while heap:
        node = heapq.heappop(heap)
        order.append(node)
        for nxt in adj[node]:
            indeg[nxt] -= 1
            if indeg[nxt] == 0:
                heapq.heappush(heap, nxt)
    if len(order) != len(indeg):
        return None
    return order


def find_cycle(tasks: list[str], edges: list[tuple[str, str]]) -> list[str] | None:
    """Deterministic cycle witness.

    Depth-first traversal that visits start nodes in ascending task-id order
    and expands each node's successors in ascending order. The witness is the
    first edge (u -> v) encountered where v is on the current recursion stack;
    the returned sequence is the stack path from v down to u, closed by v.

    Iterative implementation: safe for chains of 50k+ tasks.
    """
    adj: dict[str, list[str]] = {t: [] for t in tasks}
    for src, dst in edges:
        adj[src].append(dst)
    for lst in adj.values():
        lst.sort()

    WHITE, GRAY, BLACK = 0, 1, 2
    color: dict[str, int] = {t: WHITE for t in tasks}

    for start in sorted(tasks):
        if color[start] != WHITE:
            continue
        color[start] = GRAY
        stack: list[str] = [start]                    # the recursion stack (path)
        iters = [iter(adj[start])]
        while stack:
            node = stack[-1]
            descended = False
            for nb in iters[-1]:
                c = color[nb]
                if c == WHITE:
                    color[nb] = GRAY
                    stack.append(nb)
                    iters.append(iter(adj[nb]))
                    descended = True
                    break
                if c == GRAY:
                    # Back edge node -> nb; nb is on the current stack.
                    idx = stack.index(nb)
                    return stack[idx:] + [nb]
            if not descended:
                color[node] = BLACK
                stack.pop()
                iters.pop()
    return None
