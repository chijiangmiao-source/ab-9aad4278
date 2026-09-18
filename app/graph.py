"""Pure graph algorithms for sealing: deterministic cycle witness,
lexicographic topological order, and the canonical SHA-256 digest.

All functions are deterministic: iteration order never depends on hash
randomization or insertion order, only on task-id sort order.
"""

from __future__ import annotations

import hashlib
import heapq
from typing import Dict, Iterable, List, Sequence, Tuple

Edge = Tuple[str, str]

# Canonical digest format (documented in README):
#   line 0:  "cryoem-pipeline-digest/v1"
#   line 1:  "tasks:<N>"   followed by N lines "T <task_id>"   (task ids ascending)
#   then:    "edges:<M>"   followed by M lines "E <src> <dst>" (lexicographic by (src, dst))
# Task ids match [A-Za-z0-9][A-Za-z0-9_-]{0,63} so the space delimiter is unambiguous.
DIGEST_FORMAT_VERSION = "cryoem-pipeline-digest/v1"


def canonical_digest(task_ids: Iterable[str], edges: Iterable[Edge]) -> str:
    """SHA-256 over the canonical serialization of the task/edge sets.

    Order-independent: identical sets always yield the same digest regardless
    of the order the caller submitted them in.
    """
    tasks = sorted(set(task_ids))
    edge_list = sorted(set(edges))
    parts: List[str] = [DIGEST_FORMAT_VERSION, f"tasks:{len(tasks)}"]
    parts.extend(f"T {t}" for t in tasks)
    parts.append(f"edges:{len(edge_list)}")
    parts.extend(f"E {s} {d}" for s, d in edge_list)
    return hashlib.sha256("\n".join(parts).encode("utf-8")).hexdigest()


def find_cycle_witness(task_ids: Sequence[str], edges: Sequence[Edge]) -> List[str] | None:
    """Deterministic DFS cycle witness.

    Visits start nodes in ascending task-id order; adjacency lists are
    ascending. Returns the path from the first encountered on-stack target
    node to the current node, closed by repeating the target node
    (``[v, ..., u, v]``). Returns None when the graph is acyclic.

    Iterative (no recursion limit); O(V + E) time, O(V) memory beyond the
    adjacency map. Never enumerates paths.
    """
    adj: Dict[str, List[str]] = {}
    for t in task_ids:
        adj.setdefault(t, [])
    for s, d in edges:
        adj.setdefault(s, []).append(d)
        adj.setdefault(d, [])
    for lst in adj.values():
        lst.sort()

    WHITE, GRAY, BLACK = 0, 1, 2
    color: Dict[str, int] = {t: WHITE for t in adj}

    for start in sorted(adj):
        if color[start] != WHITE:
            continue
        color[start] = GRAY
        stack: List[str] = [start]          # explicit recursion stack
        next_idx: Dict[str, int] = {start: 0}
        path: List[str] = [start]           # nodes currently on the recursion stack
        on_path_pos: Dict[str, int] = {start: 0}
        while stack:
            node = stack[-1]
            i = next_idx[node]
            neighbors = adj[node]
            if i < len(neighbors):
                next_idx[node] = i + 1
                nb = neighbors[i]
                c = color[nb]
                if c == GRAY:
                    # First edge pointing at a node on the recursion stack.
                    return path[on_path_pos[nb]:] + [nb]
                if c == WHITE:
                    color[nb] = GRAY
                    stack.append(nb)
                    next_idx[nb] = 0
                    on_path_pos[nb] = len(path)
                    path.append(nb)
            else:
                stack.pop()
                path.pop()
                del on_path_pos[node]
                color[node] = BLACK
    return None


def lexicographic_topo_order(task_ids: Sequence[str], edges: Sequence[Edge]) -> List[str]:
    """Kahn's algorithm with a min-heap keyed by task id.

    Produces the unique topological order that is lexicographically smallest
    (ties broken by task id at every position). Assumes the graph is acyclic;
    the returned list has fewer than len(task_ids) entries otherwise.
    """
    adj: Dict[str, List[str]] = {}
    indeg: Dict[str, int] = {}
    for t in task_ids:
        adj.setdefault(t, [])
        indeg.setdefault(t, 0)
    for s, d in edges:
        adj.setdefault(s, []).append(d)
        indeg[d] = indeg.get(d, 0) + 1
        indeg.setdefault(s, 0)

    heap = [t for t, deg in indeg.items() if deg == 0]
    heapq.heapify(heap)
    order: List[str] = []
    while heap:
        node = heapq.heappop(heap)
        order.append(node)
        for nb in adj[node]:
            indeg[nb] -= 1
            if indeg[nb] == 0:
                heapq.heappush(heap, nb)
    return order
