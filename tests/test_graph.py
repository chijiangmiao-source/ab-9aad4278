"""Unit tests for the pure graph algorithms (no database needed)."""

from __future__ import annotations

import random

from app.graph import (canonical_digest, find_cycle_witness,
                       lexicographic_topo_order)


def test_digest_is_order_independent():
    tasks = ["b", "a", "c"]
    edges = [("a", "b"), ("a", "c")]
    d1 = canonical_digest(tasks, edges)
    d2 = canonical_digest(list(reversed(tasks)), list(reversed(edges)))
    assert d1 == d2
    assert len(d1) == 64 and all(ch in "0123456789abcdef" for ch in d1)


def test_digest_changes_with_content():
    assert canonical_digest(["a"], []) != canonical_digest(["a", "b"], [])
    assert canonical_digest(["a", "b"], []) != canonical_digest(["a", "b"],
                                                                [("a", "b")])


def test_cycle_witness_deterministic():
    tasks = ["m", "a", "z", "b"]
    edges = [("m", "a"), ("m", "z"), ("a", "b"), ("b", "m")]
    # DFS ascending: a -> b -> m -> a (gray) => witness a..m closed with a
    assert find_cycle_witness(tasks, edges) == ["a", "b", "m", "a"]


def test_cycle_witness_self_contained_closure():
    tasks = ["x", "y"]
    edges = [("x", "y"), ("y", "x")]
    w = find_cycle_witness(tasks, edges)
    assert w == ["x", "y", "x"]


def test_no_cycle_returns_none():
    tasks = ["a", "b", "c", "d"]
    edges = [("a", "b"), ("a", "c"), ("b", "d"), ("c", "d")]
    assert find_cycle_witness(tasks, edges) is None


def test_lexicographic_topo_order():
    tasks = ["b", "a", "c"]
    edges = [("c", "a")]
    # ready set starts as {b, c}: b first, then c, then a
    assert lexicographic_topo_order(tasks, edges) == ["b", "c", "a"]


def test_topo_respects_dependencies():
    rng = random.Random(42)
    n = 500
    tasks = [f"t{i:04d}" for i in range(n)]
    edges = []
    for _ in range(2000):
        i, j = sorted(rng.sample(range(n), 2))
        edges.append((tasks[i], tasks[j]))
    edges = sorted(set(edges))
    order = lexicographic_topo_order(tasks, edges)
    assert find_cycle_witness(tasks, edges) is None
    pos = {t: i for i, t in enumerate(order)}
    assert len(order) == n
    for s, d in edges:
        assert pos[s] < pos[d]


def test_large_chain_scales():
    # 50k-node chain: iterative DFS must not hit recursion limits
    n = 50_000
    tasks = [f"t{i:06d}" for i in range(n)]
    edges = [(tasks[i], tasks[i + 1]) for i in range(n - 1)]
    assert find_cycle_witness(tasks, edges) is None
    order = lexicographic_topo_order(tasks, edges)
    assert order == sorted(tasks)
    # add a back edge from the end to the middle -> cycle found fast
    edges2 = edges + [(tasks[-1], tasks[n // 2])]
    w = find_cycle_witness(tasks, edges2)
    assert w is not None and w[0] == w[-1] == tasks[n // 2]
