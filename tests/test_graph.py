"""Unit tests for the pure graph algorithms (no database required)."""

import random

from app.graph import canonical_digest, find_cycle, topo_order


class TestTopoOrder:
    def test_empty(self):
        assert topo_order([], []) == []

    def test_chain(self):
        assert topo_order(["c", "a", "b"], [("a", "b"), ("b", "c")]) == ["a", "b", "c"]

    def test_lexicographic_tie_break(self):
        # Diamond: after a, the smaller id must come first.
        order = topo_order(
            ["d", "c", "b", "a"], [("a", "b"), ("a", "c"), ("b", "d"), ("c", "d")]
        )
        assert order == ["a", "b", "c", "d"]

    def test_independent_components_sorted(self):
        # Zero-indegree nodes are emitted smallest-first: a, then m unlocks
        # nothing larger than z until m is emitted (m < z).
        order = topo_order(["z", "a", "m"], [("a", "m")])
        assert order == ["a", "m", "z"]

    def test_cycle_returns_none(self):
        assert topo_order(["a", "b"], [("a", "b"), ("b", "a")]) is None

    def test_large_chain(self):
        n = 50_000
        tasks = [f"t{i:06d}" for i in range(n)]
        edges = [(f"t{i:06d}", f"t{i + 1:06d}") for i in range(n - 1)]
        assert topo_order(tasks, edges) == tasks


class TestFindCycle:
    def test_acyclic(self):
        assert find_cycle(["a", "b", "c"], [("a", "b"), ("b", "c")]) is None

    def test_simple_cycle_witness(self):
        witness = find_cycle(["x", "y", "z"], [("x", "y"), ("y", "z"), ("z", "x")])
        assert witness == ["x", "y", "z", "x"]

    def test_cycle_not_in_first_component(self):
        witness = find_cycle(["a", "m", "n"], [("m", "n"), ("n", "m")])
        assert witness == ["m", "n", "m"]

    def test_witness_starts_at_stack_ancestor(self):
        # a -> b -> c -> b (back edge c->b): witness is b..c..b, not a.
        witness = find_cycle(["a", "b", "c"], [("a", "b"), ("b", "c"), ("c", "b")])
        assert witness == ["b", "c", "b"]

    def test_first_back_edge_in_deterministic_order(self):
        # From a: neighbors ascending are [b, c]. b leads to a cycle via d;
        # ensure the witness follows the ascending traversal.
        edges = [("a", "b"), ("a", "c"), ("b", "d"), ("d", "b"), ("c", "c")]
        witness = find_cycle(["a", "b", "c", "d"], edges)
        assert witness == ["b", "d", "b"]

    def test_determinism(self):
        tasks = [f"t{i}" for i in range(200)]
        rng = random.Random(42)
        edges = []
        for _ in range(400):
            s, d = rng.sample(tasks, 2)
            if (s, d) not in edges:
                edges.append((s, d))
        first = find_cycle(tasks, edges)
        for _ in range(5):
            shuffled = edges[:]
            rng.shuffle(shuffled)
            assert find_cycle(tasks, shuffled) == first

    def test_deep_chain_no_recursion_error(self):
        n = 200_000
        tasks = [str(i) for i in range(n)]
        edges = [(str(i), str(i + 1)) for i in range(n - 1)]
        assert find_cycle(tasks, edges) is None

    def test_deep_cycle_found(self):
        n = 100_000
        tasks = [str(i) for i in range(n)]
        edges = [(str(i), str(i + 1)) for i in range(n - 1)] + [(str(n - 1), "0")]
        witness = find_cycle(tasks, edges)
        assert witness == [str(i) for i in range(n)] + ["0"]


class TestCanonicalDigest:
    def test_order_independent(self):
        tasks = ["b", "a", "c"]
        edges = [("a", "b"), ("b", "c")]
        d1 = canonical_digest(tasks, edges)
        d2 = canonical_digest(list(reversed(tasks)), list(reversed(edges)))
        assert d1 == d2

    def test_content_sensitive(self):
        d1 = canonical_digest(["a"], [])
        d2 = canonical_digest(["b"], [])
        d3 = canonical_digest(["a", "b"], [("a", "b")])
        assert len({d1, d2, d3}) == 3

    def test_format(self):
        d = canonical_digest([], [])
        assert len(d) == 64 and all(c in "0123456789abcdef" for c in d)
