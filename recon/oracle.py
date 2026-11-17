"""Independent oracle: expected differences computed by direct row-by-row
comparison of the two complete inputs.

The oracle knows nothing about summaries, hashing, or the protocol.  It reads
both *input* row lists and returns the exact expected difference set keyed by
row id, so tests can compare the protocol's findings against first principles
rather than against the product's own counters.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, Mapping


@dataclass(frozen=True)
class Expected:
    only_left: dict[str, dict[str, Any]]
    only_right: dict[str, dict[str, Any]]
    changed: dict[str, tuple[dict[str, Any], dict[str, Any]]]

    def counts(self) -> dict[str, int]:
        return {
            "only_left": len(self.only_left),
            "only_right": len(self.only_right),
            "changed": len(self.changed),
        }

    def row_ids(self) -> dict[str, set[str]]:
        return {
            "only_left": set(self.only_left),
            "only_right": set(self.only_right),
            "changed": set(self.changed),
        }


def _normalise(values: Mapping[str, Any], fields: Iterable[str]) -> tuple:
    return tuple(values[f] for f in fields)


def compare_inputs(
    left: Iterable[tuple[str, Mapping[str, Any]]],
    right: Iterable[tuple[str, Mapping[str, Any]]],
    fields: Iterable[str],
) -> Expected:
    fields = list(fields)
    lmap = {row_id: dict(values) for row_id, values in left}
    rmap = {row_id: dict(values) for row_id, values in right}
    if len(lmap) != len(list(left)) or len(rmap) != len(list(right)):
        raise ValueError("duplicate row ids in oracle input")
    only_left = {k: v for k, v in lmap.items() if k not in rmap}
    only_right = {k: v for k, v in rmap.items() if k not in lmap}
    changed = {
        k: (lmap[k], rmap[k])
        for k in lmap.keys() & rmap.keys()
        if _normalise(lmap[k], fields) != _normalise(rmap[k], fields)
    }
    return Expected(only_left, only_right, changed)
