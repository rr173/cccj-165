"""Deterministic dataset generation for demos and tests.

Generation is seeded and independent of the reconciliation code.  It never
communicates the intended differences to the protocol: the generator emits two
snapshots only, and the expected difference set is derived afterwards by a
separate oracle that compares the two inputs row by row.
"""

from __future__ import annotations

import random
from dataclasses import dataclass
from typing import Any

SCHEMA_FIELDS = (
    ("payload", "text", False),
    ("amount", "int64", True),
    ("active", "bool", False),
    ("note", "text", True),
)


def demo_schema():
    from .encoding import Schema

    return Schema.declare(*SCHEMA_FIELDS)


def demo_row(index: int) -> dict[str, Any]:
    """A deterministic business row; row_id is generated separately."""

    if index % 17 == 0:
        payload = None
    return {
        "payload": "" if index % 11 == 0 else f"payload-{index:08d}-{'x' * (index % 7)}",
        "amount": None if index % 13 == 0 else index * 3 - 5,
        "active": index % 2 == 0,
        "note": None if index % 19 == 0 else f"note {index} · ünïcode ✓",
    }


def base_rows(count: int) -> list[tuple[str, dict[str, Any]]]:
    rows = []
    for i in range(count):
        row_id = f"row-{i:09d}"
        values = demo_row(i)
        if values["payload"] is None:
            # payload is a non-nullable text column; use the empty string for
            # business rows and rely on explicit null columns elsewhere.
            values = dict(values)
            values["payload"] = ""
        rows.append((row_id, values))
    return rows


@dataclass
class Variant:
    name: str
    rows: list[tuple[str, dict[str, Any]]]
    intended: dict[str, Any]


def variant_sparse(count: int, *, seed: int = 7) -> Variant:
    """Sparse, deliberate differences: one changed, one deleted, one added."""

    rows = base_rows(count)
    changed_id, changed_old = rows[count // 3]
    changed_new = dict(changed_old)
    changed_new["amount"] = (changed_old["amount"] or 0) + 1
    changed_new["note"] = "changed value — same row_id"
    rows[count // 3] = (changed_id, changed_new)

    deleted_id, deleted_row = rows[count // 2]
    rows.pop(count // 2)

    added_id = f"zzz-added-{seed:04d}"
    added_row = {
        "payload": "brand new business row",
        "amount": -42,
        "active": True,
        "note": "added",
    }
    rows.append((added_id, added_row))
    return Variant(
        "sparse",
        rows,
        {
            "changed": (changed_id, changed_old, changed_new),
            "deleted": (deleted_id, deleted_row),
            "added": (added_id, added_row),
        },
    )


def variant_dense(count: int, *, seed: int = 11, change_fraction: float = 0.35) -> Variant:
    """Densely differing variant: many updates, some deletes, some inserts."""

    rng = random.Random(seed)
    rows = base_rows(count)
    out: list[tuple[str, dict[str, Any]]] = []
    deleted: list[str] = []
    changed: list[str] = []
    for i, (row_id, values) in enumerate(rows):
        roll = rng.random()
        if roll < change_fraction * 0.3:
            deleted.append(row_id)
            continue
        if roll < change_fraction:
            mutated = dict(values)
            mutated["amount"] = (values["amount"] or 0) + rng.randint(1, 1000)
            mutated["payload"] = values["payload"] + f"/v{rng.randint(1, 9)}"
            out.append((row_id, mutated))
            changed.append(row_id)
            continue
        out.append((row_id, values))
    added: list[str] = []
    for j in range(max(1, int(count * change_fraction * 0.2))):
        row_id = f"new-{j:09d}"
        out.append(
            (
                row_id,
                {
                    "payload": f"inserted {j}",
                    "amount": -j,
                    "active": j % 2 == 0,
                    "note": None,
                },
            )
        )
        added.append(row_id)
    return Variant("dense", out, {"changed": changed, "deleted": deleted, "added": added})
