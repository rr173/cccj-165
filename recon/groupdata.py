"""Deterministic relational dataset generation for the group demo.

The generator is seeded and independent of the reconciliation code.  It emits
two dataset groups (an older and a newer collection batch) whose differences
combine, in one batch: inserted parents referenced by inserted children,
same-batch parent+child deletions, changed values, children re-pointed to
other parents, and cycles through nullable relations.  Expected differences
are derived afterwards by the independent oracle, never by the generator.
"""

from __future__ import annotations

import random
from typing import Any

from .dataset import ForeignKey, RelationManifest
from .encoding import Schema

DATASET_ID = "dataset-demo"
BATCH_LEFT = "batch-0001"
BATCH_RIGHT = "batch-0002"


def group_schemas() -> dict[str, Schema]:
    return {
        "customers": Schema.declare(
            ("name", "text", False),
            ("tier", "int64", True),
        ),
        "orders": Schema.declare(
            ("customer_id", "text", False),
            ("amount", "int64", False),
            ("note", "text", True),
        ),
        "node_a": Schema.declare(("partner", "text", True)),
        "node_b": Schema.declare(("partner", "text", True)),
    }


def group_manifest() -> RelationManifest:
    return RelationManifest.declare(
        1,
        [
            ForeignKey("orders", "customer_id", "customers", False),
            ForeignKey("node_a", "partner", "node_b", True),
            ForeignKey("node_b", "partner", "node_a", True),
        ],
    )


def base_group(
    customers: int, orders: int, links: int, *, seed: int = 7
) -> dict[str, list[tuple[str, dict[str, Any]]]]:
    """The older batch: FK-valid by construction."""

    rng = random.Random(seed)
    cust_rows = []
    for i in range(customers):
        cust_rows.append(
            (
                f"c-{i:06d}",
                {
                    "name": f"customer-{i:06d} {'ü' * (i % 3)}",
                    "tier": None if i % 7 == 0 else rng.randint(1, 5),
                },
            )
        )
    order_rows = []
    for i in range(orders):
        order_rows.append(
            (
                f"o-{i:06d}",
                {
                    "customer_id": f"c-{(i * 7 + 3) % customers:06d}",
                    "amount": (i * 13) % 1000 - 500,
                    "note": None if i % 5 == 0 else f"note-{i:06d}",
                },
            )
        )
    # Nullable cycles: a-i <-> b-i for two of three indices, NULL otherwise.
    a_rows = [
        (f"a-{i:04d}", {"partner": f"b-{i:04d}" if i % 3 else None}) for i in range(links)
    ]
    b_rows = [
        (f"b-{i:04d}", {"partner": f"a-{i:04d}" if i % 3 else None}) for i in range(links)
    ]
    return {
        "customers": cust_rows,
        "orders": order_rows,
        "node_a": a_rows,
        "node_b": b_rows,
    }


def variant_group(
    base: dict[str, list[tuple[str, dict[str, Any]]]], *, seed: int = 7
) -> tuple[dict[str, list[tuple[str, dict[str, Any]]]], dict[str, Any]]:
    """The newer batch: all change classes applied at once, still FK-valid."""

    customers = [tuple(row) for row in base["customers"]]
    orders = [tuple(row) for row in base["orders"]]
    node_a = [tuple(row) for row in base["node_a"]]
    node_b = [tuple(row) for row in base["node_b"]]
    if len(customers) < 32 or len(orders) < 64 or len(node_a) < 8:
        raise ValueError("demo group sizes are too small for the variant changes")

    cust = dict(customers)
    ords = dict(orders)
    na = dict(node_a)
    nb = dict(node_b)

    # 1. New parent referenced by a new child in the same batch.
    cust["c-new-0001"] = {"name": "customer-new-0001", "tier": 4}
    ords["o-new-0001"] = {"customer_id": "c-new-0001", "amount": 777, "note": "new"}

    # 2. Same-batch deletion of a parent and all of its children.
    doomed_parent = customers[10][0]
    del cust[doomed_parent]
    doomed_orders = [rid for rid, row in orders if row["customer_id"] == doomed_parent]
    for rid in doomed_orders:
        del ords[rid]

    # 3. Plain value changes.
    changed_cust = customers[5][0]
    cust[changed_cust] = {**cust[changed_cust], "name": cust[changed_cust]["name"] + "/v2"}
    changed_order = orders[7][0]
    ords[changed_order] = {**ords[changed_order], "amount": ords[changed_order]["amount"] + 1}

    # 4. A child re-pointed to another existing parent.
    repointed = orders[20][0]
    target_parent = customers[21][0]
    ords[repointed] = {**ords[repointed], "customer_id": target_parent}

    # 5. Cycle changes through the nullable relation: a-0002 now partners with
    #    b-0003, and a brand-new a/b pair references each other.
    na["a-0002"] = {"partner": "b-0003"}
    na["a-new"] = {"partner": "b-new"}
    nb["b-new"] = {"partner": "a-new"}

    tables = {
        "customers": sorted(cust.items()),
        "orders": sorted(ords.items()),
        "node_a": sorted(na.items()),
        "node_b": sorted(nb.items()),
    }
    summary = {
        "added_parent": "c-new-0001",
        "added_child_of_new_parent": "o-new-0001",
        "deleted_parent": doomed_parent,
        "deleted_children": sorted(doomed_orders),
        "changed_customer": changed_cust,
        "changed_order": changed_order,
        "repointed_child": repointed,
        "repointed_to": target_parent,
        "cycle_updates": ["a-0002", "a-new", "b-new"],
    }
    return tables, summary
