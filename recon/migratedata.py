"""Deterministic versioned demo data for the schema-migration flow.

Three schema versions of one dataset, exercising every supported field rule:

* **v1**: ``customers(name, tier as decimal text?)``,
  ``orders(customer_id, amount_text, state, legacy_note)``.
* **v2**: ``customers.tier`` becomes ``int64`` (strict text->int64);
  ``orders.amount_text`` is renamed to ``amount_cents`` and converted
  (text->int64); ``orders.legacy_note`` is dropped; ``orders.channel`` is
  added with the constant default ``"web"``.
* **v3**: ``customers.name`` is renamed to ``full_name``; ``customers.tier``
  becomes decimal text ``tier_code`` (int64->text); ``customers.segment`` is
  added with the constant default ``"standard"``; the foreign-key column
  ``orders.customer_id`` is renamed to ``cust_id`` (the v3 manifest follows
  the rename); ``orders.state`` is renamed to ``status`` and its codes are
  rewritten through an explicit one-to-one enum map.

The left end is generated at v1 (migration path v1->v2->v3), the right end
at v2 (path v2->v3): different-length paths that converge on v3.  The
generator is seeded and independent of the migration and reconciliation
code; expectations in tests come from an independent reference conversion.
"""

from __future__ import annotations

import random
from typing import Any

from .dataset import ForeignKey, RelationManifest
from .encoding import Schema
from .migrate import (
    CONVERT_INT64_TO_TEXT,
    CONVERT_TEXT_TO_INT64,
    FieldRule,
    MigrationCatalog,
    StepDef,
    VersionDef,
)

DATASET_ID = "dataset-migration-demo"
V1, V2, V3 = "v1", "v2", "v3"
BATCH_LEFT = "batch-0001"
BATCH_RIGHT = "batch-0002"

ENUM_V2_TO_V3 = {"N": "NEW", "P": "PAID", "C": "CANCELLED"}


def version_defs() -> dict[str, VersionDef]:
    return {
        V1: VersionDef(
            V1,
            {
                "customers": Schema.declare(
                    ("name", "text", False),
                    ("tier", "text", True),
                ),
                "orders": Schema.declare(
                    ("customer_id", "text", False),
                    ("amount_text", "text", False),
                    ("state", "text", False),
                    ("legacy_note", "text", True),
                ),
            },
            RelationManifest.declare(
                1, [ForeignKey("orders", "customer_id", "customers", False)]
            ),
        ),
        V2: VersionDef(
            V2,
            {
                "customers": Schema.declare(
                    ("name", "text", False),
                    ("tier", "int64", True),
                ),
                "orders": Schema.declare(
                    ("customer_id", "text", False),
                    ("amount_cents", "int64", False),
                    ("state", "text", False),
                    ("channel", "text", False),
                ),
            },
            RelationManifest.declare(
                2, [ForeignKey("orders", "customer_id", "customers", False)]
            ),
        ),
        V3: VersionDef(
            V3,
            {
                "customers": Schema.declare(
                    ("full_name", "text", False),
                    ("tier_code", "text", True),
                    ("segment", "text", False),
                ),
                "orders": Schema.declare(
                    ("cust_id", "text", False),
                    ("amount_cents", "int64", False),
                    ("status", "text", False),
                    ("channel", "text", False),
                ),
            },
            RelationManifest.declare(
                3, [ForeignKey("orders", "cust_id", "customers", False)]
            ),
        ),
    }


def demo_catalog() -> MigrationCatalog:
    versions = version_defs()
    return MigrationCatalog.declare(
        DATASET_ID,
        versions,
        [
            StepDef(
                V1,
                V2,
                {
                    "customers": (
                        FieldRule.copy("name"),
                        FieldRule.convert("tier", "tier", CONVERT_TEXT_TO_INT64),
                    ),
                    "orders": (
                        FieldRule.copy("customer_id"),
                        FieldRule.convert("amount_cents", "amount_text", CONVERT_TEXT_TO_INT64),
                        FieldRule.copy("state"),
                        FieldRule.default("channel", "web"),
                        # legacy_note is dropped: no rule references it.
                    ),
                },
            ),
            StepDef(
                V2,
                V3,
                {
                    "customers": (
                        FieldRule.rename("full_name", "name"),
                        FieldRule.convert("tier_code", "tier", CONVERT_INT64_TO_TEXT),
                        FieldRule.default("segment", "standard"),
                    ),
                    "orders": (
                        FieldRule.rename("cust_id", "customer_id"),
                        FieldRule.copy("amount_cents"),
                        FieldRule.enum("status", "state", ENUM_V2_TO_V3),
                        FieldRule.copy("channel"),
                    ),
                },
            ),
        ],
    )


def left_rows_v1(
    customers: int, orders: int, *, seed: int = 11
) -> dict[str, list[tuple[str, dict[str, Any]]]]:
    """The left end's older batch at schema v1.  FK-valid by construction."""

    rng = random.Random(seed)
    cust_rows = []
    for i in range(customers):
        cust_rows.append(
            (
                f"c-{i:06d}",
                {
                    "name": f"customer-{i:06d} {'ü' * (i % 3)}",
                    "tier": None if i % 7 == 0 else str(rng.randint(1, 5)),
                },
            )
        )
    states = ["N", "P", "C"]
    order_rows = []
    for i in range(orders):
        order_rows.append(
            (
                f"o-{i:06d}",
                {
                    "customer_id": f"c-{(i * 7 + 3) % customers:06d}",
                    "amount_text": str((i * 13) % 1000 - 500),
                    "state": states[i % 3],
                    "legacy_note": None if i % 5 == 0 else f"legacy-{i:06d}",
                },
            )
        )
    return {"customers": cust_rows, "orders": order_rows}


def right_rows_v2(
    left_v1: dict[str, list[tuple[str, dict[str, Any]]]], *, seed: int = 11
) -> dict[str, list[tuple[str, dict[str, Any]]]]:
    """The right end's newer batch, collected directly at schema v2.

    Derived from the same business entities as the left batch with the usual
    change classes (inserted parent + child, same-batch deletions, value
    changes, a re-pointed child), so that after both ends are migrated to v3
    the groups differ exactly by those business changes and the final state
    stays FK-valid.
    """

    customers = dict(left_v1["customers"])
    orders = dict(left_v1["orders"])
    if len(customers) < 32 or len(orders) < 64:
        raise ValueError("demo sizes are too small for the variant changes")

    # Business changes on the v1-shaped entities first ...
    cust = dict(customers)
    ords = dict(orders)

    cust["c-new-0001"] = {"name": "customer-new-0001", "tier": "4"}
    ords["o-new-0001"] = {
        "customer_id": "c-new-0001",
        "amount_text": "777",
        "state": "P",
        "legacy_note": "new",
    }
    doomed_parent = sorted(cust)[10]
    del cust[doomed_parent]
    for rid in [rid for rid, row in ords.items() if row["customer_id"] == doomed_parent]:
        del ords[rid]
    changed_cust = sorted(cust)[5]
    cust[changed_cust] = {**cust[changed_cust], "name": cust[changed_cust]["name"] + "/v2"}
    changed_order = sorted(ords)[7]
    ords[changed_order] = {
        **ords[changed_order],
        "amount_text": str(int(ords[changed_order]["amount_text"]) + 1),
    }
    repointed = sorted(ords)[20]
    ords[repointed] = {**ords[repointed], "customer_id": sorted(cust)[21]}

    # ... then expressed in the v2 layout the right site actually collects:
    # tier as int64, amount_cents as int64, channel as a real collected value
    # (mostly "web", matching the v1->v2 default, with a few real deviations).
    cust_rows = []
    for row_id in sorted(cust):
        row = cust[row_id]
        cust_rows.append(
            (
                row_id,
                {
                    "name": row["name"],
                    "tier": None if row["tier"] is None else int(row["tier"]),
                },
            )
        )
    order_rows = []
    for index, row_id in enumerate(sorted(ords)):
        row = ords[row_id]
        channel = "app" if index % 17 == 0 else ("store" if index % 29 == 0 else "web")
        order_rows.append(
            (
                row_id,
                {
                    "customer_id": row["customer_id"],
                    "amount_cents": int(row["amount_text"]),
                    "state": row["state"],
                    "channel": channel,
                },
            )
        )
    return {"customers": cust_rows, "orders": order_rows}
