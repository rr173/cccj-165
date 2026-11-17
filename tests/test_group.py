"""Relational dataset group reconciliation tests.

Expectations are derived independently from the two *input* groups: per-table
differences come from the row-by-row oracle (:func:`recon.oracle.compare_inputs`),
and foreign-key expectations come from a scan written here in the test file.
Neither the product's difference engine nor its constraint check is reused to
compute what "should" happen.
"""

from __future__ import annotations

import os
import sqlite3
import stat
from dataclasses import replace
from pathlib import Path

import pytest

from recon.dataset import (
    ForeignKey,
    GroupError,
    GroupExpectation,
    GroupStore,
    RelationManifest,
    check_compatible,
    create_group,
)
from recon.encoding import Schema, decode_record, encode_record, record_row_id_bytes
from recon.grouprecon import (
    reconcile_and_publish,
    reconcile_group,
    rebuild_records,
)
from recon.mirror import Mirror
from recon.oracle import compare_inputs
from recon.store import DatasetTag, create_snapshot
from recon.summary import SummaryParams

SCHEMAS = {
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

MANIFEST = RelationManifest.declare(
    1,
    [
        ForeignKey("orders", "customer_id", "customers", False),
        ForeignKey("node_a", "partner", "node_b", True),
        ForeignKey("node_b", "partner", "node_a", True),
    ],
)

DATASET = "ds-001"
BATCH_LEFT = "batch-0001"
BATCH_RIGHT = "batch-0002"

# The older batch (left / current mirror content).  FK-valid.
LEFT = {
    "customers": [
        ("c-001", {"name": "Ada", "tier": 1}),
        ("c-002", {"name": "Bao", "tier": None}),
        ("c-003", {"name": "Cid", "tier": 2}),
        ("c-004", {"name": "Dana", "tier": 3}),
        ("c-005", {"name": "Eli", "tier": 1}),
    ],
    "orders": [
        ("o-001", {"customer_id": "c-001", "amount": 10, "note": None}),
        ("o-002", {"customer_id": "c-002", "amount": 20, "note": "x"}),
        ("o-003", {"customer_id": "c-004", "amount": 30, "note": None}),
        ("o-004", {"customer_id": "c-004", "amount": 40, "note": "y"}),
        ("o-005", {"customer_id": "c-001", "amount": 50, "note": None}),
    ],
    "node_a": [
        ("a-1", {"partner": "b-1"}),
        ("a-2", {"partner": None}),
    ],
    "node_b": [
        ("b-1", {"partner": "a-1"}),
        ("b-2", {"partner": None}),
    ],
}

# The newer batch (right / peer).  Combines, in one batch: an inserted parent
# referenced by an inserted child, a same-batch parent+child deletion, plain
# value changes, a child re-pointed to another parent, and cycle changes
# through the nullable relation.  FK-valid as a final state.
RIGHT = {
    "customers": [
        ("c-001", {"name": "Ada", "tier": 1}),
        ("c-002", {"name": "Bao", "tier": 5}),  # changed value
        ("c-003", {"name": "Cid", "tier": 2}),
        ("c-005", {"name": "Eli", "tier": 1}),
        ("c-006", {"name": "Faye", "tier": None}),  # new parent
    ],
    "orders": [
        ("o-001", {"customer_id": "c-001", "amount": 10, "note": None}),
        ("o-002", {"customer_id": "c-002", "amount": 99, "note": "x"}),  # changed value
        ("o-005", {"customer_id": "c-003", "amount": 50, "note": None}),  # re-pointed
        ("o-006", {"customer_id": "c-006", "amount": 60, "note": None}),  # new child
    ],
    "node_a": [
        ("a-1", {"partner": "b-1"}),
        ("a-2", {"partner": "b-2"}),  # changed: joins a new 2-cycle
    ],
    "node_b": [
        ("b-1", {"partner": "a-1"}),
        ("b-2", {"partner": "a-2"}),  # changed: completes the new 2-cycle
    ],
}


# ---------------------------------------------------------------------------
# Independent helpers (no product diff / constraint code)


def _read_table_rows(group_dir: Path, table: str) -> dict[str, dict]:
    """Decode one table straight out of a group directory's SQLite file."""

    path = group_dir / "tables" / f"{table}.sqlite"
    conn = sqlite3.connect(f"file:{path.resolve().as_posix()}?mode=ro", uri=True)
    try:
        records = [bytes(r) for (r,) in conn.execute("SELECT record FROM rows ORDER BY row_id")]
    finally:
        conn.close()
    return dict(decode_record(SCHEMAS[table], record) for record in records)


def _read_all(group_dir: Path) -> dict[str, dict[str, dict]]:
    return {name: _read_table_rows(group_dir, name) for name in SCHEMAS if (group_dir / "tables" / f"{name}.sqlite").exists()}


def _independent_fk_scan(tables_rows, foreign_keys) -> list[tuple]:
    """Dangling-reference scan written here, over fully decoded rows."""

    violations = []
    for fk in foreign_keys:
        parent_ids = set(tables_rows[fk.parent_table])
        for row_id, values in tables_rows[fk.child_table].items():
            value = values[fk.child_column]
            if value is None:
                continue
            if value not in parent_ids:
                violations.append((fk.child_table, row_id, fk.child_column, value))
    return violations


def _as_maps(tables) -> dict[str, dict[str, dict]]:
    return {name: dict(rows) for name, rows in tables.items()}


def _assert_diffs_match_oracle(table: str, left_rows, right_rows, differences) -> None:
    """The reported per-table difference set must equal the independent
    row-by-row comparison, row id by row id, canonical byte by canonical byte."""

    schema = SCHEMAS[table]
    exp = compare_inputs(left_rows, right_rows, schema.field_names())
    got_left = {record_row_id_bytes(r).decode("utf-8"): r for r in differences.only_left}
    got_right = {record_row_id_bytes(r).decode("utf-8"): r for r in differences.only_right}
    got_changed = {
        record_row_id_bytes(a).decode("utf-8"): (a, b)
        for a, b in zip(differences.changed_left, differences.changed_right)
    }
    assert set(got_left) == set(exp.only_left), f"{table}: only_left ids diverge"
    assert set(got_right) == set(exp.only_right), f"{table}: only_right ids diverge"
    assert set(got_changed) == set(exp.changed), f"{table}: changed ids diverge"
    left_by_id = dict(left_rows)
    right_by_id = dict(right_rows)
    for row_id in exp.only_left:
        assert got_left[row_id] == encode_record(schema, row_id, left_by_id[row_id])
    for row_id in exp.only_right:
        assert got_right[row_id] == encode_record(schema, row_id, right_by_id[row_id])
    for row_id in exp.changed:
        mine, theirs = got_changed[row_id]
        assert mine == encode_record(schema, row_id, left_by_id[row_id])
        assert theirs == encode_record(schema, row_id, right_by_id[row_id])


# ---------------------------------------------------------------------------
# Builders


def _make_group(tmp_path: Path, dirname: str, dataset: str, batch: str,
                tables, manifest: RelationManifest, schemas=None) -> Path:
    schemas = schemas or SCHEMAS
    group_dir = tmp_path / dirname
    create_group(
        group_dir,
        dataset,
        batch,
        {name: (schemas[name], tables[name]) for name in tables},
        manifest,
    )
    return group_dir


def _make_groups(tmp_path: Path, *, left=LEFT, right=RIGHT, manifest=MANIFEST,
                 left_batch=BATCH_LEFT, right_batch=BATCH_RIGHT,
                 dataset=DATASET, right_dataset=None, right_manifest=None,
                 right_schemas=None):
    left_dir = _make_group(tmp_path, "left-group", dataset, left_batch, left, manifest)
    right_dir = _make_group(
        tmp_path,
        "right-group",
        right_dataset or dataset,
        right_batch,
        right,
        right_manifest or manifest,
        schemas=right_schemas,
    )
    return left_dir, right_dir


def _init_mirror(tmp_path: Path, left_dir: Path) -> Mirror:
    with GroupStore.open(left_dir) as source:
        return Mirror.initialize(tmp_path / "mirror", source)


# ---------------------------------------------------------------------------
# Happy path: simultaneous adds, deletes, value changes, and FK changes


def test_group_reconcile_publish_and_report(tmp_path):
    left_dir, right_dir = _make_groups(tmp_path)
    mirror = _init_mirror(tmp_path, left_dir)
    pointer_before = (mirror.path / "CURRENT").read_bytes()

    result = reconcile_and_publish(left_dir, right_dir, mirror.path)
    assert result.complete, result.reason

    # Per-table differences equal the independent oracle, byte for byte.
    for name in SCHEMAS:
        _assert_diffs_match_oracle(name, LEFT[name], RIGHT[name], result.tables[name].differences)

    # Group-level result identity binds both full dataset identities.
    with GroupStore.open(left_dir) as left_group, GroupStore.open(right_dir) as right_group:
        assert result.left_group_id == left_group.group_id
        assert result.right_group_id == right_group.group_id
        right_commits = {
            name: store.identity.commitment for name, store in right_group.tables.items()
        }
    assert result.left_batch == BATCH_LEFT and result.right_batch == BATCH_RIGHT
    assert result.dataset_id == DATASET
    assert result.manifest_version == 1
    assert len(result.result_id) == 64

    # Reporting: per-table input/output rows and transferred bytes.
    assert result.wire_bytes > 0
    assert result.wire_bytes == sum(rep.wire_bytes for rep in result.tables.values())
    assert result.wire_bytes == result.sent_bytes + result.received_bytes
    for name, rep in result.tables.items():
        assert rep.left_rows == len(LEFT[name])
        assert rep.right_rows == len(RIGHT[name])
        assert rep.wire_bytes == rep.sent_bytes + rep.received_bytes > 0
    assert result.publish is not None and result.publish.published
    assert result.publish.staged_rows == {name: len(RIGHT[name]) for name in SCHEMAS}

    # Publish: exactly one new generation, atomically visible.
    assert result.generation_before == 1
    assert result.generation_after == 2
    assert mirror.current_generation() == 2
    assert (mirror.path / "CURRENT").read_bytes() != pointer_before

    # The published mirror content equals the right input group, decoded
    # independently from the generation's SQLite files.
    mirror_rows = _read_all(mirror.generation_dir(2))
    assert mirror_rows == _as_maps(RIGHT)
    # The final state is FK-clean under the independent scan, including the
    # cycles formed through the nullable relation.
    assert _independent_fk_scan(mirror_rows, MANIFEST.foreign_keys) == []
    assert mirror_rows["node_a"]["a-1"]["partner"] == "b-1"
    assert mirror_rows["node_b"]["b-1"]["partner"] == "a-1"
    assert mirror_rows["node_a"]["a-2"]["partner"] == "b-2"
    assert mirror_rows["node_b"]["b-2"]["partner"] == "a-2"

    # The mirror manifest records batch identity, per-table commitments,
    # relationship definition version, and the publish generation.
    doc = mirror.publish_manifest()
    assert doc["dataset_id"] == DATASET
    assert doc["batch_id"] == BATCH_RIGHT
    assert doc["generation"] == 2
    assert doc["previous_generation"] == 1
    assert doc["manifest_version"] == MANIFEST.version
    assert doc["manifest_hash"] == MANIFEST.content_hash().hex()
    assert doc["result_id"] == result.result_id
    assert doc["group_id"] == result.right_group_id
    assert doc["source"] == {
        "left_group_id": result.left_group_id,
        "right_group_id": result.right_group_id,
    }
    for name in SCHEMAS:
        assert doc["tables"][name]["rows"] == len(RIGHT[name])
        assert doc["tables"][name]["commitment"] == right_commits[name].hex()

    # The published generation is itself a valid group carrying the new batch
    # in every table file, with the peer's group identity.
    with GroupStore.open(mirror.generation_dir(2)) as published:
        assert published.dataset_id == DATASET
        assert published.batch_id == BATCH_RIGHT
        assert published.group_id == result.right_group_id
        for name, store in published.tables.items():
            tag = store.dataset_tag()
            assert tag is not None and tag.batch_id == BATCH_RIGHT
            assert tag.table_name == name

    # The old generation is still on disk, readable, and unchanged.
    assert _read_all(mirror.generation_dir(1)) == _as_maps(LEFT)
    assert _independent_fk_scan(_read_all(mirror.generation_dir(1)), MANIFEST.foreign_keys) == []

    # The input groups were not modified: files are still read-only and the
    # group identities are stable across a second reconciliation.
    for group_dir in (left_dir, right_dir):
        for table_file in (group_dir / "tables").glob("*.sqlite"):
            assert stat.S_IMODE(table_file.stat().st_mode) == 0o444
    again = reconcile_group(left_dir, right_dir)
    assert again.complete
    assert again.result_id == result.result_id
    assert again.publish is None  # reconcile_group never touches a mirror


def test_nullable_cycle_added_in_one_batch(tmp_path):
    """A cycle created by the same batch must publish: the FK check judges the
    final state, never the transient order of table processing."""

    schemas = {"node_a": SCHEMAS["node_a"], "node_b": SCHEMAS["node_b"]}
    manifest = RelationManifest.declare(
        1,
        [
            ForeignKey("node_a", "partner", "node_b", True),
            ForeignKey("node_b", "partner", "node_a", True),
        ],
    )
    left = {
        "node_a": [("a-1", {"partner": None})],
        "node_b": [("b-1", {"partner": None})],
    }
    right = {
        "node_a": [("a-1", {"partner": "b-1"})],
        "node_b": [("b-1", {"partner": "a-1"})],
    }
    left_dir = _make_group(tmp_path, "left-group", DATASET, BATCH_LEFT, left, manifest, schemas)
    right_dir = _make_group(tmp_path, "right-group", DATASET, BATCH_RIGHT, right, manifest, schemas)
    mirror = _init_mirror(tmp_path, left_dir)

    result = reconcile_and_publish(left_dir, right_dir, mirror.path)
    assert result.complete, result.reason
    assert result.generation_after == 2
    rows = _read_all(mirror.generation_dir(2))
    assert rows["node_a"]["a-1"]["partner"] == "b-1"
    assert rows["node_b"]["b-1"]["partner"] == "a-1"
    assert _independent_fk_scan(rows, manifest.foreign_keys) == []


def test_same_batch_parent_child_delete_and_insert(tmp_path):
    """Deletion of a parent together with its children, and insertion of a
    parent together with a referencing child, are judged on the final state."""

    left_dir, right_dir = _make_groups(tmp_path)
    mirror = _init_mirror(tmp_path, left_dir)
    result = reconcile_and_publish(left_dir, right_dir, mirror.path)
    assert result.complete, result.reason
    orders = result.tables["orders"].differences
    customers = result.tables["customers"].differences
    # Same-batch deletion: parent c-004 and its children o-003/o-004 leave together.
    assert {record_row_id_bytes(r).decode() for r in customers.only_left} == {"c-004"}
    assert {record_row_id_bytes(r).decode() for r in orders.only_left} == {"o-003", "o-004"}
    # Same-batch insertion: parent c-006 and its child o-006 arrive together.
    assert {record_row_id_bytes(r).decode() for r in customers.only_right} == {"c-006"}
    assert {record_row_id_bytes(r).decode() for r in orders.only_right} == {"o-006"}
    rows = _read_all(mirror.generation_dir(2))
    assert rows["orders"]["o-006"]["customer_id"] == "c-006"
    assert "c-006" in rows["customers"]
    assert "c-004" not in rows["customers"]
    assert _independent_fk_scan(rows, MANIFEST.foreign_keys) == []


# ---------------------------------------------------------------------------
# Compatibility failures: no valid reconciliation may be formed


def test_mixed_batch_inside_one_group_rejected(tmp_path):
    left_dir, right_dir = _make_groups(tmp_path)
    # Swap one table file of the right group for one collected in another batch.
    stray = tmp_path / "stray.sqlite"
    create_snapshot(
        stray,
        SCHEMAS["orders"],
        RIGHT["orders"],
        created_from="stray-batch",
        dataset=DatasetTag(DATASET, "batch-0003", "orders"),
    )
    os.replace(stray, right_dir / "tables" / "orders.sqlite")
    with pytest.raises(GroupError, match="batch_inconsistent") as excinfo:
        reconcile_group(left_dir, right_dir)
    assert excinfo.value.code == "batch_inconsistent"


def test_missing_table_rejected(tmp_path):
    # Right group lacks the orders table (and its FK declarations).
    left_dir = _make_group(tmp_path, "left-group", DATASET, BATCH_LEFT, LEFT, MANIFEST)
    right_tables = {name: rows for name, rows in RIGHT.items() if name != "orders"}
    manifest_no_orders = RelationManifest.declare(
        1,
        [
            fk
            for fk in MANIFEST.foreign_keys
            if fk.child_table != "orders" and fk.parent_table != "orders"
        ],
    )
    right_dir = _make_group(
        tmp_path, "right-group", DATASET, BATCH_RIGHT, right_tables, manifest_no_orders
    )
    with pytest.raises(GroupError, match="table_set_mismatch") as excinfo:
        reconcile_group(left_dir, right_dir)
    assert excinfo.value.code == "table_set_mismatch"


def test_manifest_referencing_missing_table_rejected(tmp_path):
    bad_manifest = RelationManifest.declare(
        1,
        list(MANIFEST.foreign_keys)
        + [ForeignKey("orders", "note", "ghost_table", True)],
    )
    group_dir = _make_group(tmp_path, "broken", DATASET, BATCH_LEFT, LEFT, bad_manifest)
    with pytest.raises(GroupError, match="manifest_unknown_table"):
        GroupStore.open(group_dir)


def test_fk_type_mismatch_rejected(tmp_path):
    # FK declared on an int64 column: a row_id reference must be text.
    bad_manifest = RelationManifest.declare(
        1, [ForeignKey("orders", "amount", "customers", False)]
    )
    group_dir = _make_group(tmp_path, "broken", DATASET, BATCH_LEFT, LEFT, bad_manifest)
    with pytest.raises(GroupError, match="fk_type_mismatch"):
        GroupStore.open(group_dir)


def test_fk_nullability_mismatch_rejected(tmp_path):
    # customer_id is non-nullable; the FK declaration says nullable.
    bad_manifest = RelationManifest.declare(
        1, [ForeignKey("orders", "customer_id", "customers", True)]
    )
    group_dir = _make_group(tmp_path, "broken", DATASET, BATCH_LEFT, LEFT, bad_manifest)
    with pytest.raises(GroupError, match="fk_nullability_mismatch"):
        GroupStore.open(group_dir)


def test_manifest_mismatch_between_ends_rejected(tmp_path):
    other_manifest = RelationManifest.declare(2, MANIFEST.foreign_keys)  # bumped version
    left_dir, right_dir = _make_groups(tmp_path, right_manifest=other_manifest)
    with pytest.raises(GroupError, match="manifest_mismatch") as excinfo:
        reconcile_group(left_dir, right_dir)
    assert excinfo.value.code == "manifest_mismatch"


def test_dataset_id_mismatch_rejected(tmp_path):
    left_dir, right_dir = _make_groups(tmp_path, right_dataset="ds-other")
    with pytest.raises(GroupError, match="dataset_mismatch"):
        reconcile_group(left_dir, right_dir)


def test_schema_type_change_rejected(tmp_path):
    # The right group changed customers.tier from int64 to text.
    alt_schemas = dict(SCHEMAS)
    alt_schemas["customers"] = Schema.declare(
        ("name", "text", False), ("tier", "text", True)
    )
    right = dict(RIGHT)
    right["customers"] = [
        (row_id, {"name": v["name"], "tier": None if v["tier"] is None else str(v["tier"])})
        for row_id, v in RIGHT["customers"]
    ]
    left_dir, right_dir = _make_groups(
        tmp_path, right=right, right_schemas=alt_schemas
    )
    with pytest.raises(GroupError, match="schema_mismatch") as excinfo:
        reconcile_group(left_dir, right_dir)
    assert excinfo.value.code == "schema_mismatch"


# ---------------------------------------------------------------------------
# Publish-time failures: the old mirror must survive untouched


def test_dangling_reference_blocks_publish(tmp_path):
    # The peer's final state contains an order whose customer does not exist.
    right = {name: list(rows) for name, rows in RIGHT.items()}
    right["orders"] = right["orders"] + [
        ("o-009", {"customer_id": "c-ghost", "amount": 1, "note": None})
    ]
    left_dir, right_dir = _make_groups(tmp_path, right=right)
    mirror = _init_mirror(tmp_path, left_dir)
    pointer_before = (mirror.path / "CURRENT").read_bytes()

    result = reconcile_and_publish(left_dir, right_dir, mirror.path)

    # The transfer itself completed (every table commitment matched), but the
    # final-state FK check refused to publish the dangling reference.
    assert all(rep.complete for rep in result.tables.values())
    assert not result.complete
    assert result.reason.startswith("publish_failed:fk_violation")
    assert result.publish is not None and not result.publish.published
    assert result.publish.fk_violations == [
        {
            "child_table": "orders",
            "child_row_id": "o-009",
            "column": "customer_id",
            "value": "c-ghost",
            "parent_table": "customers",
        }
    ]
    # Nothing was published: pointer, generation, and content are unchanged.
    assert result.generation_before == 1
    assert result.generation_after == 1
    assert mirror.current_generation() == 1
    assert (mirror.path / "CURRENT").read_bytes() == pointer_before
    assert _read_all(mirror.generation_dir(1)) == _as_maps(LEFT)
    # No staged leftovers remain.
    assert sorted(p.name for p in (mirror.path / "generations").iterdir()) == ["000001"]
    # The old mirror still serves readers through a fresh open.
    with mirror.open_current() as current:
        assert current.batch_id == BATCH_LEFT
        assert current.group_id == result.left_group_id


def test_commitment_mismatch_blocks_publish(tmp_path):
    left_dir, right_dir = _make_groups(tmp_path)
    mirror = _init_mirror(tmp_path, left_dir)
    result = reconcile_group(left_dir, right_dir)
    assert result.complete

    with GroupStore.open(left_dir) as left, GroupStore.open(right_dir) as right:
        expectation = GroupExpectation.from_group(right)
        # Simulate a peer whose advertised table commitment does not match the
        # content the differences reconstruct: the group-level re-check must
        # refuse to publish, even though the per-table sessions succeeded.
        tampered = dict(expectation.table_commitments)
        tampered["orders"] = b"\x00" * 32
        expectation = replace(expectation, table_commitments=tampered)
        rebuilt = {
            name: rebuild_records(left.tables[name], result.tables[name].differences)
            for name in left.table_names()
        }
        report = mirror.stage_publish(
            rebuilt,
            expectation,
            result_id=result.result_id,
            left_group_id=result.left_group_id,
            right_group_id=result.right_group_id,
        )

    assert not report.published
    assert report.reason == "commitment_mismatch:orders"
    assert report.generation_before == 1
    assert report.generation_after == 1
    assert mirror.current_generation() == 1
    assert _read_all(mirror.generation_dir(1)) == _as_maps(LEFT)
    assert sorted(p.name for p in (mirror.path / "generations").iterdir()) == ["000001"]


def test_failed_publish_leaves_old_mirror_fully_readable(tmp_path):
    """After any failed application the old generation remains intact and
    readable, and the input snapshots are untouched."""

    right = {name: list(rows) for name, rows in RIGHT.items()}
    right["node_a"] = right["node_a"] + [("a-9", {"partner": "b-ghost"})]
    left_dir, right_dir = _make_groups(tmp_path, right=right)
    mirror = _init_mirror(tmp_path, left_dir)

    result = reconcile_and_publish(left_dir, right_dir, mirror.path)
    assert not result.complete
    assert result.publish is not None and not result.publish.published

    # A brand-new reader sees exactly the old content.
    fresh = Mirror(tmp_path / "mirror")
    assert fresh.current_generation() == 1
    with fresh.open_current() as current:
        assert current.batch_id == BATCH_LEFT
        for name in SCHEMAS:
            decoded = {}
            for record in current.tables[name].iter_records():
                row_id, values = decode_record(SCHEMAS[name], record)
                decoded[row_id] = values
            assert decoded == dict(LEFT[name])
    old_doc = fresh.publish_manifest()
    assert old_doc["batch_id"] == BATCH_LEFT
    assert old_doc["generation"] == 1
    # Input groups unchanged: same identities as before the run.
    with GroupStore.open(left_dir) as left_group:
        assert left_group.group_id == result.left_group_id
        assert left_group.batch_id == BATCH_LEFT


def test_incomplete_table_means_incomplete_group(tmp_path):
    """If any one table session does not complete, the group result must not
    report completion and no publish may be attempted."""

    import recon.grouprecon as grouprecon

    left_dir, right_dir = _make_groups(tmp_path)
    mirror = _init_mirror(tmp_path, left_dir)
    real_reconcile = grouprecon.reconcile_group
    try:
        original = real_reconcile(left_dir, right_dir)
        assert original.complete
        # Force one table into an incomplete state, as a failed per-table
        # session would report it.
        broken = dict(original.tables)
        rep = broken["orders"]
        broken["orders"] = replace(rep, complete=False, reason="protocol_error: simulated")
        forced = replace(
            original,
            complete=False,
            reason="incomplete tables: orders",
            tables=broken,
        )
        grouprecon.reconcile_group = lambda *a, **kw: forced
        result = grouprecon.reconcile_and_publish(left_dir, right_dir, mirror.path)
    finally:
        grouprecon.reconcile_group = real_reconcile
    assert not result.complete
    assert result.publish is not None
    assert not result.publish.published
    assert result.publish.reason.startswith("not_attempted:")
    assert mirror.current_generation() == 1
    assert _read_all(mirror.generation_dir(1)) == _as_maps(LEFT)


def test_identical_groups_complete_and_publish_same_identity(tmp_path):
    """Reconciling a group with an identical copy still completes over the
    protocol and publishes a generation with the peer's (equal) identity."""

    left_dir, right_dir = _make_groups(tmp_path, right=LEFT, right_batch=BATCH_LEFT)
    mirror = _init_mirror(tmp_path, left_dir)
    result = reconcile_and_publish(left_dir, right_dir, mirror.path)
    assert result.complete, result.reason
    for name, rep in result.tables.items():
        assert rep.counts() == {"only_left": 0, "only_right": 0, "changed": 0}
    assert result.left_group_id == result.right_group_id
    assert result.generation_after == 2
    # The published generation carries the same content identity as both inputs.
    assert result.publish is not None and result.publish.group_id == result.left_group_id
    assert _read_all(mirror.generation_dir(2)) == _as_maps(LEFT)
