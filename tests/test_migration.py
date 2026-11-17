"""Schema-migration reconciliation tests.

Expectations are derived independently of the product's migration engine:
a reference converter written in this file reads the catalog *wire* document
and the raw input rows to compute the expected target rows, and an
independent foreign-key scan checks the final mirror state.  The product's
own counters never prove themselves correct.

The pre-existing single-table and same-schema group suites
(``test_protocol.py``, ``test_group.py``, ...) are the regression checks for
the non-migrating commands and are unaffected by this feature.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
import stat
from pathlib import Path

import pytest

from recon import migratedata
from recon.dataset import GroupStore, RelationManifest, create_group
from recon.encoding import Schema, decode_record, encode_record, record_row_id_bytes
from recon.migrate import (
    CONVERT_TEXT_TO_INT64,
    FieldRule,
    MigrationCatalog,
    MigrationConversionError,
    MigrationPlanError,
    StepDef,
    VersionDef,
    migrate_group,
    plan_migrations,
    reconcile_migrated_and_publish,
)
from recon.mirror import Mirror
from recon.oracle import compare_inputs
from recon.store import parse_schema_wire

TARGET = "v3"
DATASET = migratedata.DATASET_ID


# ---------------------------------------------------------------------------
# Independent reference logic (shares no code with recon.migrate)


def _reference_migrate(catalog_wire: dict, rows_by_table: dict, source: str, target: str):
    """Compute expected target rows straight from the catalog wire document."""

    steps = {}
    for step in catalog_wire["steps"]:
        steps.setdefault(step["from"], []).append(step)
    path = []
    version = source
    while version != target:
        outgoing = steps.get(version, [])
        assert len(outgoing) == 1, f"reference walk: expected one step from {version}"
        path.append(outgoing[0])
        version = outgoing[0]["to"]
    current = {table: dict(rows) for table, rows in rows_by_table.items()}
    for step in path:
        nxt = {}
        for table, spec in step["tables"].items():
            out_rows = {}
            for row_id, values in current[table].items():
                new_values = {}
                for rule in spec["fields"]:
                    target_field = rule["target"]
                    if "default" in rule:
                        new_values[target_field] = rule["default"]
                        continue
                    value = values[rule["from"]]
                    if value is None:
                        new_values[target_field] = None
                    elif "enum" in rule:
                        new_values[target_field] = rule["enum"][value]
                    elif rule.get("convert") == "text_to_int64":
                        new_values[target_field] = int(value)
                    elif rule.get("convert") == "int64_to_text":
                        new_values[target_field] = str(value)
                    else:
                        new_values[target_field] = value
                out_rows[row_id] = new_values
            nxt[table] = out_rows
        current = nxt
    return current


def _reference_fk_scan(rows_by_table: dict, manifest_wire: dict) -> list[tuple]:
    """Dangling-reference scan written here, over fully decoded rows."""

    violations = []
    for fk in manifest_wire["foreign_keys"]:
        parent_ids = set(rows_by_table[fk["parent_table"]])
        for row_id, values in rows_by_table[fk["child_table"]].items():
            value = values[fk["child_column"]]
            if value is None:
                continue
            if value not in parent_ids:
                violations.append((fk["child_table"], row_id, fk["child_column"], value))
    return violations


def _read_group_rows(group_dir: Path) -> dict[str, dict[str, dict]]:
    """Decode every table of a group straight out of its SQLite files."""

    meta = json.loads((group_dir / "group.json").read_text(encoding="utf-8"))
    out = {}
    for table in meta["tables"]:
        path = group_dir / "tables" / f"{table}.sqlite"
        conn = sqlite3.connect(f"file:{path.resolve().as_posix()}?mode=ro", uri=True)
        try:
            (schema_blob,) = conn.execute(
                "SELECT schema_wire FROM snapshot_meta WHERE id=1"
            ).fetchone()
            schema = parse_schema_wire(bytes(schema_blob))
            records = [
                bytes(r) for (r,) in conn.execute("SELECT record FROM rows ORDER BY row_id")
            ]
        finally:
            conn.close()
        out[table] = dict(decode_record(schema, record) for record in records)
    return out


def _hash_tree(root: Path) -> dict[str, str]:
    return {
        str(p.relative_to(root)): hashlib.sha256(p.read_bytes()).hexdigest()
        for p in sorted(root.rglob("*"))
        if p.is_file()
    }


def _target_fields(catalog_wire: dict, version: str, table: str) -> list[str]:
    return [field[0] for field in catalog_wire["versions"][version]["tables"][table]]


def _assert_diffs_match_oracle(table, schema, left_rows, right_rows, differences):
    """Byte-for-byte comparison of a reported difference set against the
    independent row-by-row oracle over the reference-migrated rows."""

    expected = compare_inputs(
        sorted(left_rows.items()), sorted(right_rows.items()), schema.field_names()
    )
    got_left = {record_row_id_bytes(r).decode("utf-8"): r for r in differences.only_left}
    got_right = {record_row_id_bytes(r).decode("utf-8"): r for r in differences.only_right}
    got_changed = {
        record_row_id_bytes(a).decode("utf-8"): (a, b)
        for a, b in zip(differences.changed_left, differences.changed_right)
    }
    assert set(got_left) == set(expected.only_left), f"{table}: only_left ids diverge"
    assert set(got_right) == set(expected.only_right), f"{table}: only_right ids diverge"
    assert set(got_changed) == set(expected.changed), f"{table}: changed ids diverge"
    for row_id in expected.only_left:
        assert got_left[row_id] == encode_record(schema, row_id, left_rows[row_id])
    for row_id in expected.only_right:
        assert got_right[row_id] == encode_record(schema, row_id, right_rows[row_id])
    for row_id in expected.changed:
        mine, theirs = got_changed[row_id]
        assert mine == encode_record(schema, row_id, left_rows[row_id])
        assert theirs == encode_record(schema, row_id, right_rows[row_id])


# ---------------------------------------------------------------------------
# Builders


def _versions() -> dict[str, VersionDef]:
    return migratedata.version_defs()


def _make_group(
    tmp_path: Path,
    dirname: str,
    version: str,
    tables: dict,
    *,
    batch: str,
    dataset: str = DATASET,
    manifest: RelationManifest | None = None,
    schema_version: str | None = "auto",
) -> Path:
    versions = _versions()
    group_dir = tmp_path / dirname
    create_group(
        group_dir,
        dataset,
        batch,
        {name: (versions[version].tables[name], tables[name]) for name in tables},
        manifest if manifest is not None else versions[version].manifest,
        schema_version=version if schema_version == "auto" else schema_version,
    )
    return group_dir


def _demo_groups(tmp_path: Path, customers: int = 48, orders: int = 144, *, seed: int = 11):
    left_rows = migratedata.left_rows_v1(customers, orders, seed=seed)
    right_rows = migratedata.right_rows_v2(left_rows, seed=seed)
    left_dir = _make_group(tmp_path, "left-v1", "v1", left_rows, batch=migratedata.BATCH_LEFT)
    right_dir = _make_group(tmp_path, "right-v2", "v2", right_rows, batch=migratedata.BATCH_RIGHT)
    return left_dir, right_dir, left_rows, right_rows


def _catalog() -> MigrationCatalog:
    return migratedata.demo_catalog()


# ---------------------------------------------------------------------------
# Happy paths: different-length migration paths converge, then reconcile


def test_migration_end_to_end_publish(tmp_path):
    left_dir, right_dir, left_rows, right_rows = _demo_groups(tmp_path)
    catalog = _catalog()
    wire = catalog.to_wire()
    # The catalog object used by the run is rebuilt from its own JSON wire
    # form, so the run and the reference share exactly the same document.
    catalog = MigrationCatalog.from_wire(json.loads(json.dumps(wire)))
    input_hashes = {
        side: _hash_tree(directory) for side, directory in (("l", left_dir), ("r", right_dir))
    }
    with GroupStore.open(left_dir) as g:
        left_group_id = g.group_id
    with GroupStore.open(right_dir) as g:
        right_group_id = g.group_id

    result = reconcile_migrated_and_publish(
        left_dir,
        right_dir,
        catalog=catalog,
        target_version=TARGET,
        mirror_dir=tmp_path / "mirror",
        initialise_mirror=True,
        work_dir=tmp_path / "work",
        keep_temp=True,
        session_prefix="e2e",
    )
    assert result.complete, result.error or (result.reconcile and result.reconcile.reason)
    assert result.stage == "done"

    # The report binds both original group identities, the target version,
    # the catalog identity, and the actual version paths.
    assert result.left.plan.source_group_id == left_group_id
    assert result.right.plan.source_group_id == right_group_id
    assert result.target_version == TARGET
    assert result.catalog_id == catalog.catalog_id()
    assert result.left.plan.path_versions == ["v1", "v2", "v3"]
    assert result.right.plan.path_versions == ["v2", "v3"]
    assert len(result.left.plan.transform_id) == 64
    assert len(result.right.plan.transform_id) == 64
    assert result.left.plan.transform_id != result.right.plan.transform_id

    # Independent reference: target rows computed from the raw inputs.
    ref_left = _reference_migrate(wire, left_rows, "v1", "v3")
    ref_right = _reference_migrate(wire, right_rows, "v2", "v3")

    # The migrated temporary datasets equal the reference, row by row.
    assert _read_group_rows(result.left.migrated_dir) == ref_left
    assert _read_group_rows(result.right.migrated_dir) == ref_right

    # Per-table differences equal the independent oracle over the reference
    # rows, canonical byte by canonical byte.
    versions = _versions()
    for table in ("customers", "orders"):
        _assert_diffs_match_oracle(
            table,
            versions[TARGET].tables[table],
            ref_left[table],
            ref_right[table],
            result.reconcile.tables[table].differences,
        )

    # Step statistics: input/output row counts and conversion counters match
    # counts computed here from the raw inputs.
    left_tier_nonnull = sum(1 for _, row in left_rows["customers"] if row["tier"] is not None)
    left_tier_null = len(left_rows["customers"]) - left_tier_nonnull
    s1 = result.left.steps[0]
    assert (s1.from_version, s1.to_version) == ("v1", "v2")
    assert s1.tables["customers"].input_rows == len(left_rows["customers"])
    assert s1.tables["customers"].output_rows == len(ref_left["customers"])
    assert s1.tables["customers"].fields["tier"].counts == {
        "converted": left_tier_nonnull,
        "nulls": left_tier_null,
    }
    assert s1.tables["orders"].fields["channel"].counts == {
        "defaulted": len(left_rows["orders"])
    }
    assert s1.tables["orders"].fields["amount_cents"].counts == {
        "converted": len(left_rows["orders"])
    }
    s2 = result.left.steps[1]
    assert (s2.from_version, s2.to_version) == ("v2", "v3")
    assert s2.tables["orders"].fields["status"].counts == {"mapped": len(left_rows["orders"])}
    assert s2.tables["orders"].fields["cust_id"].renamed is True
    assert s2.tables["customers"].fields["segment"].counts == {
        "defaulted": len(left_rows["customers"])
    }
    assert len(result.right.steps) == 1
    assert result.right.steps[0].tables["orders"].input_rows == len(right_rows["orders"])

    # The mirror published exactly one new generation whose content equals
    # the reference-migrated right batch, FK-clean under the v3 manifest.
    assert result.generation_before == 1
    assert result.generation_after == 2
    mirror = Mirror(tmp_path / "mirror")
    assert mirror.current_generation() == 2
    mirror_rows = _read_group_rows(mirror.generation_dir(2))
    assert mirror_rows == ref_right
    assert _reference_fk_scan(mirror_rows, wire["versions"]["v3"]["manifest"]) == []
    # Generation 1 holds the reference-migrated left batch, still readable.
    assert _read_group_rows(mirror.generation_dir(1)) == ref_left

    # The publish manifest links the migration run: new batch, schema
    # version, group-level result id, and both migrated group identities.
    doc = mirror.publish_manifest()
    assert doc["schema_version"] == "v3"
    assert doc["batch_id"] == result.right.migrated_batch
    assert doc["result_id"] == result.reconcile.result_id
    assert doc["group_id"] == result.right.migrated_group_id
    assert doc["source"] == {
        "left_group_id": result.left.migrated_group_id,
        "right_group_id": result.right.migrated_group_id,
    }

    # The original snapshots were never modified: still read-only, same
    # bytes, same identities.
    for directory in (left_dir, right_dir):
        for table_file in (directory / "tables").glob("*.sqlite"):
            assert stat.S_IMODE(table_file.stat().st_mode) == 0o444
    assert _hash_tree(left_dir) == input_hashes["l"]
    assert _hash_tree(right_dir) == input_hashes["r"]
    with GroupStore.open(left_dir) as g:
        assert g.group_id == left_group_id
    with GroupStore.open(right_dir) as g:
        assert g.group_id == right_group_id


def test_empty_path_when_one_end_already_at_target(tmp_path):
    """Left is already at the target version (empty path); right migrates
    v1->v2->v3.  Both still converge and publish."""

    left_dir, right_dir, left_rows, right_rows_v2 = _demo_groups(tmp_path)
    catalog = _catalog()
    wire = catalog.to_wire()
    versions = _versions()

    # Rebuild the left group directly at v3 from independently migrated rows.
    right_rows_v1 = migratedata.left_rows_v1(48, 144, seed=11)
    left_v3_ref = _reference_migrate(wire, right_rows_v1, "v1", "v3")
    left_v3_rows = {table: sorted(rows.items()) for table, rows in left_v3_ref.items()}
    left_v3_dir = _make_group(
        tmp_path, "left-v3", "v3", left_v3_rows, batch="batch-0003"
    )
    # Right stays at v1 this time (two-step path).
    right_v1_dir = _make_group(
        tmp_path, "right-v1", "v1", right_rows_v1, batch=migratedata.BATCH_LEFT
    )

    result = reconcile_migrated_and_publish(
        left_v3_dir,
        right_v1_dir,
        catalog=catalog,
        target_version=TARGET,
        mirror_dir=tmp_path / "mirror",
        initialise_mirror=True,
        session_prefix="empty-path",
    )
    assert result.complete, result.error
    assert result.left.plan.path_versions == ["v3"]
    assert result.left.steps == []
    assert result.right.plan.path_versions == ["v1", "v2", "v3"]
    # The empty-path end keeps its original group identity and batch.
    with GroupStore.open(left_v3_dir) as g:
        assert result.left.migrated_group_id == g.group_id
    assert result.left.migrated_batch == "batch-0003"

    ref_right = _reference_migrate(wire, right_rows_v1, "v1", "v3")
    mirror = Mirror(tmp_path / "mirror")
    assert mirror.current_generation() == 2
    assert _read_group_rows(mirror.generation_dir(2)) == ref_right
    assert _reference_fk_scan(
        _read_group_rows(mirror.generation_dir(2)), wire["versions"]["v3"]["manifest"]
    ) == []
    # Identical inputs on both ends after migration: no differences.
    for table, report in result.reconcile.tables.items():
        assert report.counts() == {"only_left": 0, "only_right": 0, "changed": 0}


def test_transform_identity_stable_across_runs_and_inputs_untouched(tmp_path):
    left_dir, right_dir, left_rows, right_rows = _demo_groups(tmp_path)
    catalog = _catalog()
    before = {d: _hash_tree(d) for d in (left_dir, right_dir)}

    results = []
    for run in (1, 2):
        results.append(
            reconcile_migrated_and_publish(
                left_dir,
                right_dir,
                catalog=catalog,
                target_version=TARGET,
                mirror_dir=tmp_path / f"mirror-{run}",
                initialise_mirror=True,
                work_dir=tmp_path / f"work-{run}",
                keep_temp=True,
                session_prefix=f"stable-{run}",
            )
        )
    first, second = results
    assert first.complete and second.complete

    # Same source versions, target version, and catalog content => identical
    # transformation identities and identical migrated group identities.
    for side in ("left", "right"):
        a = getattr(first, side)
        b = getattr(second, side)
        assert a.plan.transform_id == b.plan.transform_id
        assert a.migrated_group_id == b.migrated_group_id
        assert a.migrated_batch == b.migrated_batch
        assert _read_group_rows(a.migrated_dir) == _read_group_rows(b.migrated_dir)
    assert first.reconcile.result_id == second.reconcile.result_id

    # The transform identity is recomputed from first principles here.
    catalog_id = hashlib.sha256(
        b"RECON:MIGRATION-CATALOG:v1".__len__().to_bytes(4, "big")
        + b"RECON:MIGRATION-CATALOG:v1"
        + len(catalog.canonical_bytes()).to_bytes(4, "big")
        + catalog.canonical_bytes()
    ).hexdigest()
    assert first.catalog_id == catalog_id

    # Original snapshots: content and identities unchanged, still read-only.
    for directory, hashes in before.items():
        assert _hash_tree(directory) == hashes
    for directory in (left_dir, right_dir):
        for table_file in (directory / "tables").glob("*.sqlite"):
            assert stat.S_IMODE(table_file.stat().st_mode) == 0o444


def test_migrate_and_reconcile_without_mirror(tmp_path):
    """The migration flow also works as a reconcile-only run (no mirror):
    both ends converge on the target structure and the group result binds
    the migrated identities."""

    left_dir, right_dir, left_rows, right_rows = _demo_groups(tmp_path)
    catalog = _catalog()
    wire = catalog.to_wire()
    result = reconcile_migrated_and_publish(
        left_dir, right_dir, catalog=catalog, target_version="v3"
    )
    assert result.complete
    assert result.stage == "done"
    assert result.reconcile is not None
    assert result.reconcile.publish is None
    assert result.generation_before is None and result.generation_after is None
    ref_left = _reference_migrate(wire, left_rows, "v1", "v3")
    ref_right = _reference_migrate(wire, right_rows, "v2", "v3")
    versions = _versions()
    for table in ("customers", "orders"):
        _assert_diffs_match_oracle(
            table,
            versions["v3"].tables[table],
            ref_left[table],
            ref_right[table],
            result.reconcile.tables[table].differences,
        )


# ---------------------------------------------------------------------------
# Plan errors: raised before any row is converted


def test_no_path_is_a_plan_error(tmp_path):
    left_dir, right_dir, _, _ = _demo_groups(tmp_path)
    # v1 -> v2 -> v3 exists, but nothing leads back to v1.
    result = reconcile_migrated_and_publish(
        left_dir, right_dir, catalog=_catalog(), target_version="v1"
    )
    assert not result.complete
    assert result.stage == "plan"
    assert result.error["code"] == "no_path"
    assert result.error["context"]["source_version"] == "v2"
    assert result.error["context"]["target_version"] == "v1"
    with pytest.raises(MigrationPlanError, match="no_path"):
        plan_migrations(_catalog(), left_dir, right_dir, "v1")


def test_ambiguous_path_is_a_plan_error_and_conversion_never_starts(tmp_path):
    """Two routes v1->v3 (direct and via v2) must not be resolved by picking
    one; and because the data would also fail conversion, getting a plan
    error here proves planning happens before any row is converted."""

    versions = _versions()
    steps = list(_catalog().steps)
    # A second, direct v1->v3 step makes two routes; its enum map is also
    # missing "C", so if the planner wrongly picked this route and started
    # converting, the run would fail with a conversion error instead of the
    # expected plan error.
    steps.append(
        StepDef(
            "v1",
            "v3",
            {
                "customers": (
                    FieldRule.rename("full_name", "name"),
                    FieldRule.copy("tier_code", "tier"),
                    FieldRule.default("segment", "standard"),
                ),
                "orders": (
                    FieldRule.rename("cust_id", "customer_id"),
                    FieldRule.convert("amount_cents", "amount_text", CONVERT_TEXT_TO_INT64),
                    FieldRule.enum("status", "state", {"N": "NEW", "P": "PAID"}),  # missing "C"
                    FieldRule.default("channel", "web"),
                ),
            },
        )
    )
    catalog = MigrationCatalog.declare(DATASET, versions, steps)
    left_dir, right_dir, _, _ = _demo_groups(tmp_path)

    result = reconcile_migrated_and_publish(
        left_dir, right_dir, catalog=catalog, target_version="v3"
    )
    assert not result.complete
    assert result.stage == "plan"
    assert result.error["code"] == "ambiguous_path"
    routes = result.error["context"]["routes"]
    assert {tuple(r) for r in routes} == {
        ("v1->v2", "v2->v3"),
        ("v1->v3",),
    }
    # No migration stats were produced: conversion never started.
    assert result.left is None and result.right is None


def test_catalog_cycle_rejected(tmp_path):
    versions = {
        name: VersionDef(
            name,
            {"t": Schema.declare(("a", "text", False))},
            RelationManifest.declare(1, []),
        )
        for name in ("v1", "v2", "v3")
    }
    rules = {"t": (FieldRule.copy("a"),)}
    with pytest.raises(MigrationPlanError, match="catalog_cycle") as excinfo:
        MigrationCatalog.declare(
            DATASET,
            versions,
            [
                StepDef("v1", "v2", rules),
                StepDef("v2", "v3", rules),
                StepDef("v3", "v2", rules),
            ],
        )
    assert excinfo.value.context["cycle"] == ["v2", "v3", "v2"]


def test_unknown_and_missing_versions(tmp_path):
    left_dir, right_dir, _, _ = _demo_groups(tmp_path)
    catalog = _catalog()

    result = reconcile_migrated_and_publish(
        left_dir, right_dir, catalog=catalog, target_version="v9"
    )
    assert result.stage == "plan"
    assert result.error["code"] == "unknown_target_version"

    stray = _make_group(tmp_path, "stray", "v1", migratedata.left_rows_v1(40, 80), batch="b-x")
    meta = json.loads((stray / "group.json").read_text())
    meta["schema_version"] = "v9"
    (stray / "group.json").write_text(json.dumps(meta, indent=2, sort_keys=True) + "\n")
    result = reconcile_migrated_and_publish(
        stray, right_dir, catalog=catalog, target_version="v3"
    )
    assert result.stage == "plan"
    assert result.error["code"] == "unknown_source_version"
    assert result.error["context"]["version"] == "v9"

    no_version = _make_group(
        tmp_path, "noversion", "v1", migratedata.left_rows_v1(40, 80),
        batch="b-y", schema_version=None,
    )
    result = reconcile_migrated_and_publish(
        no_version, right_dir, catalog=catalog, target_version="v3"
    )
    assert result.stage == "plan"
    assert result.error["code"] == "schema_version_missing"


def test_group_structure_must_match_catalog_declaration(tmp_path):
    left_dir, right_dir, _, _ = _demo_groups(tmp_path)
    catalog = _catalog()

    # Group claims v1 but its customers table carries the v2 schema.
    versions = _versions()
    broken_rows = {
        "customers": [("c-1", {"name": "A", "tier": 3})],
        "orders": [
            ("o-1", {"customer_id": "c-1", "amount_text": "5", "state": "N", "legacy_note": None})
        ],
    }
    broken = tmp_path / "broken"
    create_group(
        broken,
        DATASET,
        "b-1",
        {
            "customers": (versions["v2"].tables["customers"], broken_rows["customers"]),
            "orders": (versions["v1"].tables["orders"], broken_rows["orders"]),
        },
        versions["v1"].manifest,
        schema_version="v1",
    )
    result = reconcile_migrated_and_publish(
        broken, right_dir, catalog=catalog, target_version="v3"
    )
    assert result.stage == "plan"
    assert result.error["code"] == "version_schema_mismatch"
    assert result.error["context"]["table"] == "customers"
    assert result.error["context"]["version"] == "v1"

    # Group claims v1 but its manifest is not the catalog's v1 manifest.
    wrong_manifest = RelationManifest.declare(99, versions["v1"].manifest.foreign_keys)
    broken2 = _make_group(
        tmp_path,
        "broken2",
        "v1",
        {
            "customers": [("c-1", {"name": "A", "tier": "3"})],
            "orders": [
                ("o-1", {"customer_id": "c-1", "amount_text": "5", "state": "N", "legacy_note": None})
            ],
        },
        batch="b-2",
        manifest=wrong_manifest,
    )
    result = reconcile_migrated_and_publish(
        broken2, right_dir, catalog=catalog, target_version="v3"
    )
    assert result.stage == "plan"
    assert result.error["code"] == "version_manifest_mismatch"

    # Dataset id must match the catalog's dataset.
    other = _make_group(
        tmp_path,
        "other-dataset",
        "v1",
        {
            "customers": [("c-1", {"name": "A", "tier": "3"})],
            "orders": [
                ("o-1", {"customer_id": "c-1", "amount_text": "5", "state": "N", "legacy_note": None})
            ],
        },
        batch="b-3",
        dataset="ds-other",
    )
    result = reconcile_migrated_and_publish(
        other, right_dir, catalog=catalog, target_version="v3"
    )
    assert result.stage == "plan"
    assert result.error["code"] == "dataset_mismatch"


def test_field_mapping_duplicate_missing_unknown(tmp_path):
    versions = {
        "v1": VersionDef(
            "v1",
            {"t": Schema.declare(("a", "text", False), ("b", "text", False))},
            RelationManifest.declare(1, []),
        ),
        "v2": VersionDef(
            "v2",
            {"t": Schema.declare(("x", "text", False), ("y", "text", False))},
            RelationManifest.declare(1, []),
        ),
    }

    def build(rules):
        return MigrationCatalog.declare(
            DATASET, versions, [StepDef("v1", "v2", {"t": rules})]
        )

    with pytest.raises(MigrationPlanError, match="field_mapping_duplicate") as excinfo:
        build(
            (
                FieldRule.rename("x", "a"),
                FieldRule.rename("x", "b"),
                FieldRule.rename("y", "b"),
            )
        )
    assert excinfo.value.context == {"step": "v1->v2", "table": "t", "field": "x"}

    with pytest.raises(MigrationPlanError, match="field_mapping_missing") as excinfo:
        build((FieldRule.rename("x", "a"),))
    assert excinfo.value.context["missing_fields"] == ["y"]

    with pytest.raises(MigrationPlanError, match="field_mapping_unknown") as excinfo:
        build(
            (
                FieldRule.rename("x", "a"),
                FieldRule.rename("y", "b"),
                FieldRule.rename("z", "a"),
            )
        )
    assert excinfo.value.context["unknown_fields"] == ["z"]

    with pytest.raises(MigrationPlanError, match="source_field_unknown") as excinfo:
        build(
            (
                FieldRule.rename("x", "a"),
                FieldRule.rename("y", "ghost"),
            )
        )
    assert excinfo.value.context["source"] == "ghost"

    # A step that does not cover every table of the dataset.
    versions_two = {
        "v1": VersionDef(
            "v1",
            {
                "t": Schema.declare(("a", "text", False)),
                "u": Schema.declare(("a", "text", False)),
            },
            RelationManifest.declare(1, []),
        ),
        "v2": VersionDef(
            "v2",
            {
                "t": Schema.declare(("a", "text", False)),
                "u": Schema.declare(("a", "text", False)),
            },
            RelationManifest.declare(1, []),
        ),
    }
    with pytest.raises(MigrationPlanError, match="table_coverage") as excinfo:
        MigrationCatalog.declare(
            DATASET,
            versions_two,
            [StepDef("v1", "v2", {"t": (FieldRule.copy("a"),)})],
        )
    assert excinfo.value.context["missing_tables"] == ["u"]


def test_field_rule_type_and_default_errors(tmp_path):
    versions = {
        "v1": VersionDef(
            "v1",
            {"t": Schema.declare(("a", "text", False), ("n", "int64", False))},
            RelationManifest.declare(1, []),
        ),
        "v2": VersionDef(
            "v2",
            {
                "t": Schema.declare(
                    ("a", "int64", False), ("n", "int64", False), ("extra", "text", False)
                )
            },
            RelationManifest.declare(1, []),
        ),
    }

    def build(rules):
        return MigrationCatalog.declare(
            DATASET, versions, [StepDef("v1", "v2", {"t": rules})]
        )

    valid_a = FieldRule.convert("a", "a", CONVERT_TEXT_TO_INT64)
    valid_n = FieldRule.copy("n")
    valid_extra = FieldRule.default("extra", "x")

    # Type change without a declared conversion.
    with pytest.raises(MigrationPlanError, match="type_change_without_conversion"):
        build((FieldRule.copy("a"), valid_n, valid_extra))
    # Declared conversion whose endpoint types do not match.
    with pytest.raises(MigrationPlanError, match="conversion_type_mismatch"):
        build(
            (
                FieldRule.convert("a", "a", "int64_to_text"),
                valid_n,
                valid_extra,
            )
        )
    # Enum rewrite on a non-text field.
    with pytest.raises(MigrationPlanError, match="enum_on_non_text"):
        build(
            (
                valid_a,
                FieldRule.enum("n", "n", {"1": "one"}),
                valid_extra,
            )
        )
    # Non-injective enum map.
    with pytest.raises(MigrationPlanError, match="enum_not_one_to_one"):
        versions_enum = {
            "v1": VersionDef(
                "v1", {"t": Schema.declare(("a", "text", False))}, RelationManifest.declare(1, [])
            ),
            "v2": VersionDef(
                "v2", {"t": Schema.declare(("a", "text", False))}, RelationManifest.declare(1, [])
            ),
        }
        MigrationCatalog.declare(
            DATASET,
            versions_enum,
            [StepDef("v1", "v2", {"t": (FieldRule.enum("a", "a", {"x": "1", "y": "1"}),)})],
        )
    # Constant default on a field that exists in the source schema.
    with pytest.raises(MigrationPlanError, match="default_on_existing_field"):
        build((valid_a, FieldRule.default("n", 0), valid_extra))
    # Default of the wrong type, and a null default for a non-nullable field.
    with pytest.raises(MigrationPlanError, match="invalid_default"):
        build((valid_a, valid_n, FieldRule.default("extra", 7)))
    with pytest.raises(MigrationPlanError, match="invalid_default"):
        build((valid_a, valid_n, FieldRule.default("extra", None)))
    # A rule declaring both a source and a default.
    with pytest.raises(MigrationPlanError, match="source_and_default"):
        build((valid_a, FieldRule(target="n", source="n", default_value=0), valid_extra))


def test_manifest_unmappable_is_a_plan_error(tmp_path):
    versions = {
        "v1": VersionDef(
            "v1", {"t": Schema.declare(("a", "text", False))}, RelationManifest.declare(1, [])
        ),
        "v2": VersionDef(
            "v2",
            {"t": Schema.declare(("a", "text", False))},
            RelationManifest.declare(1, []),
        ),
    }
    # The v2 manifest references a column that does not exist in v2.
    from recon.dataset import ForeignKey

    bad = VersionDef(
        "v2",
        {"t": Schema.declare(("a", "text", False))},
        RelationManifest.declare(1, [ForeignKey("t", "ghost", "t", True)]),
    )
    with pytest.raises(MigrationPlanError, match="manifest_unmappable") as excinfo:
        MigrationCatalog.declare(
            DATASET,
            {"v1": versions["v1"], "v2": bad},
            [StepDef("v1", "v2", {"t": (FieldRule.copy("a"),)})],
        )
    assert excinfo.value.context["version"] == "v2"


# ---------------------------------------------------------------------------
# Conversion failures: located, categorised, and never a false success


def _tiny_group(tmp_path, dirname, rows, *, nullable=False, batch="b-1"):
    versions = {
        "v1": VersionDef(
            "v1",
            {"t": Schema.declare(("v", "text", nullable))},
            RelationManifest.declare(1, []),
        ),
        "v2": VersionDef(
            "v2",
            {"t": Schema.declare(("v", "text", nullable))},
            RelationManifest.declare(1, []),
        ),
    }
    group_dir = tmp_path / dirname
    create_group(
        group_dir,
        DATASET,
        batch,
        {"t": (versions["v1"].tables["t"], rows)},
        versions["v1"].manifest,
        schema_version="v1",
    )
    return group_dir, versions


def test_enum_unmapped_value_reported_with_location(tmp_path):
    group_dir, versions = _tiny_group(
        tmp_path, "g", [("r-1", {"v": "A"}), ("r-2", {"v": "X"})]
    )
    catalog = MigrationCatalog.declare(
        DATASET,
        versions,
        [StepDef("v1", "v2", {"t": (FieldRule.enum("v", "v", {"A": "ACTIVE"}),)})],
    )
    with pytest.raises(MigrationConversionError) as excinfo:
        migrate_group(group_dir, catalog, "v2", tmp_path / "work")
    failures = excinfo.value.failures
    assert failures == [
        {
            "step": "v1->v2",
            "table": "t",
            "row_id": "r-2",
            "field": "v",
            "category": "enum_unmapped",
            "detail": "value 'X' is not listed in the enum map",
        }
    ]
    assert excinfo.value.step_label == "v1->v2"

    # Through the orchestration: no difference result is produced, and the
    # failure is reported with its full location.
    result = reconcile_migrated_and_publish(
        group_dir, group_dir, catalog=catalog, target_version="v2"
    )
    assert not result.complete
    assert result.stage == "migrate"
    assert result.error["code"] == "conversion_failed"
    assert result.error["failures"][0]["row_id"] == "r-2"
    assert result.reconcile is None


@pytest.mark.parametrize(
    "bad_value",
    ["12.5", "abc", "", "+7", " 7", "7 ", "1e3", "0x10", "--7", "７"],
)
def test_bad_decimal_text_rejected(tmp_path, bad_value):
    group_dir, versions = _tiny_group(tmp_path, "g", [("r-1", {"v": bad_value})])
    versions["v2"] = VersionDef(
        "v2",
        {"t": Schema.declare(("v", "int64", False))},
        RelationManifest.declare(1, []),
    )
    catalog = MigrationCatalog.declare(
        DATASET,
        versions,
        [StepDef("v1", "v2", {"t": (FieldRule.convert("v", "v", CONVERT_TEXT_TO_INT64),)})],
    )
    with pytest.raises(MigrationConversionError) as excinfo:
        migrate_group(group_dir, catalog, "v2", tmp_path / "work")
    assert excinfo.value.failures[0]["category"] == "bad_decimal_format"
    assert excinfo.value.failures[0]["row_id"] == "r-1"
    assert excinfo.value.failures[0]["field"] == "v"


@pytest.mark.parametrize(
    "bad_value",
    ["9223372036854775808", "-9223372036854775809", "99999999999999999999999999"],
)
def test_decimal_text_out_of_int64_range_rejected(tmp_path, bad_value):
    group_dir, versions = _tiny_group(tmp_path, "g", [("r-1", {"v": bad_value})])
    versions["v2"] = VersionDef(
        "v2",
        {"t": Schema.declare(("v", "int64", False))},
        RelationManifest.declare(1, []),
    )
    catalog = MigrationCatalog.declare(
        DATASET,
        versions,
        [StepDef("v1", "v2", {"t": (FieldRule.convert("v", "v", CONVERT_TEXT_TO_INT64),)})],
    )
    with pytest.raises(MigrationConversionError) as excinfo:
        migrate_group(group_dir, catalog, "v2", tmp_path / "work")
    assert excinfo.value.failures[0]["category"] == "int64_out_of_range"


def test_decimal_text_boundaries_convert_exactly(tmp_path):
    group_dir, versions = _tiny_group(
        tmp_path,
        "g",
        [
            ("r-max", {"v": "9223372036854775807"}),
            ("r-min", {"v": "-9223372036854775808"}),
            ("r-zero", {"v": "0"}),
            ("r-negzero", {"v": "-0"}),
            ("r-padded", {"v": "007"}),
        ],
    )
    versions["v2"] = VersionDef(
        "v2",
        {"t": Schema.declare(("v", "int64", False))},
        RelationManifest.declare(1, []),
    )
    catalog = MigrationCatalog.declare(
        DATASET,
        versions,
        [StepDef("v1", "v2", {"t": (FieldRule.convert("v", "v", CONVERT_TEXT_TO_INT64),)})],
    )
    migration = migrate_group(group_dir, catalog, "v2", tmp_path / "work")
    rows = _read_group_rows(migration.migrated_dir)["t"]
    assert rows == {
        "r-max": {"v": 9223372036854775807},
        "r-min": {"v": -9223372036854775808},
        "r-zero": {"v": 0},
        "r-negzero": {"v": 0},
        "r-padded": {"v": 7},
    }


def test_null_into_non_nullable_target_rejected(tmp_path):
    group_dir, versions = _tiny_group(
        tmp_path,
        "g",
        [("r-1", {"v": None}), ("r-2", {"v": "x"})],
        nullable=True,
    )
    # v2 tightens the nullability: the declaration is legal, the data is not.
    versions["v2"] = VersionDef(
        "v2",
        {"t": Schema.declare(("v", "text", False))},
        RelationManifest.declare(1, []),
    )
    catalog = MigrationCatalog.declare(
        DATASET,
        versions,
        [StepDef("v1", "v2", {"t": (FieldRule.copy("v"),)})],
    )
    with pytest.raises(MigrationConversionError) as excinfo:
        migrate_group(group_dir, catalog, "v2", tmp_path / "work")
    failures = excinfo.value.failures
    assert len(failures) == 1
    assert failures[0]["category"] == "null_violation"
    assert failures[0]["row_id"] == "r-1"
    assert failures[0]["field"] == "v"
    # The row without a null converted fine before the abort was raised:
    # exactly one failure was reported, not a silent skip.


def test_multiple_failures_collected_across_tables(tmp_path):
    left_dir, right_dir, left_rows, _ = _demo_groups(tmp_path)
    # Break one value in each of two tables; both conversions belong to the
    # same step (v1->v2), so both failures must be collected and reported
    # before the run aborts.
    broken = {name: list(rows) for name, rows in left_rows.items()}
    broken["customers"] = [
        (row_id, {**values, "tier": "abc"}) if row_id == "c-000001" else (row_id, values)
        for row_id, values in broken["customers"]
    ]
    broken["orders"] = [
        (row_id, {**values, "amount_text": "1.5"}) if row_id == "o-000001" else (row_id, values)
        for row_id, values in broken["orders"]
    ]
    left_broken = _make_group(tmp_path, "left-broken", "v1", broken, batch="b-broken")
    result = reconcile_migrated_and_publish(
        left_broken, right_dir, catalog=_catalog(), target_version="v3"
    )
    assert not result.complete
    assert result.stage == "migrate"
    assert result.error["code"] == "conversion_failed"
    by_location = {
        (f["step"], f["table"], f["row_id"], f["field"], f["category"])
        for f in result.error["failures"]
    }
    assert by_location == {
        ("v1->v2", "customers", "c-000001", "tier", "bad_decimal_format"),
        ("v1->v2", "orders", "o-000001", "amount_cents", "bad_decimal_format"),
    }
    assert result.reconcile is None


# ---------------------------------------------------------------------------
# Final-state foreign keys, publish safety, and retry


def test_dangling_fk_after_conversion_blocks_publish(tmp_path):
    """The foreign-key column is renamed by the migration (customer_id ->
    cust_id); the final whole-group state is checked under the *target*
    manifest, and a dangling reference blocks the publish."""

    left_dir, right_dir, left_rows, right_rows = _demo_groups(tmp_path)
    broken_right = {name: list(rows) for name, rows in right_rows.items()}
    broken_right["orders"] = broken_right["orders"] + [
        ("o-ghost", {"customer_id": "c-ghost", "amount_cents": 1, "state": "N", "channel": "web"})
    ]
    right_broken = _make_group(tmp_path, "right-broken", "v2", broken_right, batch="b-broken")
    catalog = _catalog()
    wire = catalog.to_wire()

    result = reconcile_migrated_and_publish(
        left_dir,
        right_broken,
        catalog=catalog,
        target_version="v3",
        mirror_dir=tmp_path / "mirror",
        initialise_mirror=True,
    )
    # Migration and per-table reconciliation succeeded; the final-state FK
    # check under the v3 manifest (on the renamed cust_id column) refused.
    assert not result.complete
    assert result.stage == "publish"
    assert result.reconcile is not None
    assert all(rep.complete for rep in result.reconcile.tables.values())
    violations = result.reconcile.publish.fk_violations
    assert violations == [
        {
            "child_table": "orders",
            "child_row_id": "o-ghost",
            "column": "cust_id",
            "value": "c-ghost",
            "parent_table": "customers",
        }
    ]
    # Only generation 1 (the migrated local state) exists and is unchanged.
    mirror = Mirror(tmp_path / "mirror")
    assert mirror.current_generation() == 1
    ref_left = _reference_migrate(wire, left_rows, "v1", "v3")
    assert _read_group_rows(mirror.generation_dir(1)) == ref_left
    assert sorted(p.name for p in (mirror.path / "generations").iterdir()) == ["000001"]


def test_failed_conversion_keeps_old_mirror_then_fixed_catalog_retries(tmp_path):
    left_dir, right_dir, left_rows, right_rows = _demo_groups(tmp_path)
    good = _catalog()
    wire = good.to_wire()

    # Initialise the mirror at generation 1 from the migrated local state.
    first = migrate_group(left_dir, good, "v3", tmp_path / "init-work")
    with GroupStore.open(first.migrated_dir) as source:
        Mirror.initialize(tmp_path / "mirror", source)
    mirror = Mirror(tmp_path / "mirror")
    pointer_before = (mirror.path / "CURRENT").read_bytes()
    gen1_rows = _read_group_rows(mirror.generation_dir(1))

    # A broken catalog: the enum map does not list "C", which the left data
    # contains.  The conversion must fail mid-migration.
    bad_wire = json.loads(json.dumps(wire))
    for step in bad_wire["steps"]:
        if step["from"] == "v2" and step["to"] == "v3":
            for rule in step["tables"]["orders"]["fields"]:
                if rule["target"] == "status":
                    del rule["enum"]["C"]
    bad = MigrationCatalog.from_wire(bad_wire)
    assert bad.catalog_id() != good.catalog_id()

    work = tmp_path / "failed-work"
    result = reconcile_migrated_and_publish(
        left_dir,
        right_dir,
        catalog=bad,
        target_version="v3",
        mirror_dir=tmp_path / "mirror",
        work_dir=work,
    )
    assert not result.complete
    assert result.stage == "migrate"
    assert result.error["code"] == "conversion_failed"
    failures = result.error["failures"]
    assert failures
    assert {f["step"] for f in failures} == {"v2->v3"}
    assert {f["category"] for f in failures} == {"enum_unmapped"}
    assert {f["table"] for f in failures} == {"orders"}
    # Every order whose state is "C" is reported by row id.
    expected_rows = {
        row_id for row_id, values in left_rows["orders"] if values["state"] == "C"
    }
    assert {f["row_id"] for f in failures} == expected_rows

    # The old mirror is untouched and still readable; no staged leftovers.
    assert mirror.current_generation() == 1
    assert (mirror.path / "CURRENT").read_bytes() == pointer_before
    assert _read_group_rows(mirror.generation_dir(1)) == gen1_rows
    assert sorted(p.name for p in (mirror.path / "generations").iterdir()) == ["000001"]
    with mirror.open_current() as current:
        assert current.schema_version == "v3"
    # Temporary migration data was cleaned up, so the retry starts fresh.
    assert not work.exists() or not any(work.iterdir())

    # Fix the catalog and retry: the run completes and publishes generation 2.
    retry = reconcile_migrated_and_publish(
        left_dir,
        right_dir,
        catalog=good,
        target_version="v3",
        mirror_dir=tmp_path / "mirror",
        work_dir=work,
    )
    assert retry.complete, retry.error
    assert retry.generation_before == 1
    assert retry.generation_after == 2
    assert mirror.current_generation() == 2
    ref_right = _reference_migrate(wire, right_rows, "v2", "v3")
    assert _read_group_rows(mirror.generation_dir(2)) == ref_right
    assert _reference_fk_scan(
        _read_group_rows(mirror.generation_dir(2)), wire["versions"]["v3"]["manifest"]
    ) == []
    # Generation 1 is still intact after the successful publish too.
    assert _read_group_rows(mirror.generation_dir(1)) == gen1_rows


# ---------------------------------------------------------------------------
# Catalog identity and wire handling


def test_catalog_identity_stable_under_wire_roundtrip(tmp_path):
    catalog = _catalog()
    wire = catalog.to_wire()
    assert MigrationCatalog.from_wire(json.loads(json.dumps(wire))).catalog_id() == (
        catalog.catalog_id()
    )
    path = tmp_path / "catalog.json"
    path.write_text(json.dumps(wire, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    assert MigrationCatalog.load(path).catalog_id() == catalog.catalog_id()
    # A semantically different catalog (one enum value changed) has a
    # different identity.
    changed = json.loads(json.dumps(wire))
    for step in changed["steps"]:
        if step["from"] == "v2":
            for rule in step["tables"]["orders"]["fields"]:
                if rule["target"] == "status":
                    rule["enum"]["C"] = "CANCELED"
    assert MigrationCatalog.from_wire(changed).catalog_id() != catalog.catalog_id()
    # Duplicate JSON keys are rejected rather than silently overwritten.
    dup = tmp_path / "dup.json"
    dup.write_text(
        '{"format": "recon-migration-catalog-v1", "format": "recon-migration-catalog-v1"}',
        encoding="utf-8",
    )
    with pytest.raises(MigrationPlanError, match="catalog_format"):
        MigrationCatalog.load(dup)


def test_catalog_from_wire_rejects_structural_garbage(tmp_path):
    with pytest.raises(MigrationPlanError, match="catalog_format"):
        MigrationCatalog.from_wire({"format": "nope"})
    with pytest.raises(MigrationPlanError, match="catalog_format"):
        MigrationCatalog.from_wire(["not", "a", "mapping"])
    wire = _catalog().to_wire()
    wire["steps"][0]["tables"]["orders"]["fields"][0]["bogus"] = 1
    with pytest.raises(MigrationPlanError, match="catalog_format"):
        MigrationCatalog.from_wire(wire)


def test_execute_migration_writes_isolated_readonly_datasets(tmp_path):
    left_dir, _, left_rows, _ = _demo_groups(tmp_path)
    catalog = _catalog()
    wire = catalog.to_wire()
    migration = migrate_group(left_dir, catalog, "v3", tmp_path / "work")

    # Every step produced its own isolated group directory, all read-only.
    work = tmp_path / "work"
    step_dirs = sorted(p.name for p in work.iterdir())
    assert step_dirs == ["step-v1-to-v2", "step-v2-to-v3"]
    for sqlite_file in work.rglob("*.sqlite"):
        assert stat.S_IMODE(sqlite_file.stat().st_mode) == 0o444

    # The final migrated group carries the target version label and the
    # deterministic migrated batch; its rows equal the reference.
    with GroupStore.open(migration.migrated_dir) as migrated:
        assert migrated.schema_version == "v3"
        assert migrated.batch_id == migration.migrated_batch
        assert migrated.manifest.content_hash() == _versions()["v3"].manifest.content_hash()
    assert migration.migrated_batch.startswith(f"{migratedata.BATCH_LEFT}=>v3#")
    assert _read_group_rows(migration.migrated_dir) == _reference_migrate(
        wire, left_rows, "v1", "v3"
    )

    # The intermediate group is a valid group at v2 as well.
    with GroupStore.open(work / "step-v1-to-v2") as intermediate:
        assert intermediate.schema_version == "v2"
    ref_v2 = _reference_migrate(wire, left_rows, "v1", "v2")
    assert _read_group_rows(work / "step-v1-to-v2") == ref_v2

    # The source group was not touched.
    with GroupStore.open(left_dir) as source:
        assert source.schema_version == "v1"
        assert source.batch_id == migratedata.BATCH_LEFT
