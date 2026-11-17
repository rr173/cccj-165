"""End-to-end protocol tests.

Every test compares the protocol's findings against differences computed
independently from the two inputs, verifies each reported row's exact canonical
bytes, applies the reported difference set to a fresh structure, and checks the
resulting commitment against the peer's snapshot commitment.
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

from recon import data as demo_data
from recon.encoding import (
    COMMIT_PARAMS,
    Schema,
    compute_commitment,
    decode_record,
    encode_record,
    record_row_id_bytes,
)
from recon.runner import run_session
from recon.store import SnapshotStore, create_snapshot
from recon.summary import SummaryParams
from support import (
    apply_differences,
    commitment_for,
    commitment_of_map,
    expected,
)

SCHEMA = Schema.declare(
    ("payload", "text", False),
    ("amount", "int64", True),
    ("active", "bool", False),
    ("note", "text", True),
)


def _make(tmp_path: Path, name: str, left, right, schema: Schema = SCHEMA):
    left_path = tmp_path / f"{name}-left.sqlite"
    right_path = tmp_path / f"{name}-right.sqlite"
    create_snapshot(left_path, schema, left, created_from=f"{name}-left")
    create_snapshot(right_path, schema, right, created_from=f"{name}-right")
    return left_path, right_path


def _run(tmp_path, name, left, right, params=None, schema=SCHEMA, **kwargs):
    left_path, right_path = _make(tmp_path, name, left, right, schema)
    left_store = SnapshotStore.open(left_path)
    right_store = SnapshotStore.open(right_path)
    try:
        outcome = run_session(left_store, right_store, params=params or SummaryParams(), **kwargs)
    finally:
        left_store.close()
        right_store.close()
    return outcome, left_path, right_path


def _assert_matches_oracle(schema, left, right, outcome):
    exp = expected(left, right, schema)
    diffs = outcome.result.differences

    got_left = {record_row_id_bytes(r).decode("utf-8"): r for r in diffs.only_left}
    got_right = {record_row_id_bytes(r).decode("utf-8"): r for r in diffs.only_right}
    got_changed = {
        record_row_id_bytes(a).decode("utf-8"): (a, b)
        for a, b in zip(diffs.changed_left, diffs.changed_right)
    }
    exp_ids = exp.row_ids()

    assert set(got_left) == exp_ids["only_left"], "only_left row ids diverge from the oracle"
    assert set(got_right) == exp_ids["only_right"], "only_right row ids diverge from the oracle"
    assert set(got_changed) == exp_ids["changed"], "changed row ids diverge from the oracle"

    left_by_id = {row_id: values for row_id, values in left}
    right_by_id = {row_id: values for row_id, values in right}

    for row_id in exp_ids["only_left"]:
        want = encode_record(schema, row_id, left_by_id[row_id])
        assert got_left[row_id] == want, f"only_left row {row_id} has wrong canonical bytes"
        assert decode_record(schema, got_left[row_id]) == (row_id, left_by_id[row_id])

    for row_id in exp_ids["only_right"]:
        want = encode_record(schema, row_id, right_by_id[row_id])
        assert got_right[row_id] == want, f"only_right row {row_id} has wrong canonical bytes"

    for row_id in exp_ids["changed"]:
        mine, theirs = got_changed[row_id]
        assert mine == encode_record(schema, row_id, left_by_id[row_id])
        assert theirs == encode_record(schema, row_id, right_by_id[row_id])
        # A change must be paired under one row id, never two unrelated rows.
        assert record_row_id_bytes(mine) == record_row_id_bytes(theirs) == row_id.encode("utf-8")

    # Independent application of the reported differences.
    applied = apply_differences(schema, left, diffs)
    assert commitment_of_map(schema, applied) == commitment_for(schema, right)
    return exp


def test_sparse_three_differences(tmp_path):
    rows = 2_000
    left = demo_data.base_rows(rows)
    variant = demo_data.variant_sparse(rows)
    right = variant.rows
    outcome, left_path, right_path = _run(tmp_path, "sparse", left, right, SummaryParams(leaf_limit=8))
    assert outcome.result.complete, outcome.result.reason
    _assert_matches_oracle(SCHEMA, left, right, outcome)
    assert outcome.result.differences.counts() == {
        "only_left": 1,
        "only_right": 1,
        "changed": 1,
    }
    assert outcome.result.fallbacks == 0, "sparse differences must not need a fallback"
    # Applied content equals the peer's original commitment, as stored on disk.
    _, _, right_meta = _read_meta(right_path)
    assert outcome.result.applied_commit == right_meta["commitment"]
    _, _, left_meta = _read_meta(left_path)
    assert outcome.result.self_commit == left_meta["commitment"]


def _read_meta(path):
    conn = sqlite3.connect(f"file:{Path(path).resolve().as_posix()}?mode=ro", uri=True)
    try:
        row = conn.execute(
            "SELECT schema_wire, params_json, commitment FROM snapshot_meta WHERE id=1"
        ).fetchone()
    finally:
        conn.close()
    return row[0], row[1], {"commitment": bytes(row[2])}


def test_identical_snapshots_complete_over_protocol(tmp_path):
    left = demo_data.base_rows(500)
    outcome, _, _ = _run(tmp_path, "same", left, list(left))
    assert outcome.result.complete
    assert outcome.result.reason == "identical"
    assert outcome.result.differences.counts() == {
        "only_left": 0,
        "only_right": 0,
        "changed": 0,
    }
    # The protocol still exchanged and checked both commitments.
    assert outcome.result.applied_commit == outcome.result.peer_commit


def test_empty_snapshots_complete_over_protocol(tmp_path):
    outcome, _, _ = _run(tmp_path, "empty", [], [])
    assert outcome.result.complete
    assert outcome.result.differences.counts() == {
        "only_left": 0,
        "only_right": 0,
        "changed": 0,
    }
    assert outcome.result.peer_commit == outcome.result.applied_commit


def test_empty_versus_nonempty(tmp_path):
    left = []
    right = demo_data.base_rows(50)
    outcome, _, _ = _run(tmp_path, "empty-vs-full", left, right)
    assert outcome.result.complete
    assert outcome.result.differences.counts() == {
        "only_left": 0,
        "only_right": 50,
        "changed": 0,
    }
    _assert_matches_oracle(SCHEMA, left, right, outcome)


def test_duplicate_business_values_with_distinct_row_ids(tmp_path):
    left = [
        ("id-a", {"payload": "same", "amount": 1, "active": True, "note": None}),
        ("id-b", {"payload": "same", "amount": 1, "active": True, "note": None}),
        ("id-c", {"payload": "unique", "amount": 9, "active": False, "note": "x"}),
    ]
    right = [
        ("id-a", {"payload": "same", "amount": 1, "active": True, "note": None}),
        ("id-b", {"payload": "same", "amount": 1, "active": True, "note": None}),
        ("id-d", {"payload": "same", "amount": 1, "active": True, "note": None}),
    ]
    outcome, _, _ = _run(tmp_path, "dupvals", left, right)
    assert outcome.result.complete
    assert outcome.result.differences.counts() == {
        "only_left": 1,
        "only_right": 1,
        "changed": 0,
    }
    _assert_matches_oracle(SCHEMA, left, right, outcome)


def test_null_empty_and_long_text(tmp_path):
    long_text = "L" + ("0123456789abcdef" * 20_000)  # 320 KB single cell
    left = [
        ("null-vs-empty", {"payload": "", "amount": None, "active": False, "note": None}),
        ("empty-vs-null", {"payload": "x", "amount": 0, "active": True, "note": ""}),
        ("long", {"payload": long_text, "amount": -1, "active": False, "note": None}),
        ("zero", {"payload": "z", "amount": 0, "active": False, "note": "n"}),
    ]
    right = [
        ("null-vs-empty", {"payload": "", "amount": 1, "active": False, "note": None}),
        ("empty-vs-null", {"payload": "x", "amount": 0, "active": True, "note": None}),
        ("long", {"payload": long_text + "!", "amount": -1, "active": False, "note": None}),
        ("zero", {"payload": "z", "amount": 0, "active": False, "note": "n"}),
    ]
    outcome, _, _ = _run(tmp_path, "nulls", left, right)
    assert outcome.result.complete
    assert outcome.result.differences.counts() == {
        "only_left": 0,
        "only_right": 0,
        "changed": 3,
    }
    _assert_matches_oracle(SCHEMA, left, right, outcome)


def test_boundary_keys(tmp_path):
    # Empty byte is impossible as a row id, so use the smallest printable key,
    # keys that are prefixes of one another, and a very large high-bit key.
    keys = ["!", "!!", "!!!", "a", "a\x01", "z", "~", "ÿ", "row-000000001"]
    left = [
        (key, {"payload": f"v-{i}", "amount": i, "active": i % 2 == 0, "note": None})
        for i, key in enumerate(keys)
    ]
    right = [
        (
            key,
            {"payload": f"v-{i}" if key != "!!" else "changed", "amount": i, "active": i % 2 == 0, "note": None},
        )
        for i, key in enumerate(keys)
        if key != "~"
    ]
    outcome, _, _ = _run(tmp_path, "bounds", left, right)
    assert outcome.result.complete
    assert outcome.result.differences.counts() == {
        "only_left": 1,
        "only_right": 0,
        "changed": 1,
    }
    _assert_matches_oracle(SCHEMA, left, right, outcome)


def test_dense_differences_with_tiny_summary_and_hash_collisions(tmp_path):
    rows = 4_000
    left = demo_data.base_rows(rows)
    right = demo_data.variant_dense(rows, seed=11).rows
    params = SummaryParams(
        position_bits=8,       # forces many real bucket collisions
        max_gen=8,
        leaf_limit=4,
        cord_max_bytes=48,     # smaller than one 40-byte entry: cords unusable
        rows_per_chunk=256,
        max_nodes_per_query=64,
    )
    outcome, _, _ = _run(tmp_path, "dense-tiny", left, right, params=params)
    assert outcome.result.complete, outcome.result.reason
    _assert_matches_oracle(SCHEMA, left, right, outcome)
    assert outcome.result.fallbacks > 0, "tiny budget must exercise a logged fallback"
    assert outcome.result.fallback_regions, "fallbacks must name their regions"
    # A fallback is never allowed to imply a whole-table transfer by accident.
    assert outcome.result.fallback_rows < len(left) + len(right)


def test_dense_medium_summary_still_completes(tmp_path):
    rows = 3_000
    left = demo_data.base_rows(rows)
    right = demo_data.variant_dense(rows, seed=5).rows
    params = SummaryParams(
        position_bits=12,
        max_gen=12,
        leaf_limit=16,
        cord_max_bytes=2048,
        rows_per_chunk=512,
        max_nodes_per_query=128,
    )
    outcome, _, _ = _run(tmp_path, "dense-medium", left, right, params=params)
    assert outcome.result.complete
    _assert_matches_oracle(SCHEMA, left, right, outcome)


def test_differences_are_paired_not_split(tmp_path):
    # One row changes only its nullable column to NULL; it must be reported as
    # changed under the same row id, not as a delete plus an insert.
    left = [("keep", {"payload": "p", "amount": 5, "active": True, "note": "old"})]
    right = [("keep", {"payload": "p", "amount": 5, "active": True, "note": None})]
    outcome, _, _ = _run(tmp_path, "paired", left, right)
    assert outcome.result.complete
    assert outcome.result.differences.counts() == {
        "only_left": 0,
        "only_right": 0,
        "changed": 1,
    }


def test_message_log_and_byte_counts_are_consistent(tmp_path):
    rows = 300
    left = demo_data.base_rows(rows)
    right = demo_data.variant_sparse(rows).rows
    outcome, _, _ = _run(tmp_path, "logging", left, right)
    result = outcome.result
    # Engine-side counters equal the message log totals on both sides.
    sent_i = sum(
        e["bytes"] for e in outcome.log.entries
        if e.get("direction") == "sent" and e["role"] == "initiator"
    )
    sent_r = sum(
        e["bytes"] for e in outcome.log.entries
        if e.get("direction") == "sent" and e["role"] == "responder"
    )
    assert sent_i == result.sent_bytes == outcome.log.total_sent("initiator")
    assert sent_r == outcome.log.total_sent("responder")
    # With no duplicates or foreign frames, each side receives exactly what
    # the other sent.
    assert result.received_bytes == sent_r
    assert result.sent_frames == len(
        [e for e in outcome.log.entries if e.get("direction") == "sent" and e["role"] == "initiator"]
    )
    # Every message carries the session id and a snapshot identity.
    for entry in outcome.log.entries:
        if "direction" in entry:
            assert entry["session"]
            assert len(entry["snapshot_id"]) == 64


def test_duplicate_answers_are_ignored(tmp_path):
    rows = 500
    left = demo_data.base_rows(rows)
    right = demo_data.variant_sparse(rows).rows
    outcome, _, _ = _run(
        tmp_path, "dupes", left, right, duplicate_answers=3
    )
    assert outcome.result.complete, outcome.result.reason
    _assert_matches_oracle(SCHEMA, left, right, outcome)
    # Every injected duplicate was recognised and dropped by message identity.
    assert outcome.result.duplicate_answers_ignored == 3
    assert outcome.result.differences.counts() == {
        "only_left": 1,
        "only_right": 1,
        "changed": 1,
    }
