"""Scale and independent-verification tests.

These run at the size the task requires (at least 100k rows, three differences),
report exact byte counts, and verify the outcome against a from-scratch,
row-by-row oracle plus a temp-structure application of the reported differences.
"""

from __future__ import annotations

import json
import sqlite3
import time
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
from support import apply_differences, commitment_for, commitment_of_map, expected

SCHEMA = Schema.declare(
    ("payload", "text", False),
    ("amount", "int64", True),
    ("active", "bool", False),
    ("note", "text", True),
)


def _snapshots(tmp_path, left, right):
    lp = tmp_path / "left.sqlite"
    rp = tmp_path / "right.sqlite"
    create_snapshot(lp, SCHEMA, left, created_from="left")
    create_snapshot(rp, SCHEMA, right, created_from="right")
    return lp, rp


def _full_sizes(store: SnapshotStore) -> dict[str, int]:
    records = list(store.iter_records())
    canonical = sum(len(r) for r in records)
    hex_wire = sum(len(r.hex()) for r in records)
    return {
        "rows": len(records),
        "canonical_row_bytes": canonical,
        "hex_row_bytes": hex_wire,
        "mean_row_bytes": canonical // max(1, len(records)),
    }


def _apply_in_fresh_sqlite(schema, applied: dict[bytes, bytes], path: Path) -> bytes:
    """Persist the applied map to a brand-new SQLite DB and re-commit it."""

    conn = sqlite3.connect(str(path))
    try:
        conn.execute("CREATE TABLE applied (row_id BLOB PRIMARY KEY, record BLOB NOT NULL UNIQUE)")
        with conn:
            conn.executemany(
                "INSERT INTO applied(row_id, record) VALUES (?, ?)", applied.items()
            )
        rows = []
        for key, record in conn.execute("SELECT row_id, record FROM applied ORDER BY row_id"):
            record = bytes(record)
            decoded_id, _ = decode_record(schema, record)
            assert record_row_id_bytes(record) == bytes(key)
            rows.append(record)
    finally:
        conn.close()
    return compute_commitment(schema, COMMIT_PARAMS, rows)


def test_100k_three_differences_exact_bytes_and_full_oracle(tmp_path):
    rows = 100_000
    left = demo_data.base_rows(rows)
    variant = demo_data.variant_sparse(rows)
    right = variant.rows
    lp, rp = _snapshots(tmp_path, left, right)

    left_store = SnapshotStore.open(lp)
    right_store = SnapshotStore.open(rp)
    try:
        left_sizes = _full_sizes(left_store)
        right_sizes = _full_sizes(right_store)
        t0 = time.time()
        outcome = run_session(left_store, right_store, params=SummaryParams(leaf_limit=64))
        elapsed = time.time() - t0
    finally:
        left_store.close()
        right_store.close()

    result = outcome.result
    assert result.complete, result.reason

    # 1. Independent row-by-row oracle on the complete inputs.
    exp = expected(left, right, SCHEMA)
    assert result.differences.counts() == exp.counts() == {
        "only_left": 1,
        "only_right": 1,
        "changed": 1,
    }
    got = {
        "only_left": {record_row_id_bytes(r).decode() for r in result.differences.only_left},
        "only_right": {record_row_id_bytes(r).decode() for r in result.differences.only_right},
        "changed": {record_row_id_bytes(r).decode() for r in result.differences.changed_left},
    }
    assert got == exp.row_ids()

    # Each reported record's canonical bytes match the corresponding input row.
    left_by_id = dict(left)
    right_by_id = dict(right)
    for record in result.differences.only_left:
        row_id, values = decode_record(SCHEMA, record)
        assert values == left_by_id[row_id]
        assert row_id not in right_by_id
    for record in result.differences.only_right:
        row_id, values = decode_record(SCHEMA, record)
        assert values == right_by_id[row_id]
        assert row_id not in left_by_id
    for mine, theirs in zip(result.differences.changed_left, result.differences.changed_right):
        row_id = record_row_id_bytes(mine).decode()
        assert mine == encode_record(SCHEMA, row_id, left_by_id[row_id])
        assert theirs == encode_record(SCHEMA, row_id, right_by_id[row_id])
        assert left_by_id[row_id] != right_by_id[row_id]

    # 2. Apply the reported differences to a fresh structure and compare the
    #    commitment with the peer snapshot's on-disk commitment.
    applied = apply_differences(SCHEMA, left, result.differences)
    applied_commit = _apply_in_fresh_sqlite(SCHEMA, applied, tmp_path / "applied.sqlite")
    peer = SnapshotStore.open(rp)
    try:
        assert applied_commit == peer.identity.commitment == result.peer_commit
    finally:
        peer.close()
    assert len(applied) == len(right)
    # Every surviving peer row is byte-identical, not just equal in count.
    peer = SnapshotStore.open(rp)
    try:
        for key, record in peer.records_by_key().items():
            assert applied[key] == record
    finally:
        peer.close()

    # 3. Exact byte counts: the whole exchange is far smaller than one full
    #    side's rows, on the wire and even in raw canonical bytes.
    total = result.sent_bytes + result.received_bytes
    assert total < left_sizes["canonical_row_bytes"] // 20
    assert total < left_sizes["hex_row_bytes"] // 20
    assert result.fallbacks == 0
    assert result.cord_requests <= 8

    print(
        json.dumps(
            {
                "rows": rows,
                "wire_total_bytes": total,
                "initiator_sent_bytes": result.sent_bytes,
                "responder_sent_bytes": result.received_bytes,
                "full_left_canonical_bytes": left_sizes["canonical_row_bytes"],
                "full_right_canonical_bytes": right_sizes["canonical_row_bytes"],
                "wire_as_fraction_of_canonical_left": round(
                    total / left_sizes["canonical_row_bytes"], 4
                ),
                "frames": result.sent_frames + result.received_frames,
                "summary_regions": result.summary_regions,
                "cord_requests": result.cord_requests,
                "probe_rows": result.probe_rows,
                "fallbacks": result.fallbacks,
                "elapsed_seconds": round(elapsed, 2),
                "differences": result.differences.counts(),
            },
            indent=2,
        )
    )


def test_dense_with_tiny_summary_full_verification(tmp_path):
    rows = 20_000
    left = demo_data.base_rows(rows)
    right = demo_data.variant_dense(rows, seed=21).rows
    lp, rp = _snapshots(tmp_path, left, right)
    params = SummaryParams(
        position_bits=10,
        max_gen=10,
        leaf_limit=8,
        cord_max_bytes=64,
        rows_per_chunk=512,
        max_nodes_per_query=64,
    )
    left_store = SnapshotStore.open(lp)
    right_store = SnapshotStore.open(rp)
    try:
        outcome = run_session(left_store, right_store, params=params)
    finally:
        left_store.close()
        right_store.close()
    result = outcome.result
    assert result.complete, result.reason
    exp = expected(left, right, SCHEMA)
    assert result.differences.counts() == exp.counts()
    assert result.fallbacks > 0
    assert result.fallback_regions

    applied = apply_differences(SCHEMA, left, result.differences)
    applied_commit = _apply_in_fresh_sqlite(SCHEMA, applied, tmp_path / "applied.sqlite")
    peer = SnapshotStore.open(rp)
    try:
        assert applied_commit == peer.identity.commitment
        assert len(applied) == peer.identity.row_count
        # Spot-check every confirming row by byte identity.
        for key in list(peer.records_by_key())[:1000]:
            assert applied[key] == peer.records_by_key()[key]
    finally:
        peer.close()


def test_identical_large_uses_tiny_wire_budget(tmp_path):
    rows = 50_000
    left = demo_data.base_rows(rows)
    lp, rp = _snapshots(tmp_path, left, list(left))
    left_store = SnapshotStore.open(lp)
    right_store = SnapshotStore.open(rp)
    try:
        canonical = sum(len(r) for r in left_store.iter_records())
        outcome = run_session(left_store, right_store, params=SummaryParams())
    finally:
        left_store.close()
        right_store.close()
    result = outcome.result
    assert result.complete
    assert result.differences.counts() == {"only_left": 0, "only_right": 0, "changed": 0}
    total = result.sent_bytes + result.received_bytes
    # Two aggregate summaries plus handshake; no rows cross at all.
    assert total < 10_000, total
    assert result.probe_rows == 0 and result.fallback_rows == 0
    # Two handshake frames plus one aggregate summary round trip; no rows.
    assert total < 6_000
    assert total < canonical // 500
