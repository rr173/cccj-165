"""Deliberate hash-bucket crowding.

With a narrow position width, many distinct row ids share a hash position.  The
protocol must still resolve differences exactly: colliding positions are
compared by actual row id, never by position alone, and no fallback may be
triggered by a collision.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from recon import data as demo_data
from recon.encoding import Schema
from recon.runner import run_session
from recon.store import SnapshotStore, create_snapshot
from recon.summary import (
    CORD_ENTRY_BYTES,
    LocalIndex,
    NodeId,
    Region,
    SummaryParams,
    position_of_row_key,
)
from support import apply_differences, commitment_for, commitment_of_map, expected

SCHEMA = Schema.declare(
    ("payload", "text", False),
    ("amount", "int64", True),
    ("active", "bool", False),
    ("note", "text", True),
)


def _crowded_positions(count: int, bits: int) -> dict[int, int]:
    """How many rows share each position when the width is ``bits``."""

    buckets: dict[int, int] = {}
    for i in range(count):
        position = position_of_row_key(f"row-{i:09d}".encode(), bits)
        buckets[position] = buckets.get(position, 0) + 1
    return buckets


def test_bucket_crowding_is_real_with_narrow_width():
    buckets = _crowded_positions(20_000, bits=4)
    # 16 positions for 20k rows: crowding is guaranteed, not incidental.
    assert len(buckets) <= 16
    assert max(buckets.values()) > 500


def test_crowded_buckets_still_resolve_every_difference(tmp_path):
    rows = 8_000
    left = demo_data.base_rows(rows)
    right = demo_data.variant_dense(rows, seed=31).rows
    lp = tmp_path / "l.sqlite"
    rp = tmp_path / "r.sqlite"
    create_snapshot(lp, SCHEMA, left, created_from="l")
    create_snapshot(rp, SCHEMA, right, created_from="r")
    params = SummaryParams(
        position_bits=4,   # 16 buckets: extreme crowding
        max_gen=4,
        leaf_limit=8,
        cord_max_bytes=1 << 20,
        rows_per_chunk=512,
        max_nodes_per_query=64,
        max_key_splits=4096,
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
    applied = apply_differences(SCHEMA, left, result.differences)
    peer = SnapshotStore.open(rp)
    try:
        assert commitment_of_map(SCHEMA, applied) == peer.identity.commitment
        assert len(applied) == peer.identity.row_count
    finally:
        peer.close()


def test_identical_rows_in_one_bucket_are_not_reported(tmp_path):
    # A small hand-built set that deliberately shares few buckets.
    left = [
        ("a", {"payload": "same", "amount": 1, "active": True, "note": None}),
        ("b", {"payload": "same", "amount": 1, "active": True, "note": None}),
        ("c", {"payload": "different", "amount": 2, "active": False, "note": "n"}),
    ]
    right = list(left)
    right[2] = ("c", {"payload": "different!", "amount": 2, "active": False, "note": "n"})
    lp = tmp_path / "l.sqlite"
    rp = tmp_path / "r.sqlite"
    create_snapshot(lp, SCHEMA, left, created_from="l")
    create_snapshot(rp, SCHEMA, right, created_from="r")
    left_store = SnapshotStore.open(lp)
    right_store = SnapshotStore.open(rp)
    try:
        outcome = run_session(
            left_store, right_store, params=SummaryParams(position_bits=1, max_gen=1)
        )
    finally:
        left_store.close()
        right_store.close()
    assert outcome.result.complete
    assert outcome.result.differences.counts() == {
        "only_left": 0,
        "only_right": 0,
        "changed": 1,
    }
    # With one bit there is a single split; the two identical business rows
    # must not be reported, and the single real change must be paired.
    from recon.encoding import decode_record, record_row_id_bytes

    changed_ids = {record_row_id_bytes(r).decode() for r in outcome.result.differences.changed_left}
    assert changed_ids == {"c"}
    _, values = decode_record(SCHEMA, outcome.result.differences.changed_left[0])
    assert values["payload"] == "different"
