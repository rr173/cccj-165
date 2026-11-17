"""Two-process tests: genuinely separate OS processes, byte-only relay.

These assert the task's isolation property: each worker receives only its own
snapshot path, and the supervisor relays opaque frames.  They also verify the
100k-row sparse case end to end against the independent oracle.
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

from recon import data as demo_data
from recon.encoding import Schema, record_row_id_bytes
from recon.store import SnapshotStore, create_snapshot
from recon.summary import SummaryParams
from recon.twoprocess import run_two_process
from support import commitment_for, expected

SCHEMA = Schema.declare(
    ("payload", "text", False),
    ("amount", "int64", True),
    ("active", "bool", False),
    ("note", "text", True),
)


def test_workers_receive_only_their_own_path():
    """The launcher passes exactly one snapshot path per process."""

    from recon import twoprocess

    captured = {}

    class FakeProc:
        stdin = None
        stdout = None
        stderr = None

        def wait(self, timeout=None):
            return 0

    def fake_popen(argv, **kwargs):
        # argv layout: [python, -I, -c, BOOT, mode, snapshot, ...]
        captured.setdefault(mode_of(argv), []).append(argv)
        return FakeProc()

    original = subprocess.Popen
    try:
        subprocess.Popen = fake_popen
        twoprocess._launch(["initiator", "/tmp/only-left.sqlite", "initiator", "responder"])
        twoprocess._launch(["responder", "/tmp/only-right.sqlite", "/tmp/cache.sqlite", "responder"])
    finally:
        subprocess.Popen = original
    assert captured["initiator"][0][5] == "/tmp/only-left.sqlite"
    assert captured["responder"][0][5] == "/tmp/only-right.sqlite"
    # Neither invocation mentions the other side's snapshot path.
    assert all("/tmp/only-right.sqlite" not in part for part in captured["initiator"][0])
    assert all("/tmp/only-left.sqlite" not in part for part in captured["responder"][0])


def mode_of(argv):
    return argv[4]


def test_two_process_sparse_100k(tmp_path):
    rows = 100_000
    left = demo_data.base_rows(rows)
    right = demo_data.variant_sparse(rows).rows
    lp = tmp_path / "left.sqlite"
    rp = tmp_path / "right.sqlite"
    create_snapshot(lp, SCHEMA, left, created_from="l")
    create_snapshot(rp, SCHEMA, right, created_from="r")

    outcome = run_two_process(
        lp,
        rp,
        params=SummaryParams(leaf_limit=64, rows_per_chunk=4096),
        answer_cache=tmp_path / "answers.sqlite",
        timeout=900,
    )
    assert not outcome.timed_out
    assert outcome.returncodes == (0, 0), (outcome.stderr_initiator, outcome.stderr_responder)
    result = outcome.initiator_final["result"]
    assert result["complete"], result["reason"]
    exp = expected(left, right, SCHEMA)
    assert result["differences"] == exp.counts()
    assert result["differences"] == {"only_left": 1, "only_right": 1, "changed": 1}
    # The peer's original on-disk commitment is reproduced exactly.
    right_store = SnapshotStore.open(rp)
    try:
        assert bytes.fromhex(result["applied_commit"]) == right_store.identity.commitment
    finally:
        right_store.close()
    # Wire volume is far below either full snapshot's canonical size.
    total = (
        outcome.initiator_to_responder.bytes + outcome.responder_to_initiator.bytes
    )
    canonical_peer = sum(len(r) for r in SnapshotStore.open(rp).iter_records())
    assert total < canonical_peer // 10, (total, canonical_peer)
    assert result["fallbacks"] == 0
    # Per-direction byte counts agree with the relay's own tally.
    assert outcome.initiator_to_responder.bytes == result["sent_bytes"]
    assert outcome.responder_to_initiator.bytes + sum(
        m["bytes"] for m in outcome.messages if m.get("duplicate")
    ) == result["received_bytes"]


def test_two_process_duplicate_delivery(tmp_path):
    rows = 1_000
    left = demo_data.base_rows(rows)
    right = demo_data.variant_sparse(rows).rows
    lp = tmp_path / "left.sqlite"
    rp = tmp_path / "right.sqlite"
    create_snapshot(lp, SCHEMA, left, created_from="l")
    create_snapshot(rp, SCHEMA, right, created_from="r")
    outcome = run_two_process(
        lp,
        rp,
        params=SummaryParams(leaf_limit=16),
        duplicate_answers=5,
        answer_cache=tmp_path / "answers.sqlite",
        timeout=900,
    )
    assert not outcome.timed_out
    result = outcome.initiator_final["result"]
    assert result["complete"], result["reason"]
    assert result["differences"] == {"only_left": 1, "only_right": 1, "changed": 1}
    assert result["duplicate_answers_ignored"] == 5


def test_two_process_identical_and_empty(tmp_path):
    left = demo_data.base_rows(2_000)
    lp = tmp_path / "left.sqlite"
    rp = tmp_path / "right.sqlite"
    create_snapshot(lp, SCHEMA, left, created_from="l")
    create_snapshot(rp, SCHEMA, left, created_from="r")
    outcome = run_two_process(
        lp, rp, params=SummaryParams(), answer_cache=tmp_path / "a1.sqlite", timeout=600
    )
    result = outcome.initiator_final["result"]
    assert result["complete"]
    assert result["differences"] == {"only_left": 0, "only_right": 0, "changed": 0}
    assert outcome.initiator_to_responder.bytes + outcome.responder_to_initiator.bytes < 20_000

    ep_l = tmp_path / "empty-l.sqlite"
    ep_r = tmp_path / "empty-r.sqlite"
    create_snapshot(ep_l, SCHEMA, [], created_from="l")
    create_snapshot(ep_r, SCHEMA, [], created_from="r")
    empty = run_two_process(
        ep_l, ep_r, params=SummaryParams(), answer_cache=tmp_path / "a2.sqlite", timeout=600
    )
    empty_result = empty.initiator_final["result"]
    assert empty_result["complete"]
    assert empty_result["differences"] == {"only_left": 0, "only_right": 0, "changed": 0}


def test_two_process_dense_with_tiny_summary(tmp_path):
    rows = 3_000
    left = demo_data.base_rows(rows)
    right = demo_data.variant_dense(rows, seed=13).rows
    lp = tmp_path / "left.sqlite"
    rp = tmp_path / "right.sqlite"
    create_snapshot(lp, SCHEMA, left, created_from="l")
    create_snapshot(rp, SCHEMA, right, created_from="r")
    outcome = run_two_process(
        lp,
        rp,
        params=SummaryParams(
            position_bits=8,
            max_gen=8,
            leaf_limit=4,
            cord_max_bytes=48,
            rows_per_chunk=256,
            max_nodes_per_query=64,
        ),
        answer_cache=tmp_path / "answers.sqlite",
        timeout=900,
    )
    assert not outcome.timed_out
    result = outcome.initiator_final["result"]
    assert result["complete"], result["reason"]
    exp = expected(left, right, SCHEMA)
    assert result["differences"] == exp.counts()
    assert result["fallbacks"] > 0
    right_store = SnapshotStore.open(rp)
    try:
        assert bytes.fromhex(result["applied_commit"]) == right_store.identity.commitment
    finally:
        right_store.close()
