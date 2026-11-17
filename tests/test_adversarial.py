"""Adversarial and identity tests.

These exercise the failure modes the protocol must handle explicitly rather
than silently: undecodable or oversize summaries, corrupted proofs, frames from
the wrong snapshot/session/endpoint, a commitment that cannot be reconciled,
replay of a whole session, and deliberate response reordering.
"""

from __future__ import annotations

import json
import threading
import uuid
from pathlib import Path

import pytest

from recon import data as demo_data
from recon.encoding import Schema, decode_record, encode_record, record_row_id_bytes
from recon.initiator import Initiator, ReconciliationError
from recon.runner import CallbackLink, MessageLog, run_session
from recon.responder import Responder
from recon.store import AnswerCache, SnapshotStore, create_snapshot
from recon.summary import (
    CORD_ENTRY,
    CORD_HEADER,
    SummaryParams,
    NodeId,
    Region,
    SummaryError,
    build_cord,
    decode_cord,
    node_proof,
    verify_proof,
)
from support import apply_differences, commitment_for, commitment_of_map, expected

SCHEMA = Schema.declare(
    ("payload", "text", False),
    ("amount", "int64", True),
    ("active", "bool", False),
    ("note", "text", True),
)


def _initiator_request(responder_session, peer_identity, own_identity, msg_id, request_id):
    """Build a request envelope as the initiator would, for rejection tests."""

    return {
        "v": 1,
        "kind": "summaries",
        "msg_id": msg_id,
        "session": responder_session.session,
        "from": "initiator",
        "to": "responder",
        "snapshot": peer_identity.to_wire(),
        "peer": own_identity.to_wire(),
        "negotiated": responder_session.negotiated,
        "request_id": request_id,
        "regions": [Region.root().to_wire()],
        "want": "aggregate",
    }


def _pair(tmp_path, name, left, right):
    lp = tmp_path / f"{name}-l.sqlite"
    rp = tmp_path / f"{name}-r.sqlite"
    create_snapshot(lp, SCHEMA, left, created_from="l")
    create_snapshot(rp, SCHEMA, right, created_from="r")
    return SnapshotStore.open(lp), SnapshotStore.open(rp)


# ---------------------------------------------------------------- summaries


def test_undecodable_cord_is_rejected():
    region = Region.root(16)
    bad = CORD_HEADER.pack(2) + CORD_ENTRY.pack(0, b"\x00" * 32)
    with pytest.raises(SummaryError):
        decode_cord(bad, region, expected_count=2)
    truncated = CORD_HEADER.pack(1)
    with pytest.raises(SummaryError):
        decode_cord(truncated, region)


def test_out_of_node_positions_are_rejected():
    region = Region(NodeId(gen=4, tag=0b0000, bits=8))
    # Position 0x80 is outside the 0x00 prefix node.
    cord = CORD_HEADER.pack(1) + CORD_ENTRY.pack(0x80, b"\x11" * 32)
    with pytest.raises(SummaryError):
        decode_cord(cord, region)


def test_cord_count_mismatch_is_rejected():
    region = Region.root(8)
    cord = CORD_HEADER.pack(1) + CORD_ENTRY.pack(0, b"\x11" * 32)
    with pytest.raises(SummaryError):
        decode_cord(cord, region, expected_count=2)


def test_cord_proof_binds_region_and_payload():
    region = Region(NodeId(gen=2, tag=1, bits=8))
    entries = [__import__("recon.summary", fromlist=["Entry"]).Entry(0x40, b"\x22" * 32)]
    cord = build_cord(entries)
    proof = node_proof("cord", region, cord).hex()
    verify_proof("cord", region, cord, proof)
    other_region = Region(NodeId(gen=2, tag=2, bits=8))
    with pytest.raises(SummaryError):
        verify_proof("cord", other_region, cord, proof)
    with pytest.raises(SummaryError):
        verify_proof("cord", region, cord + b"\x00", proof)
    with pytest.raises(SummaryError):
        verify_proof("summary", region, cord, proof)


def test_corrupted_summary_forces_progress_and_still_completes(tmp_path):
    """A peer that returns undecodable cords must not stop the reconciliation.

    The initiator narrows the region instead (hash split, then key split, then
    a logged exact fallback), and the final commitment still verifies.
    """

    rows = 800
    left = demo_data.base_rows(rows)
    right = demo_data.variant_sparse(rows).rows

    class CorruptingLink(CallbackLink):
        def send(self, message):
            if message["kind"] == "summaries" and message.get("want") == "cord":
                # Replace the request with one for a batch; the responder below
                # will corrupt the cord bytes in its answer.
                pass
            return super().send(message)

    left_store, right_store = _pair(tmp_path, "corrupt", left, right)
    log = MessageLog()
    cache = AnswerCache.open(tmp_path / "corrupt-answers.sqlite")
    to_r = []
    from_r = []
    init_link = CallbackLink("initiator", "responder", to_r.append)
    resp_link = CallbackLink("responder", "initiator", from_r.append)
    init = Initiator(
        "initiator", left_store, link=init_link, log=log, params=SummaryParams(
            leaf_limit=8, cord_max_bytes=4096, rows_per_chunk=256
        )
    )
    responder = Responder("responder", right_store, link=resp_link, log=log, cache=cache)

    # Wrap the responder's summary handler to corrupt every cord payload while
    # keeping the frame well-formed enough to reach the initiator.
    original = responder._respond

    def corrupt(session, request, kind, extra):
        if kind == "summaries_ack" and "results" in extra:
            for entry in extra["results"]:
                if entry.get("cord"):
                    raw = bytearray.fromhex(entry["cord"])
                    raw[-1] ^= 0xFF
                    entry["cord"] = bytes(raw).hex()
                    entry["cord_proof"] = "00" * 32
        return original(session, request, kind, extra)

    responder._respond = corrupt

    def pump():
        while True:
            item = init_link.inbox.get()
            if item is None:
                return
            responder.receive(item)
            while to_r:
                from_r.append(to_r.pop(0))

    # Simpler synchronous wiring: drive both engines on one thread via queues.
    to_resp = []
    to_init = []
    init_link = CallbackLink("initiator", "responder", to_resp.append)
    resp_link = CallbackLink("responder", "initiator", to_init.append)
    init.link = init_link
    responder.link = resp_link

    def step():
        while to_resp:
            responder.receive(to_resp.pop(0))
        while to_init:
            init_link.inbox.put(to_init.pop(0))

    import queue

    init_link.inbox = queue.Queue()

    def recv():
        while init_link.inbox.empty():
            step()
        return init_link.inbox.get()

    init_link.recv = recv
    result = init.run(uuid.uuid4().hex)
    assert result.complete, result.reason
    assert result.applied_commit == right_store.identity.commitment
    kinds = [n.action for n in result.region_notes]
    assert "cord_bad_proof" in kinds or "cord_undecodable" in kinds or "cord_rejected" in kinds
    cache.close()
    left_store.close()
    right_store.close()


def test_foreign_snapshot_frame_is_rejected(tmp_path):
    left = demo_data.base_rows(50)
    right = demo_data.variant_sparse(50).rows
    left_store, right_store = _pair(tmp_path, "foreign", left, right)
    other_path = tmp_path / "other.sqlite"
    create_snapshot(other_path, SCHEMA, left, created_from="other")
    other = SnapshotStore.open(other_path)
    log = MessageLog()
    cache = AnswerCache.open(tmp_path / "foreign-answers.sqlite")
    outbox = []
    resp_link = CallbackLink("responder", "initiator", outbox.append)
    responder = Responder("responder", right_store, link=resp_link, log=log, cache=cache)

    from recon.encoding import COMMIT_PARAMS, schema_wire
    from recon.summary import SummaryParams as SP
    from recon.wire import PeerSnapshot, decode_frame, encode_frame

    own = PeerSnapshot.from_identity(right_store.identity)
    # A handshake from the real peer establishes the pinned session.
    hello = {
        "v": 1, "kind": "hello", "msg_id": "h1", "session": "s1",
        "from": "initiator", "to": "responder", "snapshot": own.to_wire(),
        "peer": own.to_wire(), "negotiated": {}, "request_id": "h1",
        "schema": schema_wire(SCHEMA).hex(), "schema_sha": own.schema_sha.hex(),
        "negotiate": SP().to_wire(), "commit": dict(COMMIT_PARAMS),
    }
    responder.receive(encode_frame(hello))
    assert responder.session is not None
    outbox.clear()

    # A frame carrying a *different* snapshot identity is refused before any
    # region is touched.
    foreign = PeerSnapshot.from_identity(other.identity)
    message = _initiator_request(responder.session, own, own, "m1", "r1")
    message["snapshot"] = foreign.to_wire()
    responder.receive(encode_frame(message))
    rejected = [e for e in log.entries if e.get("event") == "request_rejected"]
    assert rejected, "a frame for another snapshot must be rejected"
    assert rejected[0]["code"] == "identity_snapshot"
    # The responder answers with exactly one explicit error frame and serves
    # no data for the rejected request.
    assert len(outbox) == 1
    error = decode_frame(outbox[0])
    assert error["kind"] == "error"
    assert error["code"] == "identity_snapshot"
    assert "records" not in error and "results" not in error

    # A frame addressed to a different receiver identity is also refused.
    before = len(log.entries)
    message = _initiator_request(responder.session, own, own, "m2", "r2")
    message["peer"] = foreign.to_wire()
    responder.receive(encode_frame(message))
    rejected = [e for e in log.entries[before:] if e.get("event") == "request_rejected"]
    assert rejected and rejected[0]["code"] == "identity_peer"
    assert len(outbox) == 2
    assert decode_frame(outbox[1])["kind"] == "error"
    cache.close()
    left_store.close()
    right_store.close()
    other.close()


def test_wrong_session_and_endpoint_are_rejected(tmp_path):
    rows = 20
    left = demo_data.base_rows(rows)
    right = demo_data.variant_sparse(rows).rows
    left_store, right_store = _pair(tmp_path, "wrongsession", left, right)
    log = MessageLog()
    cache = AnswerCache.open(tmp_path / "ws-answers.sqlite")
    outbox = []
    resp_link = CallbackLink("responder", "initiator", outbox.append)
    responder = Responder("responder", right_store, link=resp_link, log=log, cache=cache)
    from recon.summary import SummaryParams as SP
    from recon.encoding import COMMIT_PARAMS
    from recon.wire import PeerSnapshot, SessionContext, decode_frame, encode_frame

    own = PeerSnapshot.from_identity(right_store.identity)
    session = SessionContext(
        role="responder", peer_role="initiator", session="live",
        own=own, peer=own,
        negotiated={"schema": [], "commit": dict(COMMIT_PARAMS), "summary": SP().to_wire()},
    )
    # Establish the live session with a valid hello first.
    hello = {
        "v": 1, "kind": "hello", "msg_id": "h1", "session": "live",
        "from": "initiator", "to": "responder", "snapshot": own.to_wire(),
        "peer": own.to_wire(), "negotiated": {}, "request_id": "h1",
        "schema": __import__("recon.encoding", fromlist=["schema_wire"]).schema_wire(SCHEMA).hex(),
        "schema_sha": own.schema_sha.hex(),
        "negotiate": SP().to_wire(), "commit": dict(COMMIT_PARAMS),
    }
    responder.receive(encode_frame(hello))
    assert responder.session is not None
    outbox.clear()

    for mutate in (
        lambda m: m.update({"session": "stale"}),
        lambda m: m.update({"to": "someone-else"}),
        lambda m: m.update({"from": "someone-else"}),
    ):
        msg = _initiator_request(responder.session, own, own, "x1", "x1")
        mutate(msg)
        responder.receive(encode_frame(msg))
    codes = [e["code"] for e in log.entries if e.get("event") == "request_rejected"]
    assert "identity_session" in codes
    assert "identity_to" in codes
    assert "identity_from" in codes
    # Every rejection produced a single explicit error frame, never data.
    assert len(outbox) == 3
    for raw in outbox:
        frame = decode_frame(raw)
        assert frame["kind"] == "error"
        assert "records" not in frame and "results" not in frame
    cache.close()
    left_store.close()
    right_store.close()


def test_wrong_pinned_peer_identity_is_an_explicit_protocol_error(tmp_path):
    """If the initiator has pinned the wrong peer commitment, the peer's
    rejection must surface as an explicit protocol error, never a hang or a
    false success."""

    rows = 200
    left = demo_data.base_rows(rows)
    right = demo_data.variant_sparse(rows).rows
    left_store, right_store = _pair(tmp_path, "mismatch", left, right)
    log = MessageLog()
    cache = AnswerCache.open(tmp_path / "mm-answers.sqlite")
    to_resp: list[bytes] = []
    to_init: list[bytes] = []
    import queue

    init_link = CallbackLink("initiator", "responder", to_resp.append)
    resp_link = CallbackLink("responder", "initiator", to_init.append)
    init_link.inbox = queue.Queue()
    params = SummaryParams(leaf_limit=8)
    init = Initiator("initiator", left_store, link=init_link, log=log, params=params)
    responder = Responder("responder", right_store, link=resp_link, log=log, cache=cache)

    def step():
        while to_resp:
            responder.receive(to_resp.pop(0))
        while to_init:
            init_link.inbox.put(to_init.pop(0))

    def recv():
        while init_link.inbox.empty():
            step()
        return init_link.inbox.get()

    init_link.recv = recv
    from recon.wire import PeerSnapshot

    real_hello = init.hello

    def hello_then_lie(session_id):
        reply = real_hello(session_id)
        assert init.session is not None
        tampered = PeerSnapshot.from_wire(reply["snapshot"])
        object.__setattr__(
            init.session,
            "peer",
            PeerSnapshot(
                snapshot_id=tampered.snapshot_id,
                schema_sha=tampered.schema_sha,
                commitment=b"\x00" * 32,
                row_count=tampered.row_count,
                first_key=tampered.first_key,
                last_key=tampered.last_key,
            ),
        )
        return reply

    
    init.hello = hello_then_lie
    try:
        init.run(uuid.uuid4().hex)
    except ReconciliationError as exc:
        assert exc.code in ("identity_peer", "identity_snapshot", "protocol"), exc
    except Exception as exc:  # IdentityError is itself a ProtocolError
        from recon.wire import ProtocolError

        assert isinstance(exc, ProtocolError), exc
        assert getattr(exc, "code", "") in ("identity_peer", "identity_snapshot", "protocol")
    else:  # pragma: no cover - a false success here would be a real bug
        raise AssertionError("a wrong pinned peer identity must not reconcile successfully")
    # The responder logged the rejection rather than serving data.
    rejected = [e for e in log.entries if e.get("event") == "request_rejected"]
    assert rejected and rejected[0]["code"] in ("identity_peer", "identity_snapshot")
    cache.close()
    left_store.close()
    right_store.close()


def test_corrupted_detail_cannot_produce_false_success(tmp_path):
    """If the peer lies in its detail rows, the applied content cannot match
    the peer commitment.  The run must report incomplete with an explicit
    protocol error, never claim success."""

    rows = 300
    left = demo_data.base_rows(rows)
    right = demo_data.variant_sparse(rows).rows
    left_store, right_store = _pair(tmp_path, "lying", left, right)
    log = MessageLog()
    cache = AnswerCache.open(tmp_path / "lying-answers.sqlite")
    to_resp: list[bytes] = []
    to_init: list[bytes] = []
    import queue

    init_link = CallbackLink("initiator", "responder", to_resp.append)
    resp_link = CallbackLink("responder", "initiator", to_init.append)
    init_link.inbox = queue.Queue()
    params = SummaryParams(leaf_limit=8, rows_per_chunk=256)
    init = Initiator("initiator", left_store, link=init_link, log=log, params=params)
    responder = Responder("responder", right_store, link=resp_link, log=log, cache=cache)

    # Corrupt every detail record the peer sends: append a marker to its
    # payload.  The canonical row still decodes, so the lie reaches the
    # initiator's application step, where the commitment must catch it.
    original_send = resp_link.send
    tampered_count = [0]

    def lying_send(message):
        if message.get("kind") in ("probe_ack", "fallback_ack") and message.get("records"):
            replaced = []
            for hex_record in message["records"]:
                record = bytes.fromhex(hex_record)
                row_id, values = decode_record(SCHEMA, record)
                values = dict(values, payload=values["payload"] + "#LIE")
                replaced.append(encode_record(SCHEMA, row_id, values).hex())
                tampered_count[0] += 1
            message = dict(message)
            message["records"] = replaced
        return original_send(message)

    resp_link.send = lying_send

    def step():
        while to_resp:
            responder.receive(to_resp.pop(0))
        while to_init:
            init_link.inbox.put(to_init.pop(0))

    def recv():
        while init_link.inbox.empty():
            step()
        return init_link.inbox.get()

    init_link.recv = recv
    result = init.run(uuid.uuid4().hex)
    assert tampered_count[0] > 0, "the test must actually corrupt some detail rows"
    assert not result.complete
    assert result.reason.startswith("protocol_error")
    assert result.applied_commit != result.peer_commit
    cache.close()
    left_store.close()
    right_store.close()


def test_session_replay_returns_identical_answers(tmp_path):
    from recon.runner import ReplaySession

    rows = 400
    left = demo_data.base_rows(rows)
    right = demo_data.variant_sparse(rows).rows
    left_store, right_store = _pair(tmp_path, "replay", left, right)
    cache_path = tmp_path / "replay-answers.sqlite"
    params = SummaryParams(leaf_limit=8, rows_per_chunk=256)

    session_id = "fixed-session-for-replay"
    first_session = ReplaySession(left_store, right_store, params=params, cache_path=cache_path)
    first = first_session.run(session_id)
    stats_one = first_session.responder.stats.to_dict()
    first_session.close()

    second_session = ReplaySession(left_store, right_store, params=params, cache_path=cache_path)
    second = second_session.run(session_id)
    stats_two = second_session.responder.stats.to_dict()
    second_session.close()

    assert first.complete and second.complete
    assert first.differences.counts() == second.differences.counts()
    assert first.applied_commit == second.applied_commit
    # The second run was served from the persisted answer cache: no rows were
    # recomputed and no summaries were rebuilt.
    assert stats_two["cache_hits"] > 0
    assert stats_two["requests"] == stats_one["requests"]
    assert stats_two["summaries_served"] == 0
    assert stats_two["probe_pages_served"] == 0
    assert stats_two["fallback_pages_served"] == 0
    left_store.close()
    right_store.close()
