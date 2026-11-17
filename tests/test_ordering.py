"""Response ordering and envelope identity tests.

The transport is synchronous, but a real network can deliver an older answer
after a newer request.  The initiator must attribute answers by identity
(request id + request message id), never by arrival order.
"""

from __future__ import annotations

import queue
import uuid
from pathlib import Path

import pytest

from recon import data as demo_data
from recon.encoding import Schema
from recon.wire import decode_frame
from recon.initiator import Initiator
from recon.responder import Responder
from recon.runner import CallbackLink, MessageLog
from recon.store import AnswerCache, SnapshotStore, create_snapshot
from recon.summary import SummaryParams
from support import expected

SCHEMA = Schema.declare(
    ("payload", "text", False),
    ("amount", "int64", True),
    ("active", "bool", False),
    ("note", "text", True),
)


def test_out_of_order_answers_are_attributed_by_identity(tmp_path):
    rows = 400
    left = demo_data.base_rows(rows)
    right = demo_data.variant_sparse(rows).rows
    lp = tmp_path / "l.sqlite"
    rp = tmp_path / "r.sqlite"
    create_snapshot(lp, SCHEMA, left, created_from="l")
    create_snapshot(rp, SCHEMA, right, created_from="r")
    left_store = SnapshotStore.open(lp)
    right_store = SnapshotStore.open(rp)
    log = MessageLog()
    cache = AnswerCache.open(tmp_path / "answers.sqlite")

    to_resp: list[bytes] = []
    to_init: "queue.Queue[bytes]" = queue.Queue()
    init_link = CallbackLink("initiator", "responder", to_resp.append)
    init_link.inbox = to_init
    resp_link = CallbackLink("responder", "initiator", lambda raw: None)
    params = SummaryParams(leaf_limit=8, rows_per_chunk=256)
    init = Initiator("initiator", left_store, link=init_link, log=log, params=params)
    responder = Responder("responder", right_store, link=resp_link, log=log, cache=cache)

    produced: list[bytes] = []
    resp_link.send = lambda message: produced.append(
        __import__("recon.wire", fromlist=["encode_frame"]).encode_frame(message)
    ) or produced[-1]

    # Deliver every answer twice and, from the third answer onward, also
    # re-deliver the *previous* answer before it.  Earlier answers are stale by
    # request id, so the initiator must ignore them by identity.
    delivered = 0
    previous: bytes | None = None

    def pump() -> None:
        nonlocal delivered, previous
        while to_resp:
            responder.receive(to_resp.pop(0))
        while produced:
            raw = produced.pop(0)
            delivered += 1
            if previous is not None and delivered % 2 == 0:
                to_init.put(previous)   # stale, arrives out of order
            to_init.put(raw)
            to_init.put(raw)            # duplicate delivery
            previous = raw

    def recv():
        while to_init.empty():
            pump()
        return to_init.get()

    init_link.recv = recv
    result = init.run(uuid.uuid4().hex)

    assert result.complete, result.reason
    exp = expected(left, right, SCHEMA)
    assert result.differences.counts() == exp.counts() == {
        "only_left": 1,
        "only_right": 1,
        "changed": 1,
    }
    assert result.ignored_frames + result.duplicate_answers_ignored > 0
    cache.close()
    left_store.close()
    right_store.close()


def test_every_message_carries_full_envelope_identity(tmp_path):
    rows = 200
    left = demo_data.base_rows(rows)
    right = demo_data.variant_sparse(rows).rows
    lp = tmp_path / "l.sqlite"
    rp = tmp_path / "r.sqlite"
    create_snapshot(lp, SCHEMA, left, created_from="l")
    create_snapshot(rp, SCHEMA, right, created_from="r")
    left_store = SnapshotStore.open(lp)
    right_store = SnapshotStore.open(rp)
    log = MessageLog()

    frames: list[bytes] = []
    to_resp: list[bytes] = []
    to_init: "queue.Queue[bytes]" = queue.Queue()
    resp_frames: list[bytes] = []
    init_link = CallbackLink("initiator", "responder", lambda raw: (frames.append(raw), to_resp.append(raw)))
    init_link.inbox = to_init
    resp_link = CallbackLink("responder", "initiator", lambda raw: (frames.append(raw), resp_frames.append(raw)))
    params = SummaryParams(leaf_limit=8)
    init = Initiator("initiator", left_store, link=init_link, log=log, params=params)
    responder = Responder(
        "responder", right_store, link=resp_link, log=log,
        cache=AnswerCache.open(tmp_path / "a.sqlite"),
    )

    def pump() -> None:
        while to_resp:
            responder.receive(to_resp.pop(0))
        while resp_frames:
            to_init.put(resp_frames.pop(0))

    def recv():
        while to_init.empty():
            pump()
        return to_init.get()

    init_link.recv = recv
    result = init.run(uuid.uuid4().hex)
    assert result.complete

    session_id = None
    roles = set()
    for raw in frames:
        message = decode_frame(raw)
        roles.add(message["from"])
        assert message["snapshot"]["id"]
        assert message["peer"]["id"]
        assert message["session"]
        # The hello frame predates negotiation and carries an empty set; every
        # later frame carries the full negotiated parameters.
        if message["kind"] != "hello":
            assert message["negotiated"]
        else:
            assert message["negotiated"] == {}
        if message["kind"] != "hello":
            assert message["request_id"]
        if message["kind"] in (
            "hello_ack", "summaries_ack", "probe_ack", "fallback_ack", "error"
        ):
            assert "request_msg_id" in message
        session_id = session_id or message["session"]
        assert message["session"] == session_id
    assert roles == {"initiator", "responder"}
    left_store.close()
    right_store.close()
