"""Initiator engine: discover, verify, and apply the difference set.

The initiator reads only its own fixed snapshot.  It never receives the peer's
whole table unless an explicitly named *region* falls back to exact comparison.

Phases
------
1. **Handshake** — exchange schemas, pin both snapshot identities, negotiate
   codec, commitment parameters, and a configured summary budget.  The budget
   is a configured value; it is never derived from the data or its differences.
2. **Locate** — walk the key-hash tree:
   * Compare compact aggregates.  Equal aggregates are treated as equal *only*
     provisionally; the final commitment check is authoritative.
   * A region that differs and is large is split: by hash prefix first, then by
     the peer-reported row-key median.  Each decision and reason is logged.
   * A region small enough is compared by cord, which names the exact hash
     positions whose fingerprints differ.
   * A cord that is oversize, undecodable, or fails its proof triggers explicit
     narrowing (hash split, then key split), and only when neither can make
     progress does the region fall back to transferring its actual rows.  The
     fallback names the region and is logged; it is never the whole table by
     accident.
   * Candidate positions are resolved by paged ``probe`` requests that fetch
     the peer's actual canonical records.  Verification is by row id and
     canonical bytes; summary digests never write data.
3. **Apply and verify** — reconstruct the peer's content locally in a temporary
   structure (in-memory map plus a temporary SQLite table), recompute its
   commitment, and require exact equality with the peer's pinned commitment.
   If it differs, run an *exhaustive* locate pass that ignores provisional
   aggregate-equality skips, then verify again.  A remaining mismatch is
   reported as incomplete with an explicit protocol error, never as success.
"""

from __future__ import annotations

import os
import sqlite3
import struct
import tempfile
from dataclasses import dataclass, field
from typing import Any, Iterable, Mapping, Sequence

from . import encoding, summary
from .encoding import COMMIT_PARAMS, decode_record, record_row_id_bytes
from .store import SnapshotStore, parse_schema_wire
from .summary import (
    LocalIndex,
    Region,
    SummaryError,
    SummaryParams,
    compare_cords,
    decode_cord,
)
from .wire import (
    PeerSnapshot,
    ProtocolError,
    SessionContext,
    decode_frame,
    encode_frame,
    sha256,
    validate_frame,
)


class ReconciliationError(RuntimeError):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(f"{code}: {message}")
        self.code = code
        self.message = message


@dataclass
class DifferenceSet:
    only_left: list[bytes] = field(default_factory=list)
    only_right: list[bytes] = field(default_factory=list)
    changed_left: list[bytes] = field(default_factory=list)
    changed_right: list[bytes] = field(default_factory=list)

    def counts(self) -> dict[str, int]:
        return {
            "only_left": len(self.only_left),
            "only_right": len(self.only_right),
            "changed": len(self.changed_left),
        }

    def peer_side_records(self) -> list[bytes]:
        """The peer's records for every confirmed difference."""

        return list(self.only_right) + list(self.changed_right)

    def left_side_records(self) -> list[bytes]:
        return list(self.only_left) + list(self.changed_left)


@dataclass
class RegionNote:
    region: str
    action: str
    reason: str
    rows: int | None = None


@dataclass
class RunResult:
    complete: bool
    reason: str
    differences: DifferenceSet
    self_commit: bytes
    peer_commit: bytes
    applied_commit: bytes
    sent_bytes: int
    received_bytes: int
    sent_frames: int
    received_frames: int
    sent_by_kind: dict[str, int]
    received_by_kind: dict[str, int]
    region_notes: list[RegionNote]
    probes: int
    fallbacks: int
    fallback_regions: list[str]
    summary_requests: int
    summary_regions: int
    cord_requests: int
    cord_bytes: int
    probe_rows: int
    fallback_rows: int
    handshake_notes: list[str]
    duplicate_answers_ignored: int
    ignored_frames: int
    temp_verify_rows: int
    exhaustive_rounds: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "complete": self.complete,
            "reason": self.reason,
            "differences": self.differences.counts(),
            "self_commit": self.self_commit.hex(),
            "peer_commit": self.peer_commit.hex(),
            "applied_commit": self.applied_commit.hex(),
            "sent_bytes": self.sent_bytes,
            "received_bytes": self.received_bytes,
            "sent_frames": self.sent_frames,
            "received_frames": self.received_frames,
            "sent_by_kind": dict(self.sent_by_kind),
            "received_by_kind": dict(self.received_by_kind),
            "region_notes": [n.__dict__ for n in self.region_notes],
            "probes": self.probes,
            "fallbacks": self.fallbacks,
            "fallback_regions": list(self.fallback_regions),
            "summary_requests": self.summary_requests,
            "summary_regions": self.summary_regions,
            "cord_requests": self.cord_requests,
            "cord_bytes": self.cord_bytes,
            "probe_rows": self.probe_rows,
            "fallback_rows": self.fallback_rows,
            "handshake_notes": self.handshake_notes,
            "duplicate_answers_ignored": self.duplicate_answers_ignored,
            "ignored_frames": self.ignored_frames,
            "temp_verify_rows": self.temp_verify_rows,
            "exhaustive_rounds": self.exhaustive_rounds,
        }


@dataclass
class WorkItem:
    region: Region
    key_splits: int = 0


class Initiator:
    def __init__(
        self,
        role: str,
        store: SnapshotStore,
        *,
        link,
        log,
        params: SummaryParams,
        verify_temp_sqlite: bool = True,
    ) -> None:
        self.role = role
        self.store = store
        self.identity = store.identity
        self.schema = store.schema
        self.link = link
        self.peer_role = getattr(link, "peer_role", "peer")
        self.log = log
        self.params = params
        self.verify_temp_sqlite = verify_temp_sqlite
        self.index = LocalIndex(store.iter_records(), params.position_bits)
        self.session: SessionContext | None = None
        self._msg_counter = 0
        self._request_counter = 0
        self._seen_answers: set[str] = set()
        self.sent_bytes = 0
        self.received_bytes = 0
        self.sent_frames = 0
        self.received_frames = 0
        self.sent_by_kind: dict[str, int] = {}
        self.received_by_kind: dict[str, int] = {}
        self.duplicate_answers = 0
        self.ignored_frames = 0
        self.summary_requests = 0
        self.summary_regions = 0
        self.cord_requests = 0
        self.cord_bytes = 0
        self.probes = 0
        self.probe_rows = 0
        self.fallbacks = 0
        self.fallback_rows = 0
        self.fallback_regions: list[str] = []

    # ------------------------------------------------------------ transport

    def _msg_id(self, kind: str) -> str:
        self._msg_counter += 1
        return f"{self.role}-{kind}-{self._msg_counter:07d}"

    def _request_id(self, tag: str) -> str:
        self._request_counter += 1
        return f"{self.role}-{tag}-{self._request_counter:07d}"

    def _record_sent(self, message: Mapping[str, Any], size: int) -> None:
        self.sent_frames += 1
        self.sent_bytes += size
        kind = str(message["kind"])
        self.sent_by_kind[kind] = self.sent_by_kind.get(kind, 0) + size
        self.log.message_sent(message, size)

    def _record_received(self, message: Mapping[str, Any], size: int) -> None:
        self.received_frames += 1
        self.received_bytes += size
        kind = str(message["kind"])
        self.received_by_kind[kind] = self.received_by_kind.get(kind, 0) + size
        self.log.message_received(message, size)

    def _await_answer(self, rid: str, mid: str) -> dict[str, Any]:
        assert self.session is not None
        while True:
            raw = self.link.recv()
            if raw is None:
                raise ReconciliationError("eof", f"transport closed while awaiting {rid}")
            try:
                reply = decode_frame(raw)
            except ProtocolError as exc:
                self.ignored_frames += 1
                self.log.event("foreign_frame_rejected", code=exc.code, detail=exc.message)
                raise ReconciliationError(exc.code, exc.message) from exc
            validate_frame(self.session, reply)
            if reply.get("request_id") != rid:
                # A byte-identical re-delivery of an answer already consumed is
                # a duplicate, not a stale cross-request frame.  Either way it
                # is ignored; the distinction is recorded for the audit trail.
                if reply["msg_id"] in self._seen_answers:
                    self.duplicate_answers += 1
                    self.log.event(
                        "duplicate_answer_ignored",
                        kind=reply["kind"],
                        msg_id=reply["msg_id"],
                        request_id=reply.get("request_id"),
                    )
                else:
                    self.ignored_frames += 1
                    self.log.event(
                        "stale_answer_ignored",
                        kind=reply["kind"],
                        request_id=reply.get("request_id"),
                        awaiting=rid,
                    )
                continue
            if reply.get("request_msg_id") != mid:
                self.ignored_frames += 1
                self.log.event(
                    "answer_identity_mismatch",
                    kind=reply["kind"],
                    request_id=rid,
                    detail=reply.get("request_msg_id"),
                )
                continue
            if reply["msg_id"] in self._seen_answers:
                self.duplicate_answers += 1
                self.log.event(
                    "duplicate_answer_ignored", kind=reply["kind"], msg_id=reply["msg_id"]
                )
                continue
            if reply["kind"] == "error":
                raise ReconciliationError(
                    str(reply.get("code", "peer_error")),
                    str(reply.get("message", "peer reported an error")),
                )
            self._seen_answers.add(reply["msg_id"])
            self._record_received(reply, len(raw))
            return reply

    def call(self, kind: str, extra: Mapping[str, Any], tag: str) -> dict[str, Any]:
        assert self.session is not None
        rid = self._request_id(tag)
        mid = self._msg_id(kind)
        message = self.session.envelope(kind, mid, request_id=rid, extra=extra)
        raw = encode_frame(message)
        self._record_sent(message, len(raw))
        self.link.send(message)
        return self._await_answer(rid, mid)

    # ------------------------------------------------------------ handshake

    def hello(self, session_id: str) -> dict[str, Any]:
        own = PeerSnapshot.from_identity(self.identity)
        rid = self._request_id("hello")
        mid = self._msg_id("hello")
        message = {
            "v": 1,
            "kind": "hello",
            "msg_id": mid,
            "session": session_id,
            "from": self.role,
            "to": self.peer_role,
            "snapshot": own.to_wire(),
            "peer": own.to_wire(),
            "negotiated": {},
            "request_id": rid,
            "schema": encoding.schema_wire(self.schema).hex(),
            "schema_sha": own.schema_sha.hex(),
            "negotiate": self.params.to_wire(),
            "commit": dict(COMMIT_PARAMS),
        }
        raw = encode_frame(message)
        self._record_sent(message, len(raw))
        self.link.send(message)

        while True:
            incoming = self.link.recv()
            if incoming is None:
                raise ReconciliationError("eof", "transport closed during handshake")
            reply = decode_frame(incoming)
            if reply["kind"] == "error":
                if reply.get("request_id") == rid:
                    raise ReconciliationError(
                        str(reply.get("code", "peer_error")),
                        str(reply.get("message", "handshake rejected")),
                    )
                if reply["msg_id"] in self._seen_answers:
                    self.duplicate_answers += 1
                    self.log.event("duplicate_answer_ignored", kind="error", msg_id=reply["msg_id"])
                else:
                    self.ignored_frames += 1
                    self.log.event("stale_answer_ignored", kind="error", awaiting=rid)
                continue
            if reply["kind"] != "hello_ack":
                self.ignored_frames += 1
                self.log.event("unexpected_frame", kind=reply["kind"])
                continue
            if reply.get("request_id") != rid:
                if reply["msg_id"] in self._seen_answers:
                    self.duplicate_answers += 1
                    self.log.event(
                        "duplicate_answer_ignored", kind="hello_ack", msg_id=reply["msg_id"]
                    )
                else:
                    self.ignored_frames += 1
                    self.log.event("stale_answer_ignored", kind="hello_ack", awaiting=rid)
                continue
            if reply.get("request_msg_id") != mid:
                self.ignored_frames += 1
                self.log.event("answer_identity_mismatch", kind=reply["kind"])
                continue
            if reply["msg_id"] in self._seen_answers:
                self.duplicate_answers += 1
                self.log.event(
                    "duplicate_answer_ignored", kind=reply["kind"], msg_id=reply["msg_id"]
                )
                continue
            peer_snap = PeerSnapshot.from_wire(reply["snapshot"])
            peer_schema = parse_schema_wire(bytes.fromhex(reply["schema"]))
            if peer_snap.schema_sha != sha256(encoding.schema_wire(peer_schema)):
                raise ReconciliationError("schema_hash", "peer schema disagrees with its hash")
            if peer_schema.signature() != self.schema.signature():
                raise ReconciliationError(
                    "schema_mismatch",
                    f"peer schema {peer_schema.signature()} differs from local "
                    f"{self.schema.signature()}",
                )
            session = SessionContext(
                role=self.role,
                peer_role=self.peer_role,
                session=session_id,
                own=own,
                peer=peer_snap,
                negotiated=dict(reply["negotiated"]),
            )
            validate_frame(session, reply)
            self.session = session
            self._seen_answers.add(reply["msg_id"])
            self._record_received(reply, len(incoming))
            return reply

    # ---------------------------------------------------------------- run

    def run(self, session_id: str) -> RunResult:
        notes: list[RegionNote] = []
        handshake_notes: list[str] = []
        hello = self.hello(session_id)
        assert self.session is not None
        negotiated = dict(hello["negotiated"])
        if dict(negotiated.get("commit", {})) != dict(COMMIT_PARAMS):
            raise ReconciliationError(
                "commit_mismatch", "peer negotiated different commitment parameters"
            )
        peer_params = SummaryParams.from_wire(dict(negotiated["summary"]))
        if peer_params != self.params:
            raise ReconciliationError(
                "summary_mismatch", "peer negotiated a different summary budget"
            )
        if dict(negotiated.get("summary")) != self.params.to_wire():
            raise ReconciliationError("summary_mismatch", "summary budget echo disagrees")
        handshake_notes.append(f"schema pinned: {negotiated.get('schema')}")
        handshake_notes.append(f"summary budget: {peer_params.to_wire()}")
        handshake_notes.append(
            f"peer snapshot {self.session.peer.snapshot_id[:16]} rows={self.session.peer.row_count}"
        )

        root = Region.root(self.params.position_bits)
        peer_root = self._fetch_summaries([root])[0]
        local_root = self.index.node_aggregate(root)

        differences = DifferenceSet()
        exhaustive_rounds = 0
        applied = b""
        temp_rows = 0

        if local_root.equals(peer_root.aggregate()):
            differences = DifferenceSet()
            applied, temp_rows = self._apply_and_verify(differences)
            if applied == self.session.peer.commitment:
                return self._finish(
                    True, "identical", differences, notes, handshake_notes, applied,
                    temp_rows, exhaustive_rounds,
                )
            notes.append(
                RegionNote(
                    root.label(),
                    "root_summary_collision",
                    "root aggregates matched but commitments differ; descending exhaustively",
                    local_root.count,
                )
            )

        differences, applied, temp_rows = self._locate_and_apply(notes, force=False)
        if applied != self.session.peer.commitment:
            exhaustive_rounds = 1
            notes.append(
                RegionNote(
                    root.label(),
                    "exhaustive_retry",
                    "commitment mismatch after normal pass; ignoring aggregate skips",
                    len(self.index),
                )
            )
            differences, applied, temp_rows = self._locate_and_apply(notes, force=True)
            if applied != self.session.peer.commitment:
                return self._finish(
                    False,
                    "protocol_error: applied content does not match the peer commitment "
                    "even after exhaustive comparison",
                    differences,
                    notes,
                    handshake_notes,
                    applied,
                    temp_rows,
                    exhaustive_rounds,
                )
        return self._finish(
            True, "ok", differences, notes, handshake_notes, applied, temp_rows,
            exhaustive_rounds,
        )

    def _finish(self, complete, reason, differences, notes, handshake_notes, applied,
                temp_rows, exhaustive_rounds) -> RunResult:
        assert self.session is not None
        return RunResult(
            complete=complete,
            reason=reason,
            differences=differences,
            self_commit=self.identity.commitment,
            peer_commit=self.session.peer.commitment,
            applied_commit=applied,
            sent_bytes=self.sent_bytes,
            received_bytes=self.received_bytes,
            sent_frames=self.sent_frames,
            received_frames=self.received_frames,
            sent_by_kind=dict(self.sent_by_kind),
            received_by_kind=dict(self.received_by_kind),
            region_notes=notes,
            probes=self.probes,
            fallbacks=self.fallbacks,
            fallback_regions=list(self.fallback_regions),
            summary_requests=self.summary_requests,
            summary_regions=self.summary_regions,
            cord_requests=self.cord_requests,
            cord_bytes=self.cord_bytes,
            probe_rows=self.probe_rows,
            fallback_rows=self.fallback_rows,
            handshake_notes=handshake_notes,
            duplicate_answers_ignored=self.duplicate_answers,
            ignored_frames=self.ignored_frames,
            temp_verify_rows=temp_rows,
            exhaustive_rounds=exhaustive_rounds,
        )

    def _locate_and_apply(self, notes, *, force: bool):
        left_only: dict[bytes, bytes] = {}
        right_only: dict[bytes, bytes] = {}
        changed: dict[bytes, tuple[bytes, bytes]] = {}
        self._locate(notes, left_only, right_only, changed, force=force)
        differences = DifferenceSet()
        differences.only_left = sorted(left_only.values(), key=record_row_id_bytes)
        differences.only_right = sorted(right_only.values(), key=record_row_id_bytes)
        for key in sorted(changed):
            mine, theirs = changed[key]
            differences.changed_left.append(mine)
            differences.changed_right.append(theirs)
        applied, temp_rows = self._apply_and_verify(differences)
        return differences, applied, temp_rows

    def _locate(self, notes, left_only, right_only, changed, *, force: bool) -> None:
        """Breadth-first refinement with one batched round per level at most.

        Each round: fetch summaries for the frontier, classify regions, and
        fetch all cords for the cord-eligible ones in a *single* batched
        request.  That keeps round trips proportional to the tree depth, not to
        the number of differing leaves.
        """

        frontier: list[WorkItem] = [WorkItem(Region.root(self.params.position_bits))]
        while frontier:
            summaries = self._fetch_summaries([item.region for item in frontier])
            next_frontier: list[WorkItem] = []
            cord_items: list[tuple[WorkItem, summary.NodeSummary]] = []
            for item, peer_summary in zip(frontier, summaries):
                decision = self._classify(
                    item, peer_summary, notes, force=force
                )
                if decision is None:
                    continue
                kind, payload = decision
                if kind == "split":
                    next_frontier.extend(payload)
                elif kind == "cord":
                    cord_items.append((item, peer_summary))
                elif kind == "fallback":
                    self._fallback(
                        payload, notes, left_only, right_only, changed
                    )
                elif kind == "local_only":
                    for row in self.index.node_slice(payload):
                        left_only[row.row_key] = row.record
            if cord_items:
                self._resolve_cords(
                    cord_items, next_frontier, notes,
                    left_only, right_only, changed, force=force,
                )
            frontier = next_frontier

    def _classify(self, item, peer_summary, notes, *, force: bool):
        """Return ('split', items) | ('cord', None) | ('fallback', None) | None."""

        region = item.region
        local_agg = self.index.node_aggregate(region)
        peer_agg = peer_summary.aggregate()
        if local_agg.equals(peer_agg) and not force:
            notes.append(RegionNote(region.label(), "skip", "summary_equal", local_agg.count))
            return None
        larger = max(local_agg.count, peer_summary.count)
        if larger == 0:
            notes.append(RegionNote(region.label(), "skip", "region_empty", 0))
            return None
        if peer_summary.count == 0:
            # The peer's own summary asserts there are no rows in this region,
            # so every local row here is only-left.  This is a *decision*, not a
            # mutation: the caller records them.  The final commitment check
            # still verifies that the peer told the truth.
            rows = self.index.node_slice(region)
            notes.append(
                RegionNote(region.label(), "local_only", "peer_region_empty", len(rows))
            )
            return ("local_only", region)

        if larger > self.params.leaf_limit:
            if region.node.gen < int(self.params.max_gen):
                notes.append(
                    RegionNote(
                        region.label(),
                        "hash_split",
                        f"summary_differs rows={local_agg.count}/{peer_summary.count}",
                        larger,
                    )
                )
                return ("split", [WorkItem(c, item.key_splits) for c in region.hash_children()])
            pivot = peer_summary.median
            if (
                item.key_splits < self.params.max_key_splits
                and pivot is not None
                and self._progressing_key_split(region, pivot)
            ):
                left, right = region.key_children(pivot)
                notes.append(
                    RegionNote(
                        region.label(),
                        "key_split",
                        f"hash_generation_exhausted rows={local_agg.count}/{peer_summary.count}",
                        larger,
                    )
                )
                return (
                    "split",
                    [WorkItem(left, item.key_splits + 1), WorkItem(right, item.key_splits + 1)],
                )
            return ("fallback", region)

        # Leaf: if even an empty cord would exceed the byte budget, the summary
        # is unusable by configuration and the region goes to exact narrowing
        # immediately.  This is what makes a deliberately tiny budget exercise
        # the fallback path rather than silently succeeding.
        if summary.CORD_HEADER.size + larger * summary.CORD_ENTRY_BYTES > self.params.cord_max_bytes:
            notes.append(
                RegionNote(
                    region.label(),
                    "cord_rejected",
                    f"minimum_cord_bytes={summary.CORD_HEADER.size + larger * summary.CORD_ENTRY_BYTES}"
                    f" budget={self.params.cord_max_bytes}",
                    larger,
                )
            )
            return self._narrow(item, peer_summary, notes)
        return ("cord", None)

    def _narrow(self, item, peer_summary, notes):
        region = item.region
        if region.node.gen < int(self.params.max_gen):
            notes.append(RegionNote(region.label(), "hash_split", "summary_unusable"))
            return ("split", [WorkItem(c, item.key_splits) for c in region.hash_children()])
        pivot = peer_summary.median
        if (
            item.key_splits < self.params.max_key_splits
            and pivot is not None
            and self._progressing_key_split(region, pivot)
        ):
            left, right = region.key_children(pivot)
            notes.append(RegionNote(region.label(), "key_split", "summary_unusable"))
            return (
                "split",
                [WorkItem(left, item.key_splits + 1), WorkItem(right, item.key_splits + 1)],
            )
        return ("fallback", region)

    def _resolve_cords(self, cord_items, next_frontier, notes, left_only,
                       right_only, changed, *, force: bool) -> None:
        """Batch the cord requests, then compare each decodable leaf."""

        regions = [item.region for item, _ in cord_items]
        replies: list[dict[str, Any]] = []
        chunk = self.params.max_nodes_per_query
        for start in range(0, len(regions), chunk):
            part = regions[start : start + chunk]
            reply = self.call(
                "summaries",
                {"regions": [r.to_wire() for r in part], "want": "cord"},
                "cord",
            )
            self.summary_requests += 1
            self.summary_regions += len(part)
            self.cord_requests += len(part)
            results = reply.get("results")
            if not isinstance(results, list) or len(results) != len(part):
                raise ReconciliationError(
                    "bad_response", "peer returned the wrong number of cords"
                )
            replies.extend(results)
        for (item, peer_summary), region_entry in zip(cord_items, replies):
            region = item.region
            larger = max(self.index.node_count(region), peer_summary.count)
            if region_entry.get("oversize") is not None:
                notes.append(
                    RegionNote(
                        region.label(),
                        "cord_rejected",
                        f"cord_bytes={region_entry['oversize']}",
                        larger,
                    )
                )
                self._apply_decision(
                    self._narrow(item, peer_summary, notes), next_frontier, notes,
                    left_only, right_only, changed,
                )
                continue
            cord_hex = region_entry.get("cord")
            if not isinstance(cord_hex, str):
                notes.append(RegionNote(region.label(), "cord_undecodable", "no cord", larger))
                self._apply_decision(
                    self._narrow(item, peer_summary, notes), next_frontier, notes,
                    left_only, right_only, changed,
                )
                continue
            cord = bytes.fromhex(cord_hex)
            if len(cord) > self.params.cord_max_bytes:
                notes.append(
                    RegionNote(
                        region.label(), "cord_rejected", f"cord_bytes={len(cord)}", larger
                    )
                )
                self._apply_decision(
                    self._narrow(item, peer_summary, notes), next_frontier, notes,
                    left_only, right_only, changed,
                )
                continue
            try:
                summary.verify_proof("cord", region, cord, str(region_entry.get("cord_proof", "")))
            except SummaryError as exc:
                notes.append(RegionNote(region.label(), "cord_bad_proof", str(exc), larger))
                self._apply_decision(
                    self._narrow(item, peer_summary, notes), next_frontier, notes,
                    left_only, right_only, changed,
                )
                continue
            self.cord_bytes += len(cord)
            try:
                peer_entries = decode_cord(cord, region, expected_count=peer_summary.count)
            except SummaryError as exc:
                notes.append(RegionNote(region.label(), "cord_undecodable", str(exc), larger))
                self._apply_decision(
                    self._narrow(item, peer_summary, notes), next_frontier, notes,
                    left_only, right_only, changed,
                )
                continue
            local_entries = self.index.node_entries(region)
            comparison = compare_cords(local_entries, peer_entries)
            if comparison.empty:
                notes.append(
                    RegionNote(
                        region.label(),
                        "cord_equal",
                        f"entries={comparison.equal_entries}",
                        larger,
                    )
                )
                continue
            notes.append(
                RegionNote(
                    region.label(),
                    "cord_diff",
                    f"candidate_positions={len(comparison.positions_with_differences)}",
                    larger,
                )
            )
            self._resolve_positions(
                region,
                set(comparison.positions_with_differences),
                left_only,
                right_only,
                changed,
            )

    def _apply_decision(self, decision, next_frontier, notes, left_only, right_only, changed) -> None:
        """Act on a classification decision that arrived without a usable cord.

        A region that cannot be narrowed any further is transferred exactly and
        audibly; it is never dropped.
        """

        if decision is None:
            return
        kind, payload = decision
        if kind == "split":
            next_frontier.extend(payload)
        elif kind == "fallback":
            region = payload
            self._fallback(region, notes, left_only, right_only, changed)
        elif kind == "local_only":
            for row in self.index.node_slice(payload):
                left_only[row.row_key] = row.record
        else:
            raise AssertionError(f"unknown decision {kind!r}")

    def _progressing_key_split(self, region: Region, pivot: bytes) -> bool:
        if not region.contains_key(pivot):
            return False
        left, right = region.key_children(pivot)
        if right is None or left == region or right == region:
            return False
        return True

    # ------------------------------------------------------------ requests

    def _fetch_summaries(self, regions: Sequence[Region]) -> list[summary.NodeSummary]:
        assert self.session is not None
        out: list[summary.NodeSummary] = []
        chunk = self.params.max_nodes_per_query
        for start in range(0, len(regions), chunk):
            part = regions[start : start + chunk]
            reply = self.call(
                "summaries",
                {"regions": [r.to_wire() for r in part], "want": "aggregate"},
                "summ",
            )
            self.summary_requests += 1
            self.summary_regions += len(part)
            results = reply.get("results")
            if not isinstance(results, list) or len(results) != len(part):
                raise ReconciliationError(
                    "bad_response", "peer returned the wrong number of summaries"
                )
            for region, entry in zip(part, results):
                node_summary = summary.NodeSummary.from_wire(entry["summary"], "summary")
                if node_summary.region != region:
                    raise ReconciliationError(
                        "bad_response", "peer summary is for a different region"
                    )
                out.append(node_summary)
        return out

    # ------------------------------------------------------------- exact

    def _resolve_positions(self, region, positions, left_only, right_only, changed) -> None:
        self.probes += 1
        peer_rows: dict[bytes, bytes] = {}
        after: bytes | None = None
        pages = 0
        while True:
            extra: dict[str, Any] = {
                "region": region.to_wire(),
                "positions": sorted(positions),
                "limit": self.params.rows_per_chunk,
            }
            if after is not None:
                extra["after"] = after.hex()
            reply = self.call("probe", extra, "probe")
            pages += 1
            records = reply.get("records")
            if not isinstance(records, list):
                raise ReconciliationError("bad_response", "probe response lacks records")
            for hex_record in records:
                record = bytes.fromhex(hex_record)
                try:
                    decode_record(self.schema, record)
                except encoding.CanonicalizationError as exc:
                    raise ReconciliationError(
                        "bad_record", f"peer row is not canonical: {exc}"
                    ) from exc
                key = record_row_id_bytes(record)
                if key in peer_rows and peer_rows[key] != record:
                    raise ReconciliationError(
                        "bad_response", "peer returned two different rows for one row id"
                    )
                peer_rows[key] = record
            self.probe_rows += len(records)
            if not reply.get("more"):
                break
            next_after = reply.get("next_after")
            if not next_after:
                raise ReconciliationError(
                    "bad_response", "probe reports more rows without a cursor"
                )
            after = bytes.fromhex(next_after)
            pages += 1
            if pages > 2_000_000:
                raise ReconciliationError("runaway", "probe pagination does not terminate")
        self._merge_probe(region, positions, peer_rows, left_only, right_only, changed)

    def _fallback(self, region, notes, left_only, right_only, changed) -> None:
        self.fallbacks += 1
        self.fallback_regions.append(region.label())
        peer_rows: dict[bytes, bytes] = {}
        after: bytes | None = None
        pages = 0
        rows_fetched = 0
        while True:
            extra: dict[str, Any] = {"region": region.to_wire(), "limit": self.params.rows_per_chunk}
            if after is not None:
                extra["after"] = after.hex()
            reply = self.call("fallback", extra, "fallback")
            pages += 1
            records = reply.get("records")
            if not isinstance(records, list):
                raise ReconciliationError("bad_response", "fallback response lacks records")
            for hex_record in records:
                record = bytes.fromhex(hex_record)
                try:
                    decode_record(self.schema, record)
                except encoding.CanonicalizationError as exc:
                    raise ReconciliationError(
                        "bad_record", f"peer row is not canonical: {exc}"
                    ) from exc
                key = record_row_id_bytes(record)
                if key in peer_rows and peer_rows[key] != record:
                    raise ReconciliationError(
                        "bad_response", "peer returned two different rows for one row id"
                    )
                peer_rows[key] = record
            rows_fetched += len(records)
            self.fallback_rows += len(records)
            if not reply.get("more"):
                break
            next_after = reply.get("next_after")
            if not next_after:
                raise ReconciliationError(
                    "bad_response", "fallback reports more rows without a cursor"
                )
            after = bytes.fromhex(next_after)
            if pages > 2_000_000:
                raise ReconciliationError("runaway", "fallback pagination does not terminate")
        notes.append(
            RegionNote(
                region.label(),
                "fallback_exact",
                f"pages={pages} rows={rows_fetched}",
                rows_fetched,
            )
        )
        self._merge_exact(region, peer_rows, left_only, right_only, changed)

    def _merge_probe(self, region, positions, peer_rows, left_only, right_only, changed) -> None:
        """Merge a probe response that covers only the named positions.

        Local rows outside the probed positions were declared equal by the
        cord comparison and must not be re-examined here.
        """

        local_rows = {
            e.row_key: e.record
            for e in self.index.node_slice(region)
            if e.position in positions
        }
        for key, record in local_rows.items():
            if key not in peer_rows:
                left_only[key] = record
            elif peer_rows[key] != record:
                changed[key] = (record, peer_rows[key])
        for key, record in peer_rows.items():
            if key not in local_rows:
                right_only[key] = record

    def _merge_exact(self, region, peer_rows, left_only, right_only, changed) -> None:
        local_rows = self.index.node_by_key(region)
        for key, record in local_rows.items():
            if key not in peer_rows:
                left_only[key] = record
            elif peer_rows[key] != record:
                changed[key] = (record, peer_rows[key])
        for key, record in peer_rows.items():
            if key not in local_rows:
                right_only[key] = record

    # ------------------------------------------------------- apply/verify

    def _apply_and_verify(self, differences: DifferenceSet) -> tuple[bytes, int]:
        """Apply the peer's side of the differences to a temporary structure.

        Reconstruction: every local row except my-only rows and my side of
        changed rows, plus the peer's added rows and the peer's side of changed
        rows.  The temporary structure is an in-memory map *and*, when enabled,
        a real temporary SQLite database whose rows are read back and decoded,
        so verification exercises an actual persisted application, not just
        arithmetic on the difference lists.
        """

        remove = {record_row_id_bytes(r) for r in differences.left_side_records()}
        rebuilt: dict[bytes, bytes] = {}
        for entry in self.index.node_slice(Region.root(self.params.position_bits)):
            if entry.row_key in remove:
                continue
            rebuilt[entry.row_key] = entry.record
        for record in differences.peer_side_records():
            key = record_row_id_bytes(record)
            if key in rebuilt:
                raise ReconciliationError(
                    "apply_conflict", f"peer record for row {key!r} collides with kept local row"
                )
            rebuilt[key] = record

        records = sorted(rebuilt.values())
        commitment = encoding.compute_commitment(self.schema, self.identity.params, records)
        temp_rows = len(records)
        if self.verify_temp_sqlite:
            temp_commit, temp_rows = self._verify_in_temp_sqlite(records)
            if temp_commit != commitment:
                raise ReconciliationError(
                    "temp_verify", "temporary SQLite application disagrees with in-memory rebuild"
                )
        return commitment, temp_rows

    def _verify_in_temp_sqlite(self, records: Iterable[bytes]) -> tuple[bytes, int]:
        fd, path = tempfile.mkstemp(prefix="recon-apply-", suffix=".sqlite")
        os.close(fd)
        try:
            conn = sqlite3.connect(path)
            try:
                conn.execute(
                    "CREATE TABLE applied (row_id BLOB PRIMARY KEY, record BLOB NOT NULL UNIQUE)"
                )
                with conn:
                    conn.executemany(
                        "INSERT INTO applied(row_id, record) VALUES (?, ?)",
                        ((record_row_id_bytes(r), r) for r in records),
                    )
                stored = []
                for row_id, record in conn.execute(
                    "SELECT row_id, record FROM applied ORDER BY row_id"
                ):
                    record = bytes(record)
                    decode_record(self.schema, record)
                    if record_row_id_bytes(record) != bytes(row_id):
                        raise ReconciliationError(
                            "temp_verify", "temporary row key does not match its record"
                        )
                    stored.append(record)
            finally:
                conn.close()
            commitment = encoding.compute_commitment(
                self.schema, self.identity.params, stored
            )
            return commitment, len(stored)
        finally:
            try:
                os.unlink(path)
            except FileNotFoundError:
                pass
