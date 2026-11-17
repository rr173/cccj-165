"""Responder engine: the serving side of a reconciliation session.

The responder reads only its own fixed snapshot.  It answers four request
kinds:

* ``hello``      negotiate codec, commitment parameters, summary budget, schema.
* ``summaries``  compact aggregate (or cord) summaries for a batch of regions.
* ``probe``      actual canonical rows at named hash positions, paged by key.
* ``fallback``   every row of a region inside a row-key interval, paged.

Answers are deterministic functions of the request and are cached in the
serving snapshot's own ``messages`` table, keyed by session + request id +
request message id.  A redelivered request returns the byte-identical cached
frame and cannot double count or mutate anything.  The request cache key
includes the pinned snapshot identity through the validated envelope, so a
frame for another snapshot is rejected before the cache is consulted.

The responder never sends its whole table unless the initiator explicitly asks
for a region's rows, and every such region is named and logged.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

from . import encoding
from .encoding import Schema
from .store import SnapshotStore, SnapshotIdentity, parse_schema_wire
from .summary import (
    LocalIndex,
    Region,
    SummaryError,
    SummaryParams,
    node_proof,
)
from .wire import (
    DecodeError,
    PeerSnapshot,
    ProtocolError,
    SessionContext,
    decode_frame,
    negotiate,
    request_key,
    sha256,
    validate_frame,
)


@dataclass
class ResponderStats:
    requests: int = 0
    cache_hits: int = 0
    summaries_served: int = 0
    cords_served: int = 0
    probe_pages_served: int = 0
    fallback_pages_served: int = 0
    rows_served: int = 0
    rejected: int = 0

    def to_dict(self) -> dict[str, int]:
        return dict(self.__dict__)


class Responder:
    def __init__(self, role: str, store: SnapshotStore, *, link, log, cache=None) -> None:
        self.role = role
        self.store = store
        self.cache = cache
        self.identity: SnapshotIdentity = store.identity
        self.schema: Schema = store.schema
        self.link = link
        self.peer_role = getattr(link, "peer_role", "peer")
        self.log = log
        self.session: SessionContext | None = None
        self.summary_params: SummaryParams | None = None
        self.index: LocalIndex | None = None
        self.stats = ResponderStats()
        self._answers: dict[str, bytes] = {}
        self._msg_counter = 0

    # ------------------------------------------------------------- plumbing

    def _msg_id(self, kind: str) -> str:
        self._msg_counter += 1
        return f"{self.role}-{kind}-{self._msg_counter:06d}"

    def _cached(self, session: SessionContext, request: Mapping[str, Any]) -> bytes | None:
        rid = str(request.get("request_id", ""))
        key = request_key(session.session, rid, str(request["msg_id"]))
        if key in self._answers:
            self.stats.cache_hits += 1
            self.log.event("answer_replayed", request_id=rid, bytes=len(self._answers[key]))
            return self._answers[key]
        stored = self.cache.find(key) if self.cache is not None else self.store.find_cached_answer(key)
        if stored is not None:
            _, payload = stored
            self._answers[key] = payload
            self.stats.cache_hits += 1
            self.log.event("answer_replayed_from_store", request_id=rid, bytes=len(payload))
            return payload
        return None

    def _respond(
        self,
        session: SessionContext,
        request: Mapping[str, Any],
        kind: str,
        extra: Mapping[str, Any],
    ) -> bytes:
        rid = str(request.get("request_id", ""))
        message = session.envelope(
            kind,
            self._msg_id(kind),
            request_id=rid,
            extra={"request_msg_id": str(request["msg_id"]), **extra},
        )
        raw = self.link.send(message)
        self.log.message_sent(message, len(raw))
        key = request_key(session.session, rid, str(request["msg_id"]))
        self._answers[key] = raw
        if self.cache is None:
            try:
                self.store.store_answer(message["msg_id"], key, raw)
            except Exception as exc:  # read-only snapshot without a sidecar cache
                self.log.event("answer_cache_unavailable", detail=repr(exc))
        else:
            self.cache.store(message["msg_id"], key, raw)
        return raw

    def _error(self, session: SessionContext, request: Mapping[str, Any], code: str, detail: str) -> None:
        self.log.event("responder_error", code=code, detail=detail, request_id=request.get("request_id"))
        message = session.envelope(
            "error",
            self._msg_id("error"),
            request_id=str(request.get("request_id", "")),
            extra={
                "request_msg_id": str(request.get("msg_id", "")),
                "code": code,
                "message": detail,
            },
        )
        raw = self.link.send(message)
        self.log.message_sent(message, len(raw))

    def _bootstrap(self, request: Mapping[str, Any]) -> SessionContext:
        peer = PeerSnapshot.from_wire(request["snapshot"])
        return SessionContext(
            role=self.role,
            peer_role=str(request.get("from", "")),
            session=str(request.get("session", "")),
            own=PeerSnapshot.from_identity(self.identity),
            peer=peer,
            negotiated={},
        )

    # ----------------------------------------------------------- dispatching

    def receive(self, raw: bytes) -> None:
        try:
            request = decode_frame(raw)
        except ProtocolError as exc:
            self.stats.rejected += 1
            self.log.event("frame_rejected", code=exc.code, detail=exc.message)
            return
        kind = request["kind"]
        self.stats.requests += 1
        if kind == "hello":
            self._handle_hello(request)
            return
        if self.session is None:
            self.stats.rejected += 1
            self.log.event("request_rejected", code="no_session")
            return
        try:
            validate_frame(self.session, request)
        except ProtocolError as exc:
            self.stats.rejected += 1
            code = getattr(exc, "code", "protocol")
            self.log.event("request_rejected", code=code, detail=str(exc))
            # Answer with an explicit protocol error so a peer that pinned a
            # wrong identity learns immediately instead of waiting forever.
            # The error carries no snapshot content.
            try:
                self._error(self._bootstrap(request), request, code, str(exc))
            except Exception:  # noqa: BLE001 - rejection must not crash serving
                pass
            return
        # Replay check happens *before* any work: a repeated request must not
        # re-read rows, re-hash, or re-count anything.
        cached = self._cached(self.session, request)
        if cached is not None:
            self.link.send_bytes(cached)
            return
        try:
            if kind == "summaries":
                self._handle_summaries(request)
            elif kind == "probe":
                self._handle_probe(request)
            elif kind == "fallback":
                self._handle_fallback(request)
            else:
                raise ProtocolError("bad_kind", f"unknown request kind {kind!r}")
        except (SummaryError, DecodeError, ValueError, ProtocolError) as exc:
            self.stats.rejected += 1
            code = getattr(exc, "code", "bad_request")
            self._error(self.session, request, code, str(exc))

    # --------------------------------------------------------------- hello

    def receive_forever(self, next_frame) -> None:
        """Serve frames until the transport closes."""

        while True:
            raw = next_frame()
            if raw is None:
                return
            self.receive(raw)

    def _handle_hello(self, request: Mapping[str, Any]) -> None:
        try:
            if request["to"] != self.role:
                raise ProtocolError("identity_to", "hello addressed to another endpoint")
            peer_snap = PeerSnapshot.from_wire(request["snapshot"])
            if self.session is not None and self.session.peer != peer_snap:
                raise ProtocolError(
                    "snapshot_changed", "a different peer snapshot was already pinned"
                )
            peer_schema = parse_schema_wire(bytes.fromhex(request["schema"]))
            if peer_snap.schema_sha != sha256(encoding.schema_wire(peer_schema)):
                raise ProtocolError("schema_hash", "schema bytes disagree with advertised hash")
            negotiated = negotiate(
                initiator_schema=peer_schema,
                responder_schema=self.schema,
                initiator_summary=dict(request["negotiate"]),
                initiator_commit=dict(request["commit"]),
            )
            self.summary_params = SummaryParams.from_wire(negotiated["summary"])
            if self.index is None:
                self.index = LocalIndex(
                    self.store.iter_records(), self.summary_params.position_bits
                )
            # The responder answers hello on a context whose own identity is
            # the responder snapshot and whose peer identity is the caller.
            # Note the roles are swapped relative to the initiator's context:
            # ``own`` here is always the responder snapshot.
            session = SessionContext(
                role=self.role,
                peer_role=str(request["from"]),
                session=str(request["session"]),
                own=PeerSnapshot.from_identity(self.identity),
                peer=peer_snap,
                negotiated=negotiated,
            )
            self.session = session
            self._respond(
                session,
                request,
                "hello_ack",
                {
                    "schema": encoding.schema_wire(self.schema).hex(),
                    "snapshot": PeerSnapshot.from_identity(self.identity).to_wire(),
                },
            )
        except (ProtocolError, SummaryError, ValueError, KeyError, TypeError) as exc:
            self.stats.rejected += 1
            code = getattr(exc, "code", "hello_bad")
            self.log.event("hello_rejected", code=code, detail=str(exc))
            try:
                self._error(self._bootstrap(request), request, code, str(exc))
            except Exception:  # noqa: BLE001 - best effort before identity exists
                pass

    # ----------------------------------------------------------- summaries

    def _handle_summaries(self, request: Mapping[str, Any]) -> None:
        assert self.session is not None and self.index is not None
        assert self.summary_params is not None
        regions_raw = request.get("regions")
        if not isinstance(regions_raw, list) or not regions_raw:
            raise ProtocolError("bad_request", "summaries requires a non-empty region list")
        if len(regions_raw) > self.summary_params.max_nodes_per_query:
            raise ProtocolError("bad_request", "too many regions in one summaries request")
        want = request.get("want", "aggregate")
        if want not in ("aggregate", "cord"):
            raise ProtocolError("bad_request", "want must be 'aggregate' or 'cord'")
        results = []
        for obj in regions_raw:
            region = Region.from_wire(obj)
            node_summary = self.index.node_summary(region)
            entry: dict[str, Any] = {"summary": node_summary.to_wire("summary")}
            if want == "cord":
                cord = self.index.node_cord(region)
                if len(cord) > self.summary_params.cord_max_bytes:
                    entry["oversize"] = len(cord)
                    entry["cord"] = None
                else:
                    entry["cord"] = cord.hex()
                    entry["cord_proof"] = node_proof("cord", region, cord).hex()
                self.stats.cords_served += 1
            results.append(entry)
            self.stats.summaries_served += 1
        self._respond(self.session, request, "summaries_ack", {"results": results})

    # ---------------------------------------------------------------- probe

    def _cursor(self, region: Region, request: Mapping[str, Any]) -> bytes | None:
        raw = request.get("after")
        if raw is None:
            return None
        try:
            after = bytes.fromhex(raw)
        except (TypeError, ValueError) as exc:
            raise ProtocolError("bad_request", "after is not valid hex") from exc
        if not region.contains_key(after):
            raise ProtocolError("bad_request", "after cursor is outside the region")
        return after

    def _limit(self, request: Mapping[str, Any]) -> int:
        assert self.summary_params is not None
        try:
            limit = int(request.get("limit", self.summary_params.rows_per_chunk))
        except (TypeError, ValueError) as exc:
            raise ProtocolError("bad_request", "limit must be an integer") from exc
        if not 1 <= limit <= self.summary_params.rows_per_chunk:
            raise ProtocolError("bad_request", "limit exceeds the negotiated chunk size")
        return limit

    def _handle_probe(self, request: Mapping[str, Any]) -> None:
        assert self.session is not None and self.index is not None
        region = Region.from_wire(request["region"])
        positions_raw = request.get("positions")
        if not isinstance(positions_raw, list) or not positions_raw:
            raise ProtocolError("bad_request", "probe requires a non-empty position list")
        positions: set[int] = set()
        for value in positions_raw:
            if isinstance(value, bool) or not isinstance(value, int):
                raise ProtocolError("bad_request", "probe positions must be integers")
            if not region.node.contains(value):
                raise ProtocolError("bad_request", "probe position lies outside the node")
            positions.add(value)
        after = self._cursor(region, request)
        limit = self._limit(request)
        rows = self.index.rows_at_positions_page(region, positions, after, limit)
        remaining = self.index.remaining_rows_at_positions(region, positions, after)
        payload: dict[str, Any] = {
            "records": [r.record.hex() for r in rows],
            "more": len(rows) < remaining,
        }
        if rows:
            payload["next_after"] = rows[-1].row_key.hex()
        self.stats.probe_pages_served += 1
        self.stats.rows_served += len(rows)
        self._respond(self.session, request, "probe_ack", payload)

    def _handle_fallback(self, request: Mapping[str, Any]) -> None:
        assert self.session is not None and self.index is not None
        region = Region.from_wire(request["region"])
        after = self._cursor(region, request)
        limit = self._limit(request)
        rows = self.index.records_in_key_range(region, after, limit)
        probe = self.index.records_in_key_range(region, after, limit + 1)
        more = len(probe) > len(rows)
        payload: dict[str, Any] = {
            "records": [r.record.hex() for r in rows],
            "more": more,
        }
        if rows:
            payload["next_after"] = rows[-1].row_key.hex()
        self.stats.fallback_pages_served += 1
        self.stats.rows_served += len(rows)
        self.log.event("fallback_served", region=region.label(), rows=len(rows), more=more)
        self._respond(self.session, request, "fallback_ack", payload)
