"""Protocol wire session primitives shared by client and responder.

Messages are length-prefixed canonical JSON.  Every message carries the
sender's and receiver's fixed-snapshot identities, the initiator-chosen session
id, and the fully negotiated parameters, so a frame from another snapshot,
session, or negotiation can never be applied to this reconciliation.

The responder is *replay safe*: answers are cached by session + request id in
the serving endpoint's own SQLite database, and a repeated request returns the
byte-identical cached frame.  Request ids are deterministic, so a full session
replay (the harness does this deliberately) reuses the cache and produces the
same answers without re-reading or re-counting anything as a new difference.

The client is *order safe*: it accepts only the answer whose echoed
``request_id`` and ``request_msg_id`` match the outstanding request.  Duplicate,
stale, foreign, or misaddressed answers are counted and logged, never applied.
"""

from __future__ import annotations

import hashlib
import json
import struct
from dataclasses import dataclass, field
from typing import Any, Callable, Mapping, Protocol

from . import encoding
from .store import SnapshotIdentity

PROTOCOL_VERSION = 1
MAX_MESSAGE_BYTES = 256 * 1024 * 1024


class ProtocolError(RuntimeError):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(f"{code}: {message}")
        self.code = code
        self.message = message


class IdentityError(ProtocolError):
    pass


class DecodeError(ProtocolError):
    pass


def sha256(data: bytes) -> bytes:
    return hashlib.sha256(data).digest()


@dataclass(frozen=True)
class PeerSnapshot:
    """A compact public identity of one fixed snapshot.

    ``id`` is the snapshot identity hash; ``schema_sha`` pins the full schema
    exchanged in the handshake without re-sending it on every frame.
    """

    snapshot_id: str
    schema_sha: bytes
    commitment: bytes
    row_count: int
    first_key: bytes
    last_key: bytes

    @staticmethod
    def from_identity(identity: SnapshotIdentity) -> "PeerSnapshot":
        return PeerSnapshot(
            snapshot_id=identity.snapshot_id(),
            schema_sha=sha256(identity.schema_wire),
            commitment=identity.commitment,
            row_count=identity.row_count,
            first_key=identity.first_key,
            last_key=identity.last_key,
        )

    def to_wire(self) -> dict[str, Any]:
        return {
            "id": self.snapshot_id,
            "schema_sha": self.schema_sha.hex(),
            "commitment": self.commitment.hex(),
            "rows": self.row_count,
            "first": self.first_key.hex(),
            "last": self.last_key.hex(),
        }

    @staticmethod
    def from_wire(obj: Mapping[str, Any]) -> "PeerSnapshot":
        try:
            snap = PeerSnapshot(
                snapshot_id=str(obj["id"]),
                schema_sha=bytes.fromhex(obj["schema_sha"]),
                commitment=bytes.fromhex(obj["commitment"]),
                row_count=int(obj["rows"]),
                first_key=bytes.fromhex(obj["first"]),
                last_key=bytes.fromhex(obj["last"]),
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise DecodeError("identity_wire", f"malformed snapshot identity: {exc}") from exc
        if len(snap.snapshot_id) != 64:
            raise DecodeError("identity_wire", "snapshot id must be a SHA-256 hex string")
        if len(snap.schema_sha) != 32 or len(snap.commitment) != 32:
            raise DecodeError("identity_wire", "snapshot identity has wrong-sized hashes")
        if snap.row_count < 0:
            raise DecodeError("identity_wire", "snapshot identity has a negative row count")
        return snap


def encode_frame(message: Mapping[str, Any]) -> bytes:
    body = json.dumps(
        message, sort_keys=True, separators=(",", ":"), ensure_ascii=True
    ).encode("utf-8")
    if len(body) > MAX_MESSAGE_BYTES:
        raise ProtocolError("frame_too_large", "message exceeds the maximum frame size")
    return struct.pack(">I", len(body)) + body


def decode_frame(raw: bytes) -> dict[str, Any]:
    if len(raw) < 4:
        raise DecodeError("frame_short", "frame is shorter than its length prefix")
    (length,) = struct.unpack(">I", raw[:4])
    if length != len(raw) - 4:
        raise DecodeError("frame_length", "frame length prefix disagrees with the body length")
    if length > MAX_MESSAGE_BYTES:
        raise DecodeError("frame_too_large", "frame exceeds the maximum frame size")
    try:
        message = json.loads(raw[4:].decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise DecodeError("frame_json", f"message is not canonical JSON: {exc}") from exc
    if not isinstance(message, dict):
        raise DecodeError("frame_shape", "message body must be a JSON object")
    check_envelope(message)
    return message


_REQUIRED = ("v", "kind", "msg_id", "session", "from", "to", "snapshot", "peer", "negotiated")


def check_envelope(message: Mapping[str, Any]) -> None:
    for key in _REQUIRED:
        if key not in message:
            raise DecodeError("envelope_missing", f"message lacks required field {key!r}")
    if message["v"] != PROTOCOL_VERSION:
        raise DecodeError("version", f"unsupported protocol version {message['v']!r}")
    for key in ("kind", "msg_id", "session", "from", "to"):
        if not isinstance(message[key], str) or not message[key]:
            raise DecodeError(
                "envelope_shape", f"message field {key!r} must be a non-empty string"
            )
    if not isinstance(message["snapshot"], dict) or not isinstance(message["peer"], dict):
        raise DecodeError("envelope_shape", "message snapshot/peer must be objects")
    if not isinstance(message["negotiated"], dict):
        raise DecodeError("envelope_shape", "message negotiated must be an object")


@dataclass(frozen=True)
class SessionContext:
    """Everything a frame must match to belong to the live session."""

    role: str
    peer_role: str
    session: str
    own: PeerSnapshot
    peer: PeerSnapshot
    negotiated: dict

    def envelope(
        self,
        kind: str,
        msg_id: str,
        *,
        request_id: str | None = None,
        extra: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        message: dict[str, Any] = {
            "v": PROTOCOL_VERSION,
            "kind": kind,
            "msg_id": msg_id,
            "session": self.session,
            "from": self.role,
            "to": self.peer_role,
            "snapshot": self.own.to_wire(),
            "peer": self.peer.to_wire(),
            "negotiated": self.negotiated,
        }
        if request_id is not None:
            message["request_id"] = request_id
        if extra:
            message.update(extra)
        return message


def validate_frame(session: SessionContext, message: Mapping[str, Any]) -> None:
    check_envelope(message)
    if message["to"] != session.role:
        raise IdentityError("identity_to", f"message addressed to {message['to']!r}")
    if message["from"] != session.peer_role:
        raise IdentityError("identity_from", f"message from unexpected role {message['from']!r}")
    if message["session"] != session.session:
        raise IdentityError(
            "identity_session", f"message is for session {message['session']!r}"
        )
    snap = PeerSnapshot.from_wire(message["snapshot"])
    if snap != session.peer:
        raise IdentityError("identity_snapshot", "sender snapshot identity does not match")
    if PeerSnapshot.from_wire(message["peer"]) != session.own:
        raise IdentityError("identity_peer", "receiver snapshot identity does not match")
    if message["negotiated"] != session.negotiated:
        raise IdentityError("identity_negotiated", "negotiated parameters differ")


@dataclass(frozen=True)
class Negotiation:
    """The fully negotiated codec, commitment, and summary parameters."""

    schema_signature: tuple
    commit_params: dict
    summary: dict

    def to_wire(self) -> dict:
        return {
            "schema": list(self.schema_signature),
            "commit": self.commit_params,
            "summary": self.summary,
        }


class Link:
    """One direction of the simulated transport, with exact byte accounting."""

    def __init__(self, sender: str, on_send: Callable[[dict[str, Any], bytes], None]):
        self.sender = sender
        self.on_send = on_send
        self.frames = 0
        self.bytes = 0
        self.by_kind: dict[str, int] = {}

    def send(self, message: dict[str, Any]) -> bytes:
        raw = encode_frame(message)
        self.frames += 1
        self.bytes += len(raw)
        kind = str(message["kind"])
        self.by_kind[kind] = self.by_kind.get(kind, 0) + len(raw)
        self.on_send(message, raw)
        return raw

    def send_bytes(self, raw: bytes) -> None:
        """Send a pre-encoded frame (used when replaying a cached answer)."""

        import json as _json

        message = _json.loads(raw[4:].decode("utf-8"))
        self.send(message)


def request_key(session: str, request_id: str, request_msg_id: str) -> str:
    """Cache key binding a request to its session and message identity."""

    return hashlib.sha256(
        encoding._lp(b"RECON:REQUEST:v1")
        + encoding._lp(session.encode("utf-8"))
        + encoding._lp(request_id.encode("utf-8"))
        + encoding._lp(request_msg_id.encode("utf-8"))
    ).hexdigest()


class InvalidNegotiation(ProtocolError):
    pass


def negotiate(
    *,
    initiator_schema,
    responder_schema,
    initiator_summary: Mapping[str, Any],
    initiator_commit: Mapping[str, Any],
) -> dict:
    """Agree the exact codec, commitment parameters, and summary parameters.

    The responder verifies that both sides declare an identical schema (names,
    types, order, nullability) and an identical commitment parameter set, and
    adopts the initiator's summary budget exactly as configured.  It never
    substitutes a budget derived from the data.  Any disagreement is an
    explicit protocol error, not an implicit default.
    """

    if initiator_schema.signature() != responder_schema.signature():
        raise InvalidNegotiation(
            "schema_mismatch",
            "the two endpoints declare different schemas "
            f"({initiator_schema.signature()} vs {responder_schema.signature()})",
        )
    expected_commit = dict(encoding.COMMIT_PARAMS)
    if dict(initiator_commit) != expected_commit:
        raise InvalidNegotiation(
            "commit_mismatch",
            f"commitment parameters {dict(initiator_commit)!r} do not match {expected_commit!r}",
        )
    from .summary import SummaryParams

    try:
        params = SummaryParams.from_wire(dict(initiator_summary))
    except (ValueError, TypeError, KeyError) as exc:
        raise InvalidNegotiation("summary_params", f"invalid summary parameters: {exc}") from exc
    return {
        "schema": [list(item) for item in responder_schema.signature()],
        "commit": expected_commit,
        "summary": params.to_wire(),
    }

