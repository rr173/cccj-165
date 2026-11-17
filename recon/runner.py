"""In-process session runner used by tests and the quick demo.

Both engines get their own ``SnapshotStore`` (hence their own SQLite handle)
and a byte-only callback link; they never share rows or a database handle.  The
responder runs in a worker thread.  This exercises exactly the same engines as
the two-process demo, which is the authoritative end-to-end run.
"""

from __future__ import annotations

import queue
import threading
import uuid
from dataclasses import dataclass
from typing import Any, Callable

from .initiator import Initiator, RunResult
from .responder import Responder, ResponderStats
from .store import AnswerCache, SnapshotStore
from .summary import SummaryParams
from .wire import encode_frame


class MessageLog:
    """Append-only message log with byte counts and notable events."""

    def __init__(self) -> None:
        self.entries: list[dict[str, Any]] = []
        self._lock = threading.Lock()

    def _add(self, entry: dict[str, Any]) -> None:
        with self._lock:
            self.entries.append(entry)

    def message_sent(self, message, size):
        self._add(
            {
                "direction": "sent",
                "role": message["from"],
                "kind": message["kind"],
                "msg_id": message["msg_id"],
                "request_id": message.get("request_id"),
                "bytes": size,
                "session": message["session"],
                "snapshot_id": message["snapshot"]["id"],
            }
        )

    def message_received(self, message, size):
        self._add(
            {
                "direction": "received",
                "role": message["to"],
                "kind": message["kind"],
                "msg_id": message["msg_id"],
                "request_id": message.get("request_id"),
                "bytes": size,
                "session": message["session"],
                "snapshot_id": message["snapshot"]["id"],
            }
        )

    def event(self, name: str, **fields: Any) -> None:
        self._add({"event": name, **fields})

    def sent_by_kind(self, role: str) -> dict[str, int]:
        out: dict[str, int] = {}
        for entry in self.entries:
            if entry.get("direction") == "sent" and entry["role"] == role:
                out[entry["kind"]] = out.get(entry["kind"], 0) + entry["bytes"]
        return out

    def total_sent(self, role: str) -> int:
        return sum(self.sent_by_kind(role).values())

    def events(self, name: str | None = None) -> list[dict[str, Any]]:
        return [
            e for e in self.entries if "event" in e and (name is None or e["event"] == name)
        ]


class CallbackLink:
    """One direction of a byte pipe: ``send`` serialises and hands off bytes.

    ``recv`` blocks on a queue that the peer's link fills.  Inbox ownership is
    set after construction so the two links can be created without a cycle.
    """

    def __init__(
        self,
        role: str,
        peer_role: str,
        on_send: Callable[[bytes], None],
        on_duplicate: Callable[[bytes], None] | None = None,
    ) -> None:
        self.role = role
        self.peer_role = peer_role
        self.on_send = on_send
        self.on_duplicate = on_duplicate
        self.inbox: "queue.Queue[bytes | None]" = queue.Queue()
        self.bytes_sent = 0
        self.frames_sent = 0
        self.by_kind: dict[str, int] = {}

    def send(self, message: dict[str, Any]) -> bytes:
        raw = encode_frame(message)
        self.send_bytes(raw)
        return raw

    def send_bytes(self, raw: bytes) -> None:
        """Send an already-encoded frame (used for cached replays)."""

        self.bytes_sent += len(raw)
        self.frames_sent += 1
        self.on_send(raw)
        if self.on_duplicate is not None:
            self.on_duplicate(raw)

    def recv(self) -> bytes | None:
        try:
            return self.inbox.get(timeout=60)
        except queue.Empty:
            raise RuntimeError("transport timed out waiting for a frame") from None


@dataclass
class SessionOutcome:
    result: RunResult
    responder_stats: ResponderStats
    log: MessageLog
    responder_error: BaseException | None = None


def run_session(
    left: SnapshotStore,
    right: SnapshotStore,
    *,
    params: SummaryParams,
    session: str | None = None,
    duplicate_answers: int = 0,
    verify_temp_sqlite: bool = True,
    cache_path=None,
) -> SessionOutcome:
    session_id = session or uuid.uuid4().hex
    log = MessageLog()
    cache = AnswerCache.open(
        cache_path or right.path.with_suffix(".answers.sqlite")
    )
    to_responder: "queue.Queue[bytes | None]" = queue.Queue()
    to_initiator: "queue.Queue[bytes | None]" = queue.Queue()
    duplicates_left = [duplicate_answers]

    def init_send(raw: bytes) -> None:
        to_responder.put(raw)

    def responder_send(raw: bytes) -> None:
        to_initiator.put(raw)
        if duplicates_left[0] > 0:
            duplicates_left[0] -= 1
            to_initiator.put(raw)

    init_link = CallbackLink("initiator", "responder", init_send)
    responder_link = CallbackLink("responder", "initiator", responder_send)
    init_link.inbox = to_initiator
    responder_link.inbox = to_responder

    init = Initiator(
        "initiator",
        left,
        link=init_link,
        log=log,
        params=params,
        verify_temp_sqlite=verify_temp_sqlite,
    )
    responder = Responder("responder", right, link=responder_link, log=log, cache=cache)
    errors: list[BaseException] = []

    def serve() -> None:
        while True:
            raw = to_responder.get()
            if raw is None:
                return
            try:
                responder.receive(raw)
            except BaseException as exc:  # noqa: BLE001 - surface to the test
                errors.append(exc)
                # Unblock the initiator with EOF rather than a 60 s timeout.
                to_initiator.put(None)
                return

    thread = threading.Thread(target=serve, name="responder", daemon=True)
    thread.start()
    try:
        result = init.run(session_id)
    finally:
        to_responder.put(None)
        thread.join(timeout=30)
        cache.close()
    if errors:
        raise errors[0]
    return SessionOutcome(result=result, responder_stats=responder.stats, log=log)


class ReplaySession:
    """A driven (non-threaded) session useful for deterministic replay tests.

    Frames are pumped explicitly on one thread.  The responder's answer cache
    may be persisted so a second ``run`` with the same session id is served
    entirely from the cache.
    """

    def __init__(self, left, right, *, params, cache_path):
        import queue as _queue

        self.log = MessageLog()
        self.cache = AnswerCache.open(cache_path)
        self.to_responder: "_queue.Queue[bytes | None]" = _queue.Queue()
        self.to_initiator: "_queue.Queue[bytes | None]" = _queue.Queue()
        self.init_link = CallbackLink("initiator", "responder", self.to_responder.put)
        self.resp_link = CallbackLink("responder", "initiator", self.to_initiator.put)
        self.init_link.inbox = self.to_initiator
        self.initiator = Initiator(
            "initiator", left, link=self.init_link, log=self.log, params=params
        )
        self.responder = Responder(
            "responder", right, link=self.resp_link, log=self.log, cache=self.cache
        )

    def _pump(self) -> None:
        while True:
            try:
                raw = self.to_responder.get_nowait()
            except Exception:
                raw = None
            if raw is None:
                break
            self.responder.receive(raw)

    def _recv(self) -> bytes | None:
        while self.to_initiator.empty():
            self._pump()
        return self.to_initiator.get()

    def run(self, session_id: str):
        self.init_link.recv = self._recv
        return self.initiator.run(session_id)

    def close(self) -> None:
        self.cache.close()
