"""Worker entry points for genuinely separate endpoint processes.

Invoked as ``python -I -m recon.worker {initiator|responder} ...``.  The worker
is given only its own snapshot path (and, for a responder, its own replay-cache
path); it never learns or opens the other endpoint's files.

Control protocol: one JSON object per line on stdin, one per line on stdout.
Frames travel inside ``{"cmd":"frame","raw":<hex>}`` and
``{"event":"frame","raw":<hex>,"kind":...}`` control messages, so the process
supervisor is a pure byte carrier.
"""

from __future__ import annotations

import json
import sys
import traceback

from .initiator import Initiator
from .responder import Responder
from .store import AnswerCache, SnapshotStore
from .summary import SummaryParams
from .wire import encode_frame


class StdioLog:
    """Minimal log for a worker: never writes the peer's data anywhere."""

    def __init__(self) -> None:
        self.entries: list[dict] = []

    def message_sent(self, message, size):
        self.entries.append(
            {
                "direction": "sent",
                "kind": message["kind"],
                "msg_id": message["msg_id"],
                "bytes": size,
            }
        )

    def message_received(self, message, size):
        self.entries.append(
            {
                "direction": "received",
                "kind": message["kind"],
                "msg_id": message["msg_id"],
                "bytes": size,
            }
        )

    def event(self, name, **fields):
        self.entries.append({"event": name, **fields})


def emit(obj: dict) -> None:
    sys.stdout.write(json.dumps(obj, sort_keys=True) + "\n")
    sys.stdout.flush()


class StdioLink:
    """A blocking byte link over the worker's stdin/stdout control channel."""

    def __init__(self, role: str, peer_role: str) -> None:
        self.role = role
        self.peer_role = peer_role
        self.bytes_sent = 0

    def send(self, message: dict) -> bytes:
        raw = encode_frame(message)
        self.send_bytes(raw)
        return raw

    def send_bytes(self, raw: bytes) -> None:
        import json as _json

        kind = _json.loads(raw[4:].decode("utf-8"))["kind"]
        self.bytes_sent += len(raw)
        emit({"event": "frame", "raw": raw.hex(), "kind": kind})

    def recv(self) -> bytes | None:
        line = sys.stdin.readline()
        if not line:
            return None
        command = json.loads(line)
        if command.get("cmd") == "close":
            return None
        if command.get("cmd") != "frame":
            raise RuntimeError(f"worker received unknown command {command.get('cmd')!r}")
        return bytes.fromhex(command["raw"])


def responder_main(argv: list[str]) -> int:
    if len(argv) != 4:
        emit({"ok": False, "error": "usage: responder SNAPSHOT ANSWERCACHE ROLE"})
        return 2
    snapshot_path, cache_path, role = argv[1], argv[2], argv[3]
    store = SnapshotStore.open(snapshot_path)
    cache = AnswerCache.open(cache_path)
    log = StdioLog()
    link = StdioLink(role, "initiator")
    responder = Responder(role, store, link=link, log=log, cache=cache)
    status = 0
    try:
        responder.receive_forever(lambda: link.recv())
    except Exception:  # noqa: BLE001 - report to the supervisor
        emit({"ok": False, "error": traceback.format_exc()})
        status = 1
    finally:
        emit(
            {
                "ok": status == 0,
                "stats": responder.stats.to_dict(),
                "snapshot_id": responder.identity.snapshot_id(),
                "cache_rows": cache.count(),
                "log": log.entries,
            }
        )
        cache.close()
        store.close()
    return status


def initiator_main(argv: list[str]) -> int:
    if len(argv) != 4:
        emit({"ok": False, "error": "usage: initiator SNAPSHOT ROLE PEER_ROLE"})
        return 2
    snapshot_path, role, peer_role = argv[1], argv[2], argv[3]
    store = SnapshotStore.open(snapshot_path)
    log = StdioLog()
    link = StdioLink(role, peer_role)
    status = 0
    try:
        line = sys.stdin.readline()
        if not line:
            return 0
        command = json.loads(line)
        if command.get("cmd") != "run":
            emit({"ok": False, "error": f"unknown command {command.get('cmd')!r}"})
            return 2
        engine = Initiator(
            role,
            store,
            link=link,
            log=log,
            params=SummaryParams.from_wire(command["summary"]),
            verify_temp_sqlite=bool(command.get("verify_temp_sqlite", True)),
        )
        result = engine.run(command["session"])
        emit(
            {
                "ok": True,
                "result": result.to_dict(),
                "snapshot_id": engine.identity.snapshot_id(),
                "log": log.entries,
            }
        )
    except Exception:  # noqa: BLE001 - report to the supervisor
        emit({"ok": False, "error": traceback.format_exc()})
        status = 1
    finally:
        store.close()
    return status


def main(argv: list[str]) -> int:
    if len(argv) < 2:
        emit({"ok": False, "error": "usage: {initiator|responder} ..."})
        return 2
    mode = argv[1]
    if mode == "initiator":
        return initiator_main(argv[1:])
    if mode == "responder":
        return responder_main(argv[1:])
    emit({"ok": False, "error": f"unknown mode {mode!r}"})
    return 2


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
