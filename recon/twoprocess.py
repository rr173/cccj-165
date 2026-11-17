"""Two-process demo: two isolated endpoint processes and a byte relay supervisor.

Each endpoint runs as its own ``python -I`` process launched with only its own
snapshot path (the responder additionally gets its own replay-cache path).  The
supervisor relays opaque frames between them and records exact byte counts.  It
never opens either snapshot and never computes expected differences.

Relay model: two reader threads, one per direction, each blocking on one
worker's stdout and writing the other worker's stdin.  That keeps the
supervisor free of polling and completely data-agnostic.  A "frame" event is
forwarded verbatim; a final result/stats line is recorded and ends that
direction's relay.  Optional duplicate delivery is injected on the
responder->initiator direction only.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import threading
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .summary import SummaryParams

ROOT = str(Path(__file__).resolve().parent.parent)
WORKER_BOOT = (
    "import sys; "
    f"sys.path.insert(0, {ROOT!r}); "
    "from recon.worker import main; sys.exit(main(sys.argv))"
)


@dataclass
class DirectionStats:
    frames: int = 0
    bytes: int = 0
    by_kind: dict[str, int] = field(default_factory=dict)

    def record(self, kind: str, size: int) -> None:
        self.frames += 1
        self.bytes += size
        self.by_kind[kind] = self.by_kind.get(kind, 0) + size


@dataclass
class RelayOutcome:
    session: str
    initiator_final: dict | None
    responder_final: dict | None
    initiator_to_responder: DirectionStats
    responder_to_initiator: DirectionStats
    messages: list[dict[str, Any]]
    stderr_initiator: str
    stderr_responder: str
    returncodes: tuple[int | None, int | None]
    timed_out: bool


def _write(proc: subprocess.Popen, obj: dict) -> None:
    try:
        assert proc.stdin is not None
        proc.stdin.write(json.dumps(obj) + "\n")
        proc.stdin.flush()
    except (BrokenPipeError, ValueError):
        pass


def _launch(argv: list[str]) -> subprocess.Popen:
    env = dict(os.environ)
    # The package root must be importable in an isolated interpreter.
    env["PYTHONPATH"] = ROOT + (os.pathsep + env["PYTHONPATH"] if env.get("PYTHONPATH") else "")
    return subprocess.Popen(
        [sys.executable, "-I", "-c", WORKER_BOOT, *argv],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1,
        env=env,
    )


def run_two_process(
    left_snapshot: Path,
    right_snapshot: Path,
    *,
    params: SummaryParams,
    session: str | None = None,
    duplicate_answers: int = 0,
    answer_cache: Path | None = None,
    timeout: float = 600.0,
) -> RelayOutcome:
    session_id = session or uuid.uuid4().hex
    cache_path = answer_cache or (right_snapshot.parent / "responder-answers.sqlite")
    # A stale cache from an earlier run must not answer this run's requests,
    # because request ids restart.  Each run gets a fresh sidecar.
    if cache_path.exists():
        cache_path.unlink()
    initiator = _launch(["initiator", str(left_snapshot), "initiator", "responder"])
    responder = _launch(
        ["responder", str(right_snapshot), str(cache_path), "responder"]
    )
    i2r = DirectionStats()
    r2i = DirectionStats()
    messages: list[dict[str, Any]] = []
    finals: dict[str, dict] = {}
    done = threading.Event()
    lock = threading.Lock()
    duplicates_left = [duplicate_answers]

    def log(msg: str) -> None:
        with lock:
            messages.append(msg)

    def relay(source: subprocess.Popen, target: subprocess.Popen, direction: str, stats: DirectionStats) -> None:
        assert source.stdout is not None
        while True:
            line = source.stdout.readline()
            if not line:
                return
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                finals[direction] = {"ok": False, "error": f"unparseable worker line: {line!r}"}
                return
            if obj.get("event") == "frame":
                raw = obj["raw"]
                kind = obj.get("kind", "?")
                # A frame event carries the frame as hex, so its payload is
                # twice the wire length.  Count the actual frame size.
                stats.record(kind, len(raw) // 2)
                log({"direction": direction, "kind": kind, "bytes": len(raw) // 2})
                _write(target, {"cmd": "frame", "raw": raw})
                if direction == "responder->initiator" and duplicates_left[0] > 0:
                    duplicates_left[0] -= 1
                    log({"direction": direction, "kind": kind, "bytes": len(raw) // 2, "duplicate": True})
                    _write(target, {"cmd": "frame", "raw": raw})
                continue
            finals[direction] = obj
            if direction == "initiator->responder":
                done.set()
            return

    thread_i = threading.Thread(
        target=relay, args=(initiator, responder, "initiator->responder", i2r), daemon=True
    )
    thread_r = threading.Thread(
        target=relay, args=(responder, initiator, "responder->initiator", r2i), daemon=True
    )
    thread_i.start()
    thread_r.start()
    _write(initiator, {
        "cmd": "run",
        "session": session_id,
        "summary": params.to_wire(),
        "verify_temp_sqlite": True,
    })
    timed_out = not done.wait(timeout)
    # Closing stdin lets both workers exit their read loops cleanly.
    for proc in (initiator, responder):
        try:
            if proc.stdin is not None:
                proc.stdin.close()
        except (BrokenPipeError, ValueError):
            pass
    thread_i.join(timeout=30)
    thread_r.join(timeout=30)
    for proc in (initiator, responder):
        try:
            proc.wait(timeout=30)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=30)
    stderr_i = initiator.stderr.read() if initiator.stderr else ""
    stderr_r = responder.stderr.read() if responder.stderr else ""
    return RelayOutcome(
        session=session_id,
        initiator_final=finals.get("initiator->responder"),
        responder_final=finals.get("responder->initiator"),
        initiator_to_responder=i2r,
        responder_to_initiator=r2i,
        messages=messages,
        stderr_initiator=stderr_i,
        stderr_responder=stderr_r,
        returncodes=(initiator.returncode, responder.returncode),
        timed_out=timed_out,
    )
