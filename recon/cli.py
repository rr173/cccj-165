"""Command-line entry point: demo, benchmark, and two-process runs.

Nothing here is needed for the protocol; it exists to produce the README's
figures reproducibly.
"""

from __future__ import annotations

import argparse
import json
import shutil
import time
from pathlib import Path
from typing import Any

from . import data as demo_data
from .encoding import Schema
from .oracle import compare_inputs
from .runner import run_session
from .store import SnapshotStore, create_snapshot
from .summary import SummaryParams
from .twoprocess import run_two_process


def _schema() -> Schema:
    return demo_data.demo_schema()


def _bytes_for_full_rows(store: SnapshotStore, records: list[bytes] | None = None) -> dict[str, int]:
    """Exact canonical-size figures for comparison with protocol byte counts."""

    records = records if records is not None else list(store.iter_records())
    canonical = sum(len(r) for r in records)
    # Also report a fair wire figure: what the rows would cost as hex records
    # inside this protocol's JSON detail frames (2 hex chars per byte, plus
    # JSON quoting/object overhead approximated by the real encoder below).
    hex_total = sum(len(r.hex()) for r in records)
    json_total = len(json.dumps([r.hex() for r in records], separators=(",", ":")))
    return {
        "rows": len(records),
        "canonical_bytes": canonical,
        "hex_bytes": hex_total,
        "json_detail_payload_bytes": json_total,
        "mean_row_bytes": (canonical // len(records)) if records else 0,
    }


def _summarise_outcome(outcome) -> dict[str, Any]:
    r = outcome.result
    return {
        "complete": r.complete,
        "reason": r.reason,
        "differences": r.differences.counts(),
        "self_commit": r.self_commit.hex(),
        "peer_commit": r.peer_commit.hex(),
        "applied_commit": r.applied_commit.hex(),
        "commits_match": r.self_commit == r.peer_commit == r.applied_commit
        if r.self_commit == r.peer_commit
        else (r.applied_commit == r.peer_commit),
        "sent_bytes": r.sent_bytes,
        "received_bytes": r.received_bytes,
        "total_wire_bytes": r.sent_bytes + r.received_bytes,
        "sent_by_kind": r.sent_by_kind,
        "received_by_kind": r.received_by_kind,
        "sent_frames": r.sent_frames,
        "received_frames": r.received_frames,
        "summary_requests": r.summary_requests,
        "summary_regions": r.summary_regions,
        "cord_requests": r.cord_requests,
        "cord_bytes": r.cord_bytes,
        "probes": r.probes,
        "probe_rows": r.probe_rows,
        "fallbacks": r.fallbacks,
        "fallback_regions": r.fallback_regions,
        "fallback_region_count": len(r.fallback_regions),
        "fallback_rows": r.fallback_rows,
        "probe_requests": r.probes,
        "region_notes": [n.__dict__ for n in r.region_notes],
        "handshake_notes": r.handshake_notes,
        "duplicate_answers_ignored": r.duplicate_answers_ignored,
        "ignored_frames": r.ignored_frames,
        "exhaustive_rounds": r.exhaustive_rounds,
        "responder_stats": outcome.responder_stats.to_dict(),
        "log_bytes": outcome.log.total_sent("initiator") + outcome.log.total_sent("responder"),
        "log_entries": len(outcome.log.entries),
        "log_events": len(outcome.log.events()),
    }


def command_demo(args: argparse.Namespace) -> int:
    workdir = Path(args.workdir)
    if workdir.exists() and args.fresh:
        shutil.rmtree(workdir)
    workdir.mkdir(parents=True, exist_ok=True)
    schema = _schema()
    left_rows = demo_data.base_rows(args.rows)
    variant = (
        demo_data.variant_sparse(args.rows, seed=args.seed)
        if args.profile == "sparse"
        else demo_data.variant_dense(args.rows, seed=args.seed)
    )
    right_rows = variant.rows
    left_path = workdir / "left.sqlite"
    right_path = workdir / "right.sqlite"
    for p in (left_path, right_path):
        if p.exists():
            p.unlink()
    t0 = time.time()
    left_identity = create_snapshot(left_path, schema, left_rows, created_from="demo-left")
    right_identity = create_snapshot(right_path, schema, right_rows, created_from="demo-right")
    build_seconds = time.time() - t0
    params = SummaryParams(
        position_bits=args.position_bits,
        max_gen=args.max_gen,
        leaf_limit=args.leaf_limit,
        cord_max_bytes=args.cord_max_bytes,
        rows_per_chunk=args.rows_per_chunk,
        max_nodes_per_query=args.max_nodes_per_query,
    )
    if args.mode == "processes":
        outcome = run_two_process(
            left_path,
            right_path,
            params=params,
            duplicate_answers=args.duplicate_answers,
            answer_cache=workdir / "responder-answers.sqlite",
            timeout=args.timeout,
        )
        payload = {
            "mode": "processes",
            "session": outcome.session,
            "timed_out": outcome.timed_out,
            "returncodes": outcome.returncodes,
            "initiator": outcome.initiator_final,
            "responder": outcome.responder_final,
            "initiator_to_responder": {
                "frames": outcome.initiator_to_responder.frames,
                "bytes": outcome.initiator_to_responder.bytes,
                "by_kind": outcome.initiator_to_responder.by_kind,
            },
            "responder_to_initiator": {
                "frames": outcome.responder_to_initiator.frames,
                "bytes": outcome.responder_to_initiator.bytes,
                "by_kind": outcome.responder_to_initiator.by_kind,
            },
            "stderr_initiator": outcome.stderr_initiator,
            "stderr_responder": outcome.stderr_responder,
        }
        json.dump(payload, open(workdir / "two_process_run.json", "w"), indent=2, sort_keys=True)
        initiator_result = payload["initiator"]["result"] if payload["initiator"] else None
        result = {
            "mode": "processes",
            "session": payload["session"],
            "timed_out": payload["timed_out"],
            "returncodes": payload["returncodes"],
            "initiator_sent_bytes": payload["initiator_to_responder"]["bytes"],
            "responder_sent_bytes": payload["responder_to_initiator"]["bytes"],
            "total_wire_bytes": payload["initiator_to_responder"]["bytes"]
            + payload["responder_to_initiator"]["bytes"],
            "frames": payload["initiator_to_responder"]["frames"]
            + payload["responder_to_initiator"]["frames"],
            "by_kind": {
                "initiator->responder": payload["initiator_to_responder"]["by_kind"],
                "responder->initiator": payload["responder_to_initiator"]["by_kind"],
            },
            "result": initiator_result,
            "initiator_sent_by_kind_matches_engine": (
                initiator_result is not None
                and payload["initiator_to_responder"]["bytes"] == initiator_result["sent_bytes"]
            ),
        }
    else:
        left = SnapshotStore.open(left_path)
        right = SnapshotStore.open(right_path)
        outcome = run_session(
            left,
            right,
            params=params,
            duplicate_answers=args.duplicate_answers,
        )
        left.close()
        right.close()
        summary = _summarise_outcome(outcome)
        payload = {
            "mode": "threads",
            "left_snapshot": left_identity.snapshot_id(),
            "right_snapshot": right_identity.snapshot_id(),
            "left_rows": len(left_rows),
            "right_rows": len(right_rows),
            "build_seconds": round(build_seconds, 3),
            "summary_params": params.to_wire(),
            **summary,
        }
        json.dump(payload, open(workdir / "session_result.json", "w"), indent=2, sort_keys=True)
        result = payload
    expected = compare_inputs(left_rows, right_rows, [f[0] for f in demo_data.SCHEMA_FIELDS])
    left_store = SnapshotStore.open(left_path)
    right_store = SnapshotStore.open(right_path)
    sizes_left = _bytes_for_full_rows(left_store)
    sizes_right = _bytes_for_full_rows(right_store)
    left_store.close()
    right_store.close()
    print(
        json.dumps(
            {
                "result": result,
                "expected_difference_counts": expected.counts(),
                "full_left": sizes_left,
                "full_right": sizes_right,
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


def command_benchmark(args: argparse.Namespace) -> int:
    """Runs the README scenarios and writes a machine-readable report."""

    workdir = Path(args.workdir)
    workdir.mkdir(parents=True, exist_ok=True)
    schema = _schema()
    report: dict[str, Any] = {
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "cases": [],
    }

    def one(
        name: str,
        rows: int,
        profile: str,
        params: SummaryParams,
        *,
        seed: int = 7,
        mode: str = "threads",
        duplicate_answers: int = 0,
    ) -> dict[str, Any]:
        case_dir = workdir / name
        case_dir.mkdir(parents=True, exist_ok=True)
        left_rows = demo_data.base_rows(rows)
        variant = (
            demo_data.variant_sparse(rows, seed=seed)
            if profile == "sparse"
            else demo_data.variant_dense(rows, seed=seed)
        )
        right_rows = variant.rows
        left_path = case_dir / "left.sqlite"
        right_path = case_dir / "right.sqlite"
        for p in (left_path, right_path):
            if p.exists():
                p.unlink()
        t0 = time.time()
        create_snapshot(left_path, schema, left_rows, created_from=f"bench-{name}")
        create_snapshot(right_path, schema, right_rows, created_from=f"bench-{name}")
        build = time.time() - t0
        expected = compare_inputs(left_rows, right_rows, [f[0] for f in demo_data.SCHEMA_FIELDS])
        t1 = time.time()
        if mode == "processes":
            outcome = run_two_process(
                left_path, right_path, params=params, duplicate_answers=duplicate_answers,
                answer_cache=case_dir / "responder-answers.sqlite", timeout=1500,
            )
            reconcile_seconds = time.time() - t1
            res = outcome.initiator_final["result"] if outcome.initiator_final else None
            entry = {
                "case": name,
                "mode": "processes",
                "rows_left": len(left_rows),
                "rows_right": len(right_rows),
                "expected": expected.counts(),
                "complete": bool(res and res["complete"]),
                "reason": res["reason"] if res else "no_result",
                "differences": res["differences"] if res else None,
                "initiator_to_responder_bytes": outcome.initiator_to_responder.bytes,
                "responder_to_initiator_bytes": outcome.responder_to_initiator.bytes,
                "total_wire_bytes": outcome.initiator_to_responder.bytes
                + outcome.responder_to_initiator.bytes,
                "frames": (
                    outcome.initiator_to_responder.frames + outcome.responder_to_initiator.frames
                ),
                "frames_initiator_to_responder": outcome.initiator_to_responder.frames,
                "frames_responder_to_initiator": outcome.responder_to_initiator.frames,
                "by_kind": {
                    "i2r": outcome.initiator_to_responder.by_kind,
                    "r2i": outcome.responder_to_initiator.by_kind,
                },
                "fallbacks": res["fallbacks"] if res else None,
                "fallback_regions": res["fallback_regions"] if res else None,
                "fallback_rows": res["fallback_rows"] if res else None,
                "fallback_region_count": (
                    len(res["fallback_regions"]) if res and res.get("fallback_regions") else 0
                ),
                "probe_rows": res["probe_rows"] if res else None,
                "probe_requests": res["probes"] if res else None,
                "summary_requests": res["summary_requests"] if res else None,
                "cord_requests": res["cord_requests"] if res else None,
                "duplicate_answers_ignored": res["duplicate_answers_ignored"] if res else None,
                "exhaustive_rounds": res["exhaustive_rounds"] if res else None,
                "reconcile_seconds": round(reconcile_seconds, 3),
                "timed_out": outcome.timed_out,
                "returncodes": outcome.returncodes,
                "stderr_initiator": outcome.stderr_initiator[-2000:],
                "stderr_responder": outcome.stderr_responder[-2000:],
            }
            json.dump(entry, open(case_dir / "result.json", "w"), indent=2, sort_keys=True)
        else:
            left = SnapshotStore.open(left_path)
            right = SnapshotStore.open(right_path)
            outcome = run_session(left, right, params=params, duplicate_answers=duplicate_answers)
            reconcile_seconds = time.time() - t1
            left.close()
            right.close()
            s = _summarise_outcome(outcome)
            entry = {
                "case": name,
                "mode": "threads",
                "rows_left": len(left_rows),
                "rows_right": len(right_rows),
                "expected": expected.counts(),
                "complete": s["complete"],
                "reason": s["reason"],
                "differences": s["differences"],
                "sent_bytes": s["sent_bytes"],
                "received_bytes": s["received_bytes"],
                "total_wire_bytes": s["total_wire_bytes"],
                "sent_by_kind": s["sent_by_kind"],
                "received_by_kind": s["received_by_kind"],
                "sent_frames": s["sent_frames"],
                "received_frames": s["received_frames"],
                "summary_requests": s["summary_requests"],
                "cord_requests": s["cord_requests"],
                "probes": s["probes"],
                "probe_rows": s["probe_rows"],
                "fallbacks": s["fallbacks"],
                "fallback_regions": s["fallback_regions"],
                "fallback_region_count": len(s["fallback_regions"]),
                "fallback_rows": s["fallback_rows"],
                "probe_requests": s["probes"],
                "duplicate_answers_ignored": s["duplicate_answers_ignored"],
                "exhaustive_rounds": s["exhaustive_rounds"],
                "reconcile_seconds": round(reconcile_seconds, 3),
                "commits_match": s["commits_match"],
            }
            json.dump(entry, open(case_dir / "result.json", "w"), indent=2, sort_keys=True)
        left_store = SnapshotStore.open(left_path)
        entry["full_left"] = _bytes_for_full_rows(left_store)
        left_store.close()
        entry["build_seconds"] = round(build, 3)
        entry["summary_params"] = params.to_wire()
        return entry

    # Case 1+2: 100k rows, three differences, in-process threads and real
    # separate processes.  Byte counts are exact either way.
    for mode in ("threads", "processes"):
        report["cases"].append(
            one(
                f"sparse100k-{mode}",
                100_000,
                "sparse",
                SummaryParams(leaf_limit=64, cord_max_bytes=1 << 16, rows_per_chunk=4096),
                mode=mode,
            )
        )
    # Case 3: dense differences with a deliberately tiny summary budget, and a
    # narrow position width so real hash-bucket collisions occur.
    report["cases"].append(
        one(
            "dense-small-summary",
            20_000,
            "dense",
            SummaryParams(
                position_bits=10,
                max_gen=10,
                leaf_limit=8,
                cord_max_bytes=64,
                rows_per_chunk=512,
                max_nodes_per_query=64,
            ),
            mode="threads",
        )
    )
    # Case 4: identical snapshots must still complete over the protocol.
    case_dir = workdir / "identical"
    case_dir.mkdir(parents=True, exist_ok=True)
    left_rows = demo_data.base_rows(5_000)
    left_path = case_dir / "left.sqlite"
    right_path = case_dir / "right.sqlite"
    for p in (left_path, right_path):
        if p.exists():
            p.unlink()
    create_snapshot(left_path, schema, left_rows, created_from="identical")
    create_snapshot(right_path, schema, left_rows, created_from="identical")
    left = SnapshotStore.open(left_path)
    right = SnapshotStore.open(right_path)
    outcome = run_session(left, right, params=SummaryParams())
    left.close()
    right.close()
    s = _summarise_outcome(outcome)
    report["cases"].append(
        {
            "case": "identical-5000",
            "mode": "threads",
            "rows_left": len(left_rows),
            "rows_right": len(left_rows),
            "expected": {"only_left": 0, "only_right": 0, "changed": 0},
            "complete": s["complete"],
            "differences": s["differences"],
            "total_wire_bytes": s["total_wire_bytes"],
            "fallbacks": s["fallbacks"],
            "sent_frames": s["sent_frames"],
            "received_frames": s["received_frames"],
        }
    )
    # Case 5: empty snapshots on both sides.
    case_dir = workdir / "empty"
    case_dir.mkdir(parents=True, exist_ok=True)
    lp, rp = case_dir / "left.sqlite", case_dir / "right.sqlite"
    for p in (lp, rp):
        if p.exists():
            p.unlink()
    create_snapshot(lp, schema, [], created_from="empty")
    create_snapshot(rp, schema, [], created_from="empty")
    left = SnapshotStore.open(lp)
    right = SnapshotStore.open(rp)
    outcome = run_session(left, right, params=SummaryParams())
    left.close()
    right.close()
    s = _summarise_outcome(outcome)
    report["cases"].append(
        {
            "case": "empty",
            "mode": "threads",
            "rows_left": 0,
            "rows_right": 0,
            "expected": {"only_left": 0, "only_right": 0, "changed": 0},
            "complete": s["complete"],
            "differences": s["differences"],
            "total_wire_bytes": s["total_wire_bytes"],
            "fallbacks": s["fallbacks"],
        }
    )
    json.dump(report, open(workdir / "report.json", "w"), indent=2, sort_keys=True)
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="recon")
    parser.add_argument("--workdir", default="/tmp/recon-demo")
    parser.add_argument("--fresh", action="store_true")
    sub = parser.add_subparsers(dest="command", required=True)

    demo = sub.add_parser("demo")
    demo.add_argument("--rows", type=int, default=100_000)
    demo.add_argument("--profile", choices=["sparse", "dense"], default="sparse")
    demo.add_argument("--mode", choices=["threads", "processes"], default="processes")
    demo.add_argument("--seed", type=int, default=7)
    demo.add_argument("--leaf-limit", type=int, default=64)
    demo.add_argument("--cord-max-bytes", type=int, default=1 << 16)
    demo.add_argument("--rows-per-chunk", type=int, default=4096)
    demo.add_argument("--max-nodes-per-query", type=int, default=512)
    demo.add_argument("--position-bits", type=int, default=64)
    demo.add_argument("--max-gen", type=int, default=None)
    demo.add_argument("--duplicate-answers", type=int, default=0)
    demo.add_argument("--timeout", type=float, default=900.0)
    demo.set_defaults(func=command_demo)

    bench = sub.add_parser("benchmark")
    bench.set_defaults(func=command_benchmark)
    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
