"""Group-level reconciliation: per-table sessions plus atomic mirror publish.

The engine opens both dataset groups, verifies they are compatible, and runs
the existing single-table reconciliation session for every relation — compact
summaries, paged detail probes, and named local fallbacks are all reused
unchanged.  The per-table difference sets are then applied to the local
content and staged as the next mirror generation; the new generation becomes
visible to readers only if every table reproduces the peer's commitment and
every foreign key resolves in the staged final state.

The *group result identity* binds both ends' full dataset identities and the
per-table outcomes, so a result can be audited without re-running anything.
"""

from __future__ import annotations

import struct
import tempfile
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

from . import encoding
from .dataset import (
    GroupError,
    GroupExpectation,
    GroupStore,
    check_compatible,
)
from .encoding import record_row_id_bytes
from .initiator import DifferenceSet
from .mirror import Mirror, PublishReport
from .runner import run_session
from .summary import SummaryParams

DOMAIN_GROUP_RESULT = b"RECON:GROUP-RESULT:v1"


@dataclass
class TableReport:
    """The per-table outcome of a group reconciliation."""

    name: str
    complete: bool
    reason: str
    left_rows: int
    right_rows: int
    differences: DifferenceSet
    left_commit: bytes
    right_commit: bytes
    applied_commit: bytes
    sent_bytes: int
    received_bytes: int
    sent_frames: int
    received_frames: int
    fallback_regions: list[str]

    @property
    def wire_bytes(self) -> int:
        return self.sent_bytes + self.received_bytes

    def counts(self) -> dict[str, int]:
        return self.differences.counts()

    def difference_row_ids(self) -> dict[str, list[str]]:
        """The row ids of every confirmed difference, sorted, for audit."""

        def rid(record: bytes) -> str:
            return record_row_id_bytes(record).decode("utf-8")

        return {
            "only_left": [rid(r) for r in self.differences.only_left],
            "only_right": [rid(r) for r in self.differences.only_right],
            "changed": [rid(r) for r in self.differences.changed_left],
        }

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "complete": self.complete,
            "reason": self.reason,
            "left_rows": self.left_rows,
            "right_rows": self.right_rows,
            "differences": self.counts(),
            "difference_row_ids": self.difference_row_ids(),
            "left_commit": self.left_commit.hex(),
            "right_commit": self.right_commit.hex(),
            "applied_commit": self.applied_commit.hex(),
            "sent_bytes": self.sent_bytes,
            "received_bytes": self.received_bytes,
            "wire_bytes": self.wire_bytes,
            "sent_frames": self.sent_frames,
            "received_frames": self.received_frames,
            "fallback_regions": list(self.fallback_regions),
        }


@dataclass
class GroupRunResult:
    """The overall outcome of one group reconciliation (and publish, if run)."""

    complete: bool
    reason: str
    dataset_id: str
    left_batch: str
    right_batch: str
    manifest_version: int
    left_group_id: str
    right_group_id: str
    result_id: str
    tables: dict[str, TableReport]
    sent_bytes: int
    received_bytes: int
    publish: PublishReport | None = None
    generation_before: int | None = None
    generation_after: int | None = None

    @property
    def wire_bytes(self) -> int:
        return self.sent_bytes + self.received_bytes

    def to_dict(self) -> dict[str, Any]:
        return {
            "complete": self.complete,
            "reason": self.reason,
            "dataset_id": self.dataset_id,
            "left_batch": self.left_batch,
            "right_batch": self.right_batch,
            "manifest_version": self.manifest_version,
            "left_group_id": self.left_group_id,
            "right_group_id": self.right_group_id,
            "result_id": self.result_id,
            "sent_bytes": self.sent_bytes,
            "received_bytes": self.received_bytes,
            "wire_bytes": self.wire_bytes,
            "tables": {name: rep.to_dict() for name, rep in sorted(self.tables.items())},
            "publish": self.publish.to_dict() if self.publish else None,
            "generation_before": self.generation_before,
            "generation_after": self.generation_after,
        }


def compute_group_result_id(
    left: GroupStore,
    right: GroupStore,
    tables: Mapping[str, TableReport],
    complete: bool,
) -> str:
    """Bind both full dataset identities and every per-table outcome."""

    material = bytearray()
    material += encoding._lp(DOMAIN_GROUP_RESULT)
    material += encoding._lp(bytes.fromhex(left.group_id))
    material += encoding._lp(bytes.fromhex(right.group_id))
    material += encoding._lp(left.batch_id.encode("utf-8"))
    material += encoding._lp(right.batch_id.encode("utf-8"))
    material += struct.pack(">I", len(tables))
    for name in sorted(tables):
        report = tables[name]
        counts = report.counts()
        material += encoding._lp(name.encode("utf-8"))
        material += encoding._lp(report.left_commit)
        material += encoding._lp(report.right_commit)
        material += encoding._lp(report.applied_commit)
        material += struct.pack(
            ">QQQ", counts["only_left"], counts["only_right"], counts["changed"]
        )
        material += b"\x01" if report.complete else b"\x00"
    material += b"\x01" if complete else b"\x00"
    return encoding.sha256(bytes(material)).hex()


def rebuild_records(local_store, differences: DifferenceSet) -> list[bytes]:
    """Apply the peer's side of a per-table difference set to the local content.

    This is the group-level application: the rebuilt table is what the new
    mirror generation will store.  It is verified against the peer's table
    commitment before anything is published.
    """

    remove = {record_row_id_bytes(r) for r in differences.only_left}
    remove |= {record_row_id_bytes(r) for r in differences.changed_left}
    rebuilt: dict[bytes, bytes] = {}
    for record in local_store.iter_records():
        key = record_row_id_bytes(record)
        if key not in remove:
            rebuilt[key] = record
    for record in list(differences.only_right) + list(differences.changed_right):
        key = record_row_id_bytes(record)
        if key in rebuilt:
            raise GroupError(
                "apply_conflict",
                f"peer record for row {key!r} collides with a kept local row",
            )
        rebuilt[key] = record
    return [rebuilt[key] for key in sorted(rebuilt)]


def reconcile_group(
    left_dir: str | Path,
    right_dir: str | Path,
    *,
    params: SummaryParams | None = None,
    scratch_dir: str | Path | None = None,
    session_prefix: str | None = None,
) -> GroupRunResult:
    """Reconcile two dataset groups table by table (no mirror publish).

    Raises :class:`GroupCompatibilityError` when the two groups cannot form
    one valid reconciliation (missing tables, changed types, different
    manifests, different datasets, or mixed batches inside either group).
    """

    params = params or SummaryParams()
    left = GroupStore.open(left_dir)
    right = GroupStore.open(right_dir)
    try:
        check_compatible(left, right)
        if scratch_dir is not None:
            scratch = Path(scratch_dir)
            scratch.mkdir(parents=True, exist_ok=True)
        else:
            scratch = Path(tempfile.mkdtemp(prefix="recon-group-"))
        prefix = session_prefix or uuid.uuid4().hex
        reports: dict[str, TableReport] = {}
        for name in left.table_names():
            outcome = run_session(
                left.tables[name],
                right.tables[name],
                params=params,
                session=f"{prefix}-{name}",
                cache_path=scratch / f"answers-{name}.sqlite",
            )
            result = outcome.result
            reports[name] = TableReport(
                name=name,
                complete=result.complete,
                reason=result.reason,
                left_rows=left.tables[name].identity.row_count,
                right_rows=right.tables[name].identity.row_count,
                differences=result.differences,
                left_commit=result.self_commit,
                right_commit=result.peer_commit,
                applied_commit=result.applied_commit,
                sent_bytes=result.sent_bytes,
                received_bytes=result.received_bytes,
                sent_frames=result.sent_frames,
                received_frames=result.received_frames,
                fallback_regions=list(result.fallback_regions),
            )
        complete = all(report.complete for report in reports.values())
        if complete:
            reason = "ok"
        else:
            failed = [name for name, rep in reports.items() if not rep.complete]
            reason = f"incomplete tables: {', '.join(sorted(failed))}"
        result_id = compute_group_result_id(left, right, reports, complete)
        return GroupRunResult(
            complete=complete,
            reason=reason,
            dataset_id=left.dataset_id,
            left_batch=left.batch_id,
            right_batch=right.batch_id,
            manifest_version=left.manifest.version,
            left_group_id=left.group_id,
            right_group_id=right.group_id,
            result_id=result_id,
            tables=reports,
            sent_bytes=sum(rep.sent_bytes for rep in reports.values()),
            received_bytes=sum(rep.received_bytes for rep in reports.values()),
        )
    finally:
        left.close()
        right.close()


def reconcile_and_publish(
    left_dir: str | Path,
    right_dir: str | Path,
    mirror_path: str | Path,
    *,
    params: SummaryParams | None = None,
    scratch_dir: str | Path | None = None,
    session_prefix: str | None = None,
) -> GroupRunResult:
    """Reconcile two dataset groups and publish the peer's state to the mirror.

    The new generation is staged and verified in isolation; it becomes visible
    only when every table matches the peer's commitments and every foreign key
    resolves in the staged final state.  On any failure the result is
    incomplete and the old mirror generation stays current and unchanged.
    """

    result = reconcile_group(
        left_dir,
        right_dir,
        params=params,
        scratch_dir=scratch_dir,
        session_prefix=session_prefix,
    )
    mirror = Mirror(mirror_path)
    current = mirror.current_generation()
    result.generation_before = current
    result.generation_after = current
    if not result.complete:
        result.publish = PublishReport(
            published=False,
            reason=f"not_attempted:{result.reason}",
            generation_before=current,
            generation_after=current,
        )
        return result

    left = GroupStore.open(left_dir)
    right = GroupStore.open(right_dir)
    try:
        expectation = GroupExpectation.from_group(right)
        rebuilt = {
            name: rebuild_records(left.tables[name], result.tables[name].differences)
            for name in left.table_names()
        }
        report = mirror.stage_publish(
            rebuilt,
            expectation,
            result_id=result.result_id,
            left_group_id=result.left_group_id,
            right_group_id=result.right_group_id,
        )
    finally:
        left.close()
        right.close()
    result.publish = report
    result.generation_before = report.generation_before
    result.generation_after = report.generation_after
    if not report.published:
        result.complete = False
        result.reason = f"publish_failed:{report.reason}"
    return result
