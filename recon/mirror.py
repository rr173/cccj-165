"""Generation-based local mirror with atomic publish.

Layout::

    mirror/
      CURRENT                   # text pointer, e.g. "000002"
      generations/
        000001/                 # a complete dataset group (group.json + tables/)
          publish.json          # publish manifest (batch, commitments, ...)
        000002/
          ...

A publish builds the whole new generation in an isolated staging directory
first.  Only after *every* table's staged content reproduces the expected peer
commitment *and* every foreign key resolves in the staged final state does the
generation move into place and ``CURRENT`` flip by atomic rename.  Any failure
removes the staging directory; the old generation stays current and readable,
byte for byte.  Readers open the mirror through :meth:`Mirror.open_current`,
which yields an ordinary read-only :class:`~recon.dataset.GroupStore`.
"""

from __future__ import annotations

import json
import os
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

from .dataset import (
    GROUP_FORMAT,
    GroupExpectation,
    GroupStore,
    decode_table,
    find_fk_violations,
)
from .encoding import COMMIT_PARAMS, compute_commitment, decode_record
from .store import DatasetTag, create_snapshot

PUBLISH_FORMAT = "recon-mirror-manifest-v1"


class MirrorError(RuntimeError):
    """The mirror directory is corrupt, uninitialised, or unusable."""


@dataclass
class PublishReport:
    """The outcome of one publish attempt."""

    published: bool
    reason: str
    generation_before: int | None
    generation_after: int | None
    group_id: str | None = None
    manifest: dict[str, Any] | None = None
    fk_violations: list[dict[str, Any]] | None = None
    staged_rows: dict[str, int] | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "published": self.published,
            "reason": self.reason,
            "generation_before": self.generation_before,
            "generation_after": self.generation_after,
            "group_id": self.group_id,
            "manifest": self.manifest,
            "fk_violations": self.fk_violations,
            "staged_rows": self.staged_rows,
        }


def _write_json(path: Path, obj: Mapping[str, Any]) -> None:
    path.write_text(json.dumps(obj, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _flip_current(root: Path, generation: int) -> None:
    """Atomically move the CURRENT pointer to ``generation`` (rename)."""

    tmp = root / "CURRENT.tmp"
    tmp.write_text(f"{generation:06d}\n", encoding="utf-8")
    os.replace(tmp, root / "CURRENT")


def _group_json(
    dataset_id: str,
    batch_id: str,
    manifest,
    tables: list[str],
    schema_version: str | None = None,
) -> dict:
    doc = {
        "format": GROUP_FORMAT,
        "dataset_id": dataset_id,
        "batch_id": batch_id,
        "manifest": manifest.to_wire(),
        "tables": tables,
    }
    if schema_version is not None:
        doc["schema_version"] = schema_version
    return doc


class Mirror:
    """A local mirror of a dataset group, published in generations."""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)

    # ------------------------------------------------------------- readers

    def current_generation(self) -> int | None:
        pointer = self.path / "CURRENT"
        if not pointer.is_file():
            return None
        text = pointer.read_text(encoding="utf-8").strip()
        try:
            return int(text)
        except ValueError as exc:
            raise MirrorError(f"corrupt CURRENT pointer: {text!r}") from exc

    def generation_dir(self, generation: int) -> Path:
        return self.path / "generations" / f"{generation:06d}"

    def open_current(self) -> GroupStore:
        """Open the currently published generation as a read-only group."""

        generation = self.current_generation()
        if generation is None:
            raise MirrorError(f"mirror is not initialised: {self.path}")
        return GroupStore.open(self.generation_dir(generation))

    def publish_manifest(self, generation: int | None = None) -> dict[str, Any]:
        if generation is None:
            generation = self.current_generation()
            if generation is None:
                raise MirrorError(f"mirror is not initialised: {self.path}")
        return json.loads(
            (self.generation_dir(generation) / "publish.json").read_text(encoding="utf-8")
        )

    # -------------------------------------------------------- initialisation

    @staticmethod
    def initialize(path: str | Path, source: GroupStore) -> "Mirror":
        """Create a mirror whose generation 1 is a byte copy of ``source``."""

        root = Path(path)
        if root.exists() and any(root.iterdir()):
            raise MirrorError(f"refusing to initialise a non-empty mirror directory: {root}")
        (root / "generations").mkdir(parents=True, exist_ok=True)
        staging = root / "generations" / ".staging-000001"
        shutil.rmtree(staging, ignore_errors=True)
        staging.mkdir(parents=True)
        try:
            _copy_group_content(source, staging)
            _write_json(
                staging / "publish.json",
                {
                    "format": PUBLISH_FORMAT,
                    "dataset_id": source.dataset_id,
                    "batch_id": source.batch_id,
                    "schema_version": source.schema_version,
                    "manifest_version": source.manifest.version,
                    "manifest_hash": source.manifest.content_hash().hex(),
                    "generation": 1,
                    "previous_generation": None,
                    "group_id": source.group_id,
                    "result_id": None,
                    "source": {"left_group_id": source.group_id, "right_group_id": None},
                    "tables": _table_commitments(source),
                    "note": "initialised from the local source group",
                },
            )
            os.replace(staging, root / "generations" / "000001")
            _flip_current(root, 1)
        except Exception:
            shutil.rmtree(staging, ignore_errors=True)
            raise
        return Mirror(root)

    # -------------------------------------------------------------- publish

    def stage_publish(
        self,
        rebuilt: Mapping[str, list[bytes]],
        expectation: GroupExpectation,
        *,
        result_id: str | None,
        left_group_id: str | None,
        right_group_id: str | None,
    ) -> PublishReport:
        """Stage, verify, and atomically publish the next generation.

        ``rebuilt`` holds the final canonical records of every table (the
        local content with the peer's differences applied).  The staged
        generation must reproduce ``expectation`` exactly — every table's
        content commitment and the whole-group identity — and every foreign
        key must resolve in the staged final state.  On any mismatch nothing
        is published and the current generation is untouched.
        """

        before = self.current_generation()
        if before is None:
            raise MirrorError(f"mirror is not initialised: {self.path}")
        expected_tables = dict(expectation.table_commitments)

        def fail(reason: str, *, violations=None) -> PublishReport:
            return PublishReport(
                published=False,
                reason=reason,
                generation_before=before,
                generation_after=before,
                fk_violations=violations,
            )

        # 1. The rebuilt content must cover exactly the expected table set and
        #    reproduce every table's commitment before anything is staged.
        if set(rebuilt) != set(expected_tables):
            return fail(
                "table_set_incomplete: rebuilt tables "
                f"{sorted(rebuilt)} != expected {sorted(expected_tables)}"
            )
        for name in sorted(expected_tables):
            schema = expectation.schemas[name]
            commitment = compute_commitment(schema, COMMIT_PARAMS, rebuilt[name])
            if commitment != expected_tables[name]:
                return fail(f"commitment_mismatch:{name}")

        # 2. Build the whole new generation in an isolated staging directory.
        new_generation = before + 1
        staging = self.path / "generations" / f".staging-{new_generation:06d}"
        shutil.rmtree(staging, ignore_errors=True)
        (staging / "tables").mkdir(parents=True, exist_ok=True)
        try:
            for name in sorted(expected_tables):
                schema = expectation.schemas[name]
                rows = [decode_record(schema, record) for record in rebuilt[name]]
                create_snapshot(
                    staging / "tables" / f"{name}.sqlite",
                    schema,
                    rows,
                    created_from=(
                        f"mirror:{expectation.dataset_id}:{expectation.batch_id}:{name}"
                    ),
                    dataset=DatasetTag(expectation.dataset_id, expectation.batch_id, name),
                )
            _write_json(
                staging / "group.json",
                _group_json(
                    expectation.dataset_id,
                    expectation.batch_id,
                    expectation.manifest,
                    sorted(expected_tables),
                    expectation.schema_version,
                ),
            )

            # 3. Re-open the staged generation from disk: every table's
            #    commitment is recomputed from the stored bytes and must match
            #    the expectation, and the group identity must bind exactly the
            #    peer's dataset state.
            staged = GroupStore.open(staging)
            try:
                for name in sorted(expected_tables):
                    if staged.tables[name].identity.commitment != expected_tables[name]:
                        return fail(f"staged_commitment_mismatch:{name}")
                if staged.group_id != expectation.group_id:
                    return fail("group_id_mismatch")
                # 4. Foreign keys are checked against the staged *final* state
                #    of the whole group, never against intermediate tables.
                staged_rows = {
                    name: decode_table(store) for name, store in staged.tables.items()
                }
                violations = find_fk_violations(staged_rows, expectation.manifest)
                if violations:
                    return fail("fk_violation", violations=violations)
                staged_counts = {name: len(rows) for name, rows in staged_rows.items()}
            finally:
                staged.close()

            # 5. Everything verified: record the publish manifest, move the
            #    generation into place, and flip CURRENT atomically.
            manifest_doc = {
                "format": PUBLISH_FORMAT,
                "dataset_id": expectation.dataset_id,
                "batch_id": expectation.batch_id,
                "schema_version": expectation.schema_version,
                "manifest_version": expectation.manifest.version,
                "manifest_hash": expectation.manifest.content_hash().hex(),
                "generation": new_generation,
                "previous_generation": before,
                "group_id": expectation.group_id,
                "result_id": result_id,
                "source": {
                    "left_group_id": left_group_id,
                    "right_group_id": right_group_id,
                },
                "tables": {
                    name: {
                        "commitment": expected_tables[name].hex(),
                        "rows": staged_counts[name],
                    }
                    for name in sorted(expected_tables)
                },
            }
            _write_json(staging / "publish.json", manifest_doc)
            target = self.generation_dir(new_generation)
            if target.exists():
                # An orphan from a crashed earlier attempt; CURRENT can never
                # have pointed at a generation newer than ``before``.
                shutil.rmtree(target)
            os.replace(staging, target)
            _flip_current(self.path, new_generation)
            return PublishReport(
                published=True,
                reason="ok",
                generation_before=before,
                generation_after=new_generation,
                group_id=expectation.group_id,
                manifest=manifest_doc,
                staged_rows=staged_counts,
            )
        finally:
            # A failed attempt must leave no staged content behind; a
            # successful one has already moved the directory into place.
            shutil.rmtree(staging, ignore_errors=True)


def _copy_group_content(source: GroupStore, staging: Path) -> None:
    tables_dir = staging / "tables"
    tables_dir.mkdir(parents=True, exist_ok=True)
    for name in sorted(source.tables):
        store = source.tables[name]
        target = tables_dir / f"{name}.sqlite"
        shutil.copyfile(store.path, target)
        os.chmod(target, 0o444)
    _write_json(
        staging / "group.json",
        _group_json(
            source.dataset_id,
            source.batch_id,
            source.manifest,
            sorted(source.tables),
            source.schema_version,
        ),
    )


def _table_commitments(group: GroupStore) -> dict[str, dict[str, Any]]:
    return {
        name: {
            "commitment": store.identity.commitment.hex(),
            "rows": store.identity.row_count,
        }
        for name, store in sorted(group.tables.items())
    }
