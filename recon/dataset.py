"""Relational dataset groups: named table snapshots plus a relationship manifest.

A *dataset group* is a directory::

    group.json              # format, dataset id, batch id, manifest, table list
    tables/<name>.sqlite    # one immutable snapshot per relation

Every table file is an ordinary immutable snapshot (see :mod:`recon.store`)
that additionally carries a ``dataset_meta`` row: the stable dataset id, the
collection batch id, and the table name.  Loading a group re-verifies every
table (each store recomputes its commitment from the stored bytes) and then
requires all table files to agree on the dataset id and batch id: a group can
never be assembled from mixed batches or borrowed tables.

The relationship manifest declares foreign keys ``child_table.child_column ->
parent_table.row_id``.  Row ids are text, so a foreign-key column must be a
text column whose nullability matches the declaration; a nullable foreign key
allows NULL.  The manifest carries an explicit version and a content hash, so
both ends can prove they reconciled under the same relationship definitions.

The *group identity* binds the dataset id, the batch id, the manifest hash,
and every table's name, schema, and content commitment.  Two groups are
compatible only when they share the dataset id, the table set, every table's
schema, and the manifest; anything else is an explicit error, never an
implicit merge of missing tables, changed types, or mixed batches.

Foreign-key *validity* of the content is deliberately not checked at load
time: a snapshot may legitimately contain dangling references, and it is the
mirror publish step that refuses to make such a state visible to readers.
"""

from __future__ import annotations

import json
import re
import struct
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping

from . import encoding
from .encoding import Schema, decode_record
from .store import DatasetTag, SnapshotStore, create_snapshot

GROUP_FORMAT = "recon-group-v1"
DEFAULT_MANIFEST_VERSION = 1

DOMAIN_MANIFEST = b"RECON:MANIFEST:v1"
DOMAIN_GROUP_ID = b"RECON:GROUP-ID:v1"

_TABLE_NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]*$")


class GroupError(RuntimeError):
    """A dataset group is malformed, inconsistent, or incompatible."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(f"{code}: {message}")
        self.code = code
        self.message = message


class GroupCompatibilityError(GroupError):
    """The two groups cannot form one valid reconciliation."""


def _check_table_name(name: object) -> str:
    if not isinstance(name, str) or not _TABLE_NAME_RE.match(name):
        raise GroupError("table_name", f"unsafe table name {name!r}")
    return name


# ---------------------------------------------------------------------------
# Relationship manifest


@dataclass(frozen=True)
class ForeignKey:
    """``child_table.child_column`` references ``parent_table.row_id``."""

    child_table: str
    child_column: str
    parent_table: str
    nullable: bool = False

    def __post_init__(self) -> None:
        for value in (self.child_table, self.child_column, self.parent_table):
            if not isinstance(value, str) or not value:
                raise GroupError("manifest", "foreign key fields must be non-empty strings")
        if not isinstance(self.nullable, bool):
            raise GroupError("manifest", "foreign key nullability must be a bool")

    def canonical(self) -> dict[str, Any]:
        return {
            "child_column": self.child_column,
            "child_table": self.child_table,
            "nullable": self.nullable,
            "parent_table": self.parent_table,
        }

    @staticmethod
    def from_wire(obj: Any) -> "ForeignKey":
        if not isinstance(obj, Mapping):
            raise GroupError("manifest", "foreign key must be an object")
        try:
            return ForeignKey(
                child_table=obj["child_table"],
                child_column=obj["child_column"],
                parent_table=obj["parent_table"],
                nullable=obj["nullable"],
            )
        except KeyError as exc:
            raise GroupError("manifest", f"foreign key lacks field {exc}") from exc


@dataclass(frozen=True)
class RelationManifest:
    """The versioned set of foreign-key declarations of a dataset group."""

    version: int
    foreign_keys: tuple[ForeignKey, ...]

    @staticmethod
    def declare(version: int, foreign_keys: Iterable[ForeignKey]) -> "RelationManifest":
        if isinstance(version, bool) or not isinstance(version, int) or version < 1:
            raise GroupError("manifest", "manifest version must be a positive integer")
        fks = tuple(
            sorted(foreign_keys, key=lambda fk: (fk.child_table, fk.child_column, fk.parent_table))
        )
        seen: set[tuple[str, str]] = set()
        for fk in fks:
            if not isinstance(fk, ForeignKey):
                raise GroupError("manifest", "foreign keys must be ForeignKey instances")
            key = (fk.child_table, fk.child_column)
            if key in seen:
                raise GroupError(
                    "duplicate_foreign_key",
                    f"two foreign keys declared on {fk.child_table}.{fk.child_column}",
                )
            seen.add(key)
        return RelationManifest(version, fks)

    def to_wire(self) -> dict[str, Any]:
        return {
            "version": self.version,
            "foreign_keys": [fk.canonical() for fk in self.foreign_keys],
        }

    @staticmethod
    def from_wire(obj: Any) -> "RelationManifest":
        if not isinstance(obj, Mapping):
            raise GroupError("manifest", "manifest must be an object")
        if "version" not in obj or "foreign_keys" not in obj:
            raise GroupError("manifest", "manifest needs 'version' and 'foreign_keys'")
        fks = obj["foreign_keys"]
        if not isinstance(fks, list):
            raise GroupError("manifest", "foreign_keys must be a list")
        return RelationManifest.declare(
            obj["version"], [ForeignKey.from_wire(fk) for fk in fks]
        )

    def canonical_bytes(self) -> bytes:
        return json.dumps(self.to_wire(), sort_keys=True, separators=(",", ":")).encode("utf-8")

    def content_hash(self) -> bytes:
        return encoding.sha256(
            encoding._lp(DOMAIN_MANIFEST) + encoding._lp(self.canonical_bytes())
        )


# ---------------------------------------------------------------------------
# Group identity


def compute_group_id(
    dataset_id: str,
    batch_id: str,
    manifest: RelationManifest,
    tables: Mapping[str, SnapshotStore],
) -> str:
    """Bind dataset id, batch id, manifest, and every table's schema+content."""

    material = bytearray()
    material += encoding._lp(DOMAIN_GROUP_ID)
    material += encoding._lp(dataset_id.encode("utf-8"))
    material += encoding._lp(batch_id.encode("utf-8"))
    material += encoding._lp(manifest.content_hash())
    material += struct.pack(">I", len(tables))
    for name in sorted(tables):
        identity = tables[name].identity
        material += encoding._lp(name.encode("utf-8"))
        material += encoding._lp(identity.schema_wire)
        material += encoding._lp(identity.commitment)
    return encoding.sha256(bytes(material)).hex()


# ---------------------------------------------------------------------------
# Group store


class GroupStore:
    """Read-only access to one verified dataset group."""

    def __init__(
        self,
        path: Path,
        dataset_id: str,
        batch_id: str,
        manifest: RelationManifest,
        tables: dict[str, SnapshotStore],
        schema_version: str | None = None,
    ) -> None:
        self.path = Path(path)
        self.dataset_id = dataset_id
        self.batch_id = batch_id
        self.manifest = manifest
        self._tables = tables
        self.schema_version = schema_version
        self.group_id = compute_group_id(dataset_id, batch_id, manifest, tables)

    @staticmethod
    def open(path: str | Path) -> "GroupStore":
        root = Path(path)
        meta_path = root / "group.json"
        if not meta_path.is_file():
            raise GroupError("group_format", f"group manifest not found: {meta_path}")
        try:
            meta = json.loads(meta_path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            raise GroupError("group_format", f"group manifest is not valid JSON: {exc}") from exc
        if not isinstance(meta, dict) or meta.get("format") != GROUP_FORMAT:
            raise GroupError("group_format", f"unrecognised group format in {meta_path}")
        dataset_id = meta.get("dataset_id")
        batch_id = meta.get("batch_id")
        if not isinstance(dataset_id, str) or not dataset_id:
            raise GroupError("group_format", "dataset_id must be a non-empty string")
        if not isinstance(batch_id, str) or not batch_id:
            raise GroupError("group_format", "batch_id must be a non-empty string")
        schema_version = meta.get("schema_version")
        if schema_version is not None and (
            not isinstance(schema_version, str) or not schema_version
        ):
            raise GroupError(
                "group_format", "schema_version must be a non-empty string when present"
            )
        manifest = RelationManifest.from_wire(meta.get("manifest"))
        names = meta.get("tables")
        if (
            not isinstance(names, list)
            or not names
            or any(not isinstance(n, str) for n in names)
        ):
            raise GroupError("group_format", "tables must be a non-empty list of names")
        if len(set(names)) != len(names):
            raise GroupError("group_format", "duplicate table names in the group manifest")

        stores: dict[str, SnapshotStore] = {}
        try:
            for name in sorted(names):
                _check_table_name(name)
                table_path = root / "tables" / f"{name}.sqlite"
                if not table_path.is_file():
                    raise GroupError("table_missing", f"table file missing: {table_path}")
                store = SnapshotStore.open(table_path)
                tag = store.dataset_tag()
                if tag is None:
                    store.close()
                    raise GroupError(
                        "dataset_tag_missing",
                        f"table {name!r} carries no dataset/batch tag",
                    )
                if tag.dataset_id != dataset_id:
                    store.close()
                    raise GroupError(
                        "dataset_inconsistent",
                        f"table {name!r} belongs to dataset {tag.dataset_id!r}, "
                        f"the group claims {dataset_id!r}",
                    )
                if tag.batch_id != batch_id:
                    store.close()
                    raise GroupError(
                        "batch_inconsistent",
                        f"table {name!r} is from batch {tag.batch_id!r}, "
                        f"the group claims {batch_id!r}",
                    )
                if tag.table_name != name:
                    store.close()
                    raise GroupError(
                        "table_name_mismatch",
                        f"table file for {name!r} is tagged as {tag.table_name!r}",
                    )
                stores[name] = store
            _validate_manifest_against_schemas(manifest, stores)
        except Exception:
            for store in stores.values():
                store.close()
            raise
        return GroupStore(root, dataset_id, batch_id, manifest, stores, schema_version)

    @property
    def tables(self) -> dict[str, SnapshotStore]:
        return self._tables

    def table_names(self) -> list[str]:
        return sorted(self._tables)

    def close(self) -> None:
        for store in self._tables.values():
            store.close()

    def __enter__(self) -> "GroupStore":
        return self

    def __exit__(self, *exc) -> None:
        self.close()


def _validate_manifest_against_schemas(
    manifest: RelationManifest, stores: Mapping[str, SnapshotStore]
) -> None:
    validate_manifest_against_schema_map(
        manifest, {name: store.schema for name, store in stores.items()}
    )


def validate_manifest_against_schema_map(
    manifest: RelationManifest, schemas: Mapping[str, Schema]
) -> None:
    """Require every foreign key to reference a declared, compatible column.

    This is the schema-map form of the group loader's manifest check, shared
    with the migration catalog validator: foreign keys must point at tables
    and columns that exist, a reference to a row id must be a text column,
    and the declared nullability must match the column's.
    """

    for fk in manifest.foreign_keys:
        if fk.child_table not in schemas:
            raise GroupError(
                "manifest_unknown_table",
                f"foreign key child table {fk.child_table!r} is not in the group",
            )
        if fk.parent_table not in schemas:
            raise GroupError(
                "manifest_unknown_table",
                f"foreign key parent table {fk.parent_table!r} is not in the group",
            )
        schema = schemas[fk.child_table]
        if fk.child_column not in schema.field_names():
            raise GroupError(
                "manifest_unknown_column",
                f"foreign key column {fk.child_table}.{fk.child_column} is not in the schema",
            )
        ftype = schema.type_of(fk.child_column)
        if ftype != "text":
            raise GroupError(
                "fk_type_mismatch",
                f"foreign key {fk.child_table}.{fk.child_column} has type {ftype!r}; "
                "a reference to row_id must be a text column",
            )
        if schema.is_nullable(fk.child_column) != fk.nullable:
            raise GroupError(
                "fk_nullability_mismatch",
                f"foreign key {fk.child_table}.{fk.child_column} declares "
                f"nullable={fk.nullable} but the column declares "
                f"nullable={schema.is_nullable(fk.child_column)}",
            )


# ---------------------------------------------------------------------------
# Writing groups


def create_group(
    path: str | Path,
    dataset_id: str,
    batch_id: str,
    tables: Mapping[str, tuple[Schema, Iterable[tuple[str, Mapping[str, object]]]]],
    manifest: RelationManifest,
    *,
    schema_version: str | None = None,
) -> str:
    """Write a brand-new dataset group and return its group identity.

    Every table file is tagged with the same dataset id and batch id, so the
    group is batch-consistent by construction.  Structural validation
    (manifest sanity, batch consistency) happens when the group is *opened*;
    this writer deliberately only stamps what it is given, so tests can build
    deliberately broken groups and watch the loader reject them.

    ``schema_version`` is an optional declarative label recorded in
    ``group.json``; the migration planner validates it against the catalog's
    declared structure for that version.  It is metadata only and never
    enters the group identity (which already binds the actual schemas).
    """

    root = Path(path)
    if root.exists() and any(root.iterdir()):
        raise GroupError("group_exists", f"refusing to overwrite non-empty directory: {root}")
    if not isinstance(manifest, RelationManifest):
        raise GroupError("manifest", "manifest must be a RelationManifest")
    if not isinstance(dataset_id, str) or not dataset_id:
        raise GroupError("group_format", "dataset_id must be a non-empty string")
    if not isinstance(batch_id, str) or not batch_id:
        raise GroupError("group_format", "batch_id must be a non-empty string")
    if schema_version is not None and (
        not isinstance(schema_version, str) or not schema_version
    ):
        raise GroupError(
            "group_format", "schema_version must be a non-empty string when given"
        )
    names = sorted(tables)
    if not names:
        raise GroupError("group_format", "a group needs at least one table")
    (root / "tables").mkdir(parents=True, exist_ok=True)
    for name in names:
        _check_table_name(name)
        schema, rows = tables[name]
        create_snapshot(
            root / "tables" / f"{name}.sqlite",
            schema,
            rows,
            created_from=f"group:{dataset_id}:{batch_id}:{name}",
            dataset=DatasetTag(dataset_id, batch_id, name),
        )
    meta = {
        "format": GROUP_FORMAT,
        "dataset_id": dataset_id,
        "batch_id": batch_id,
        "manifest": manifest.to_wire(),
        "tables": names,
    }
    if schema_version is not None:
        meta["schema_version"] = schema_version
    (root / "group.json").write_text(
        json.dumps(meta, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    # Compute the group identity from the table stores directly, without the
    # loader's structural validation: deliberately broken groups (a manifest
    # referencing a missing table, a mixed batch, ...) must still be writable
    # so the *loader* can be the one to reject them.
    stores: dict[str, SnapshotStore] = {}
    try:
        for name in names:
            stores[name] = SnapshotStore.open(root / "tables" / f"{name}.sqlite")
        return compute_group_id(dataset_id, batch_id, manifest, stores)
    finally:
        for store in stores.values():
            store.close()


# ---------------------------------------------------------------------------
# Cross-end compatibility


def check_compatible(left: GroupStore, right: GroupStore) -> None:
    """Require the two groups to form one valid reconciliation.

    The dataset id, the table set, every table's schema, and the relationship
    manifest must agree.  The *batch ids* may differ: reconciling an older
    local batch against a newer peer batch is the normal case, and both batch
    identities are recorded in the result.  Anything else is an explicit
    error, never an implicit merge.
    """

    if left.dataset_id != right.dataset_id:
        raise GroupCompatibilityError(
            "dataset_mismatch",
            f"dataset ids differ: {left.dataset_id!r} vs {right.dataset_id!r}",
        )
    left_names = set(left.tables)
    right_names = set(right.tables)
    if left_names != right_names:
        raise GroupCompatibilityError(
            "table_set_mismatch",
            f"table sets differ: only left {sorted(left_names - right_names)}, "
            f"only right {sorted(right_names - left_names)}",
        )
    for name in sorted(left_names):
        left_sig = left.tables[name].schema.signature()
        right_sig = right.tables[name].schema.signature()
        if left_sig != right_sig:
            raise GroupCompatibilityError(
                "schema_mismatch",
                f"table {name!r} schema differs: {left_sig} vs {right_sig}",
            )
    if left.manifest.content_hash() != right.manifest.content_hash():
        raise GroupCompatibilityError(
            "manifest_mismatch",
            "relationship manifests differ (version or foreign keys)",
        )


# ---------------------------------------------------------------------------
# Expectation and foreign-key scan


@dataclass(frozen=True)
class GroupExpectation:
    """What a published generation must reproduce, derived from the peer group."""

    dataset_id: str
    batch_id: str
    manifest: RelationManifest
    schemas: Mapping[str, Schema]
    table_commitments: Mapping[str, bytes]
    group_id: str
    schema_version: str | None = None

    @staticmethod
    def from_group(group: GroupStore) -> "GroupExpectation":
        return GroupExpectation(
            dataset_id=group.dataset_id,
            batch_id=group.batch_id,
            manifest=group.manifest,
            schemas={name: store.schema for name, store in group.tables.items()},
            table_commitments={
                name: store.identity.commitment for name, store in group.tables.items()
            },
            group_id=group.group_id,
            schema_version=group.schema_version,
        )


def decode_table(store: SnapshotStore) -> dict[str, dict[str, Any]]:
    """Decode one table store into ``row_id -> values``."""

    rows: dict[str, dict[str, Any]] = {}
    for record in store.iter_records():
        row_id, values = decode_record(store.schema, record)
        rows[row_id] = values
    return rows


def find_fk_violations(
    table_rows: Mapping[str, Mapping[str, Mapping[str, Any]]],
    manifest: RelationManifest,
) -> list[dict[str, Any]]:
    """Scan the *final* whole-group state for dangling foreign keys.

    ``table_rows`` maps table name -> row id -> decoded values.  Every non-NULL
    foreign key must resolve to a parent row id present in the same final
    state.  The check is over the complete group at once, so the processing
    order of tables is irrelevant: parents and children added in the same
    batch, same-batch deletions, re-pointed children, and cycles through
    nullable relations are all judged on the final state only.  Nothing is
    skipped to make the check pass.
    """

    violations: list[dict[str, Any]] = []
    for fk in manifest.foreign_keys:
        parent_ids = set(table_rows.get(fk.parent_table, {}))
        child_rows = table_rows.get(fk.child_table, {})
        for row_id in sorted(child_rows):
            value = child_rows[row_id].get(fk.child_column)
            if value is None:
                continue  # nullable foreign keys allow NULL (declaration validated)
            if value not in parent_ids:
                violations.append(
                    {
                        "child_table": fk.child_table,
                        "child_row_id": row_id,
                        "column": fk.child_column,
                        "value": value,
                        "parent_table": fk.parent_table,
                    }
                )
    return violations
