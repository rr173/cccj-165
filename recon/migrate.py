"""Explicit schema-migration catalogs for cross-version dataset reconciliation.

Two sites run the same relational dataset reconciliation tooling but upgrade
on different schedules: one still emits an older schema while the other has
renamed fields, added columns, or changed enum values.  This module lets both
ends declare a *schema version* per dataset group and reconcile through an
explicit **migration catalog** instead of requiring identical table
structures.

Catalog model
-------------
A catalog is a set of declared *versions* (each with the full per-table
schema and the relationship manifest in effect at that version) plus directed
*version steps*.  Each step declares, for every table of the dataset, how the
target version's fields are derived:

* ``copy`` / ``rename`` — keep a field, optionally under a new name;
* ``default`` — add a field with an explicit constant default value;
* dropping — a source field referenced by no rule simply does not enter the
  target schema;
* ``enum`` — an explicit one-to-one rewrite of a text enumeration;
* ``convert`` — a strict ``int64`` <-> decimal-text conversion.

Planning (see :func:`plan_migrations`) happens **before any row is read for
conversion**: the catalog must be a DAG, the path from each end's source
version to the target version must exist and be unique, every step must cover
every table, field mappings must be complete and unambiguous, and every
version's relationship manifest must be mappable onto its declared schemas.
Any violation is a :class:`MigrationPlanError` carrying a stable ``code`` and
a locatable ``context`` (version, step, table, field).  The planner never
picks one of several routes on its own.

Execution (see :func:`execute_migration`) converts rows deterministically,
preserving ``row_id`` and validating every value against the declared input
and output types of each step.  There is no rounding, no truncation, no
implicit bool/int coercion, no silent null handling, and no replacement of
illegal values by defaults: an out-of-range integer, a malformed decimal
text, an unlisted enum value, or a null violating a non-nullable target
aborts the migration with a :class:`MigrationConversionError` that reports
step, table, row id, field, and category for every offending row.

Source snapshots stay read-only.  Migrated results are written to isolated
temporary dataset groups whose batch labels derive deterministically from the
source batch, the step's target version, and the catalog identity, so the
same source version, target version, and catalog content always yield the
same transformation identity.  Only after both ends have been migrated to the
same target table set, schemas, and relationship manifest does the run enter
the existing compact-summary group reconciliation
(:func:`recon.grouprecon.reconcile_and_publish`); foreign keys are checked
against the final whole-group state under the *target* manifest, and the
mirror is published atomically only when every table reconciles, every
commitment matches, and every reference resolves.
"""

from __future__ import annotations

import json
import re
import shutil
import struct
import tempfile
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable, Mapping

from . import encoding
from .dataset import (
    GroupError,
    GroupStore,
    RelationManifest,
    check_compatible,
    create_group,
    validate_manifest_against_schema_map,
)
from .encoding import INT64_MAX, INT64_MIN, Schema, decode_record, encode_record
from .grouprecon import GroupRunResult, reconcile_and_publish, reconcile_group
from .mirror import Mirror
from .summary import SummaryParams

CATALOG_FORMAT = "recon-migration-catalog-v1"

DOMAIN_CATALOG = b"RECON:MIGRATION-CATALOG:v1"
DOMAIN_TRANSFORM = b"RECON:TRANSFORM:v1"

CONVERT_TEXT_TO_INT64 = "text_to_int64"
CONVERT_INT64_TO_TEXT = "int64_to_text"
CONVERSIONS = (CONVERT_TEXT_TO_INT64, CONVERT_INT64_TO_TEXT)

#: Conversion failures are collected across the whole failing step so the
#: report can list every offending row; the cap keeps a pathological input
#: from exhausting memory.  The truncated flag is set when the cap is hit.
FAILURE_CAP = 1024

_NO_DEFAULT = object()

_DECIMAL_TEXT_RE = re.compile(r"^-?[0-9]+$")


class MigrationPlanError(RuntimeError):
    """A catalog, version, or planning problem detected before any conversion.

    Always carries a stable ``code`` and a ``context`` mapping that locates
    the problem (version, step, table, field, ...) so the caller can fix the
    catalog or the input and retry.
    """

    def __init__(self, code: str, message: str, context: Mapping[str, Any] | None = None):
        super().__init__(f"{code}: {message}")
        self.code = code
        self.message = message
        self.context = dict(context or {})

    def to_dict(self) -> dict[str, Any]:
        return {"code": self.code, "message": self.message, "context": self.context}


class MigrationConversionError(RuntimeError):
    """One or more rows could not be converted by a migration step.

    ``failures`` lists every offending row (up to :data:`FAILURE_CAP`) with
    the version step, table, row id, field, and a stable ``category``:
    ``enum_unmapped``, ``bad_decimal_format``, ``int64_out_of_range``, or
    ``null_violation``.  No difference result is ever produced from a failed
    migration.
    """

    code = "conversion_failed"

    def __init__(
        self,
        step_label: str,
        failures: list[dict[str, Any]],
        *,
        truncated: bool = False,
    ) -> None:
        self.step_label = step_label
        self.failures = failures
        self.truncated = truncated
        first = failures[0] if failures else {}
        super().__init__(
            f"conversion_failed: step {step_label} rejected {len(failures)}"
            f"{'+' if truncated else ''} row(s); first: "
            f"table={first.get('table')!r} row_id={first.get('row_id')!r} "
            f"field={first.get('field')!r} category={first.get('category')!r}"
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "code": self.code,
            "message": str(self),
            "context": {"step": self.step_label, "truncated": self.truncated},
            "failures": list(self.failures),
        }


# ---------------------------------------------------------------------------
# Field rules


@dataclass(frozen=True)
class FieldRule:
    """How one target field is derived within a version step.

    Exactly one of ``source`` (copy/rename, optionally with an enum rewrite
    or a type conversion) or a constant ``default`` (for a field that does
    not exist in the source schema) may be given.
    """

    target: str
    source: str | None = None
    default_value: Any = _NO_DEFAULT
    enum_map: tuple[tuple[str, str], ...] | None = None
    conversion: str | None = None

    @staticmethod
    def copy(target: str, source: str | None = None) -> "FieldRule":
        """Keep a field, optionally under a new name."""

        return FieldRule(target=target, source=source if source is not None else target)

    @staticmethod
    def rename(target: str, source: str) -> "FieldRule":
        return FieldRule(target=target, source=source)

    @staticmethod
    def default(target: str, value: Any) -> "FieldRule":
        """Add a field filled with an explicit constant default value."""

        return FieldRule(target=target, default_value=value)

    @staticmethod
    def enum(target: str, source: str, mapping: Mapping[str, str]) -> "FieldRule":
        """Rewrite a text enumeration through an explicit one-to-one map."""

        return FieldRule(
            target=target,
            source=source,
            enum_map=tuple(sorted(mapping.items())),
        )

    @staticmethod
    def convert(target: str, source: str, kind: str) -> "FieldRule":
        """Apply a declared strict type conversion (see ``CONVERSIONS``)."""

        return FieldRule(target=target, source=source, conversion=kind)

    def __post_init__(self) -> None:
        if not isinstance(self.target, str) or not self.target:
            raise MigrationPlanError("catalog_format", "field rule target must be a non-empty string")
        if self.source is not None and (not isinstance(self.source, str) or not self.source):
            raise MigrationPlanError("catalog_format", "field rule source must be a non-empty string")
        if self.enum_map is not None:
            for pair in self.enum_map:
                if (
                    not isinstance(pair, tuple)
                    or len(pair) != 2
                    or not all(isinstance(v, str) for v in pair)
                ):
                    raise MigrationPlanError(
                        "enum_invalid", "enum maps must be string->string pairs"
                    )
        if self.conversion is not None and self.conversion not in CONVERSIONS:
            raise MigrationPlanError(
                "unknown_conversion",
                f"unknown conversion {self.conversion!r}; allowed: {list(CONVERSIONS)}",
                {"field": self.target},
            )

    @property
    def has_default(self) -> bool:
        return self.default_value is not _NO_DEFAULT

    @property
    def kind(self) -> str:
        if self.has_default:
            return "default"
        if self.enum_map is not None:
            return "enum"
        if self.conversion is not None:
            return "convert"
        return "copy"

    def to_wire(self) -> dict[str, Any]:
        doc: dict[str, Any] = {"target": self.target}
        if self.source is not None:
            doc["from"] = self.source
        if self.has_default:
            doc["default"] = self.default_value
        if self.enum_map is not None:
            doc["enum"] = {k: v for k, v in self.enum_map}
        if self.conversion is not None:
            doc["convert"] = self.conversion
        return doc

    @staticmethod
    def from_wire(obj: Any, *, context: Mapping[str, Any]) -> "FieldRule":
        if not isinstance(obj, Mapping):
            raise MigrationPlanError("catalog_format", "field rule must be an object", context)
        unknown = set(obj) - {"target", "from", "default", "enum", "convert"}
        if unknown:
            raise MigrationPlanError(
                "catalog_format", f"field rule has unknown keys {sorted(unknown)}", context
            )
        if "target" not in obj:
            raise MigrationPlanError("catalog_format", "field rule lacks 'target'", context)
        enum_map = None
        if "enum" in obj:
            raw = obj["enum"]
            if not isinstance(raw, Mapping) or not raw:
                raise MigrationPlanError(
                    "enum_invalid", "enum must be a non-empty object", context
                )
            pairs = []
            for key, value in raw.items():
                if not isinstance(key, str) or not isinstance(value, str):
                    raise MigrationPlanError(
                        "enum_invalid", "enum keys and values must be strings", context
                    )
                pairs.append((key, value))
            enum_map = tuple(sorted(pairs))
        return FieldRule(
            target=obj["target"],
            source=obj.get("from"),
            default_value=obj["default"] if "default" in obj else _NO_DEFAULT,
            enum_map=enum_map,
            conversion=obj.get("convert"),
        )


# ---------------------------------------------------------------------------
# Catalog model


@dataclass(frozen=True)
class VersionDef:
    """The declared structure of one schema version."""

    name: str
    tables: Mapping[str, Schema]
    manifest: RelationManifest

    def to_wire(self) -> dict[str, Any]:
        return {
            "tables": {
                name: [[f.name, f.type, f.nullable] for f in self.tables[name].fields]
                for name in sorted(self.tables)
            },
            "manifest": self.manifest.to_wire(),
        }


@dataclass(frozen=True)
class StepDef:
    """One directed version step: field rules per table.

    The step's target schema and its effective relationship manifest are
    those declared by its target version in the catalog; the rules here
    describe how each target field is derived from the source version.
    """

    from_version: str
    to_version: str
    tables: Mapping[str, tuple[FieldRule, ...]]

    @property
    def label(self) -> str:
        return f"{self.from_version}->{self.to_version}"

    def to_wire(self) -> dict[str, Any]:
        return {
            "from": self.from_version,
            "to": self.to_version,
            "tables": {
                name: {"fields": [rule.to_wire() for rule in self.tables[name]]}
                for name in sorted(self.tables)
            },
        }


@dataclass(frozen=True)
class MigrationCatalog:
    """A versioned migration catalog: declared versions plus directed steps."""

    dataset_id: str
    versions: Mapping[str, VersionDef]
    steps: tuple[StepDef, ...]

    # ------------------------------------------------------------- builders

    @staticmethod
    def declare(
        dataset_id: str,
        versions: Mapping[str, VersionDef],
        steps: Iterable[StepDef],
    ) -> "MigrationCatalog":
        catalog = MigrationCatalog(dataset_id, dict(versions), tuple(steps))
        catalog._validate()
        return catalog

    @staticmethod
    def from_wire(obj: Any) -> "MigrationCatalog":
        ctx: dict[str, Any] = {}
        if not isinstance(obj, Mapping):
            raise MigrationPlanError("catalog_format", "catalog must be a JSON object")
        unknown = set(obj) - {"format", "dataset_id", "versions", "steps"}
        if unknown:
            raise MigrationPlanError(
                "catalog_format", f"catalog has unknown keys {sorted(unknown)}"
            )
        if obj.get("format") != CATALOG_FORMAT:
            raise MigrationPlanError(
                "catalog_format", f"unrecognised catalog format {obj.get('format')!r}"
            )
        dataset_id = obj.get("dataset_id")
        if not isinstance(dataset_id, str) or not dataset_id:
            raise MigrationPlanError("catalog_format", "dataset_id must be a non-empty string")
        raw_versions = obj.get("versions")
        if not isinstance(raw_versions, Mapping) or not raw_versions:
            raise MigrationPlanError("catalog_format", "versions must be a non-empty object")
        versions: dict[str, VersionDef] = {}
        for name, raw in raw_versions.items():
            vctx = {"version": name}
            if not isinstance(name, str) or not name:
                raise MigrationPlanError("catalog_format", "version names must be non-empty strings")
            if not isinstance(raw, Mapping) or set(raw) - {"tables", "manifest"}:
                raise MigrationPlanError(
                    "catalog_format", f"version {name!r} must declare 'tables' and 'manifest'",
                    vctx,
                )
            raw_tables = raw.get("tables")
            if not isinstance(raw_tables, Mapping) or not raw_tables:
                raise MigrationPlanError(
                    "catalog_format", f"version {name!r} needs a non-empty 'tables' object", vctx
                )
            tables: dict[str, Schema] = {}
            for table, fields in raw_tables.items():
                tctx = {**vctx, "table": table}
                if not isinstance(fields, list) or not fields:
                    raise MigrationPlanError(
                        "catalog_format",
                        f"table {table!r} must declare a non-empty field list",
                        tctx,
                    )
                try:
                    tables[table] = Schema.declare(*fields)
                except Exception as exc:
                    raise MigrationPlanError(
                        "catalog_format", f"invalid schema for table {table!r}: {exc}", tctx
                    ) from exc
            try:
                manifest = RelationManifest.from_wire(raw.get("manifest"))
            except GroupError as exc:
                raise MigrationPlanError(
                    "catalog_format", f"invalid manifest for version {name!r}: {exc}", vctx
                ) from exc
            versions[name] = VersionDef(name, tables, manifest)
        raw_steps = obj.get("steps")
        if not isinstance(raw_steps, list):
            raise MigrationPlanError("catalog_format", "steps must be a list")
        steps: list[StepDef] = []
        for index, raw in enumerate(raw_steps):
            sctx = {"step_index": index}
            if not isinstance(raw, Mapping) or set(raw) - {"from", "to", "tables"}:
                raise MigrationPlanError(
                    "catalog_format", "a step must be an object with 'from', 'to', 'tables'",
                    sctx,
                )
            from_v, to_v = raw.get("from"), raw.get("to")
            if not isinstance(from_v, str) or not isinstance(to_v, str) or not from_v or not to_v:
                raise MigrationPlanError(
                    "catalog_format", "step 'from'/'to' must be non-empty strings", sctx
                )
            sctx["step"] = f"{from_v}->{to_v}"
            raw_tables = raw.get("tables")
            if not isinstance(raw_tables, Mapping) or not raw_tables:
                raise MigrationPlanError(
                    "catalog_format", f"step {sctx['step']} needs a non-empty 'tables' object",
                    sctx,
                )
            tables = {}
            for table, spec in raw_tables.items():
                tctx = {**sctx, "table": table}
                if not isinstance(spec, Mapping) or set(spec) != {"fields"}:
                    raise MigrationPlanError(
                        "catalog_format",
                        f"step table {table!r} must be an object with exactly 'fields'",
                        tctx,
                    )
                raw_fields = spec["fields"]
                if not isinstance(raw_fields, list):
                    raise MigrationPlanError(
                        "catalog_format", f"fields of table {table!r} must be a list", tctx
                    )
                tables[table] = tuple(
                    FieldRule.from_wire(item, context=tctx) for item in raw_fields
                )
            steps.append(StepDef(from_v, to_v, tables))
        return MigrationCatalog.declare(dataset_id, versions, steps)

    @staticmethod
    def load(path: str | Path) -> "MigrationCatalog":
        """Load a catalog from a JSON file, rejecting duplicate object keys."""

        def no_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
            out: dict[str, Any] = {}
            for key, value in pairs:
                if key in out:
                    raise MigrationPlanError(
                        "catalog_format", f"duplicate JSON key {key!r} in catalog file"
                    )
                out[key] = value
            return out

        try:
            text = Path(path).read_text(encoding="utf-8")
            obj = json.loads(text, object_pairs_hook=no_duplicates)
        except MigrationPlanError:
            raise
        except (json.JSONDecodeError, UnicodeDecodeError, OSError) as exc:
            raise MigrationPlanError("catalog_format", f"cannot read catalog: {exc}") from exc
        return MigrationCatalog.from_wire(obj)

    # ------------------------------------------------------------ identity

    def to_wire(self) -> dict[str, Any]:
        return {
            "format": CATALOG_FORMAT,
            "dataset_id": self.dataset_id,
            "versions": {name: self.versions[name].to_wire() for name in sorted(self.versions)},
            "steps": [step.to_wire() for step in self.steps],
        }

    def canonical_bytes(self) -> bytes:
        return json.dumps(self.to_wire(), sort_keys=True, separators=(",", ":")).encode("utf-8")

    def catalog_id(self) -> str:
        """Content identity of the whole catalog (structure, rules, manifests)."""

        return encoding.sha256(
            encoding._lp(DOMAIN_CATALOG) + encoding._lp(self.canonical_bytes())
        ).hex()

    # ----------------------------------------------------------- validation

    def _validate(self) -> None:
        if not isinstance(self.dataset_id, str) or not self.dataset_id:
            raise MigrationPlanError("catalog_format", "dataset_id must be a non-empty string")
        if not self.versions:
            raise MigrationPlanError("catalog_format", "a catalog needs at least one version")

        # Every version of one dataset declares the same table set; table
        # membership changes are not a field-level migration.
        table_sets = {name: set(v.tables) for name, v in self.versions.items()}
        reference_name = sorted(self.versions)[0]
        reference = table_sets[reference_name]
        for name in sorted(self.versions):
            if table_sets[name] != reference:
                raise MigrationPlanError(
                    "table_set_mismatch",
                    f"version {name!r} declares tables {sorted(table_sets[name])} but "
                    f"version {reference_name!r} declares {sorted(reference)}",
                    {"version": name, "expected_tables": sorted(reference)},
                )

        # Every version's relationship manifest must be mappable onto its own
        # declared schemas: a foreign key that references a missing table or
        # column, a non-text column, or a mismatched nullability is a plan
        # error, never a runtime surprise.
        for name in sorted(self.versions):
            version = self.versions[name]
            try:
                validate_manifest_against_schema_map(version.manifest, version.tables)
            except GroupError as exc:
                raise MigrationPlanError(
                    "manifest_unmappable",
                    f"manifest of version {name!r} does not map onto its schemas: {exc}",
                    {"version": name},
                ) from exc

        # Steps reference declared versions and cover exactly the table set.
        seen_edges: set[tuple[str, str]] = set()
        for step in self.steps:
            ctx = {"step": step.label}
            for endpoint in (step.from_version, step.to_version):
                if endpoint not in self.versions:
                    raise MigrationPlanError(
                        "unknown_step_version",
                        f"step {step.label} references undeclared version {endpoint!r}",
                        ctx,
                    )
            if step.from_version == step.to_version:
                raise MigrationPlanError(
                    "catalog_cycle", f"step {step.label} loops onto its own version", ctx
                )
            edge = (step.from_version, step.to_version)
            if edge in seen_edges:
                raise MigrationPlanError(
                    "duplicate_step", f"two steps declared for {step.label}", ctx
                )
            seen_edges.add(edge)
            step_tables = set(step.tables)
            if step_tables != reference:
                missing = sorted(reference - step_tables)
                extra = sorted(step_tables - reference)
                raise MigrationPlanError(
                    "table_coverage",
                    f"step {step.label} must declare rules for exactly the dataset "
                    f"tables; missing={missing} extra={extra}",
                    {**ctx, "missing_tables": missing, "extra_tables": extra},
                )
            for table in sorted(step.tables):
                self._validate_table_rules(step, table)

        # The step graph must be acyclic so that "the migration path" is a
        # well-defined finite object.
        self._check_acyclic()

    def _validate_table_rules(self, step: StepDef, table: str) -> None:
        ctx = {"step": step.label, "table": table}
        source_schema = self.versions[step.from_version].tables[table]
        target_schema = self.versions[step.to_version].tables[table]
        rules = step.tables[table]

        seen_targets: set[str] = set()
        for rule in rules:
            if rule.target in seen_targets:
                raise MigrationPlanError(
                    "field_mapping_duplicate",
                    f"target field {rule.target!r} is mapped twice",
                    {**ctx, "field": rule.target},
                )
            seen_targets.add(rule.target)
        target_fields = set(target_schema.field_names())
        mapped = {rule.target for rule in rules}
        missing = sorted(target_fields - mapped)
        if missing:
            raise MigrationPlanError(
                "field_mapping_missing",
                f"target fields without a mapping: {missing}",
                {**ctx, "missing_fields": missing},
            )
        unknown = sorted(mapped - target_fields)
        if unknown:
            raise MigrationPlanError(
                "field_mapping_unknown",
                f"mappings reference fields not in the target schema: {unknown}",
                {**ctx, "unknown_fields": unknown},
            )

        source_fields = set(source_schema.field_names())
        for rule in rules:
            rctx = {**ctx, "field": rule.target}
            if rule.has_default and rule.source is not None:
                raise MigrationPlanError(
                    "source_and_default",
                    f"field {rule.target!r} declares both a source and a default",
                    rctx,
                )
            if not rule.has_default and rule.source is None:
                raise MigrationPlanError(
                    "missing_source",
                    f"field {rule.target!r} declares neither a source nor a default",
                    rctx,
                )
            target_type = target_schema.type_of(rule.target)
            target_nullable = target_schema.is_nullable(rule.target)
            if rule.has_default:
                if rule.target in source_fields:
                    raise MigrationPlanError(
                        "default_on_existing_field",
                        f"field {rule.target!r} exists in the source schema; a constant "
                        "default may only add a new field (use a copy rule to keep data)",
                        rctx,
                    )
                self._validate_default(rule, target_type, target_nullable, rctx)
                continue
            assert rule.source is not None
            if rule.source not in source_fields:
                raise MigrationPlanError(
                    "source_field_unknown",
                    f"source field {rule.source!r} is not in the {step.from_version} schema",
                    {**rctx, "source": rule.source},
                )
            source_type = source_schema.type_of(rule.source)
            if rule.enum_map is not None and rule.conversion is not None:
                raise MigrationPlanError(
                    "conflicting_modifiers",
                    f"field {rule.target!r} declares both an enum map and a conversion",
                    rctx,
                )
            if rule.enum_map is not None:
                if source_type != "text" or target_type != "text":
                    raise MigrationPlanError(
                        "enum_on_non_text",
                        f"enum rewrite on {rule.source!r}->{rule.target!r} requires text "
                        f"on both sides, got {source_type}->{target_type}",
                        rctx,
                    )
                values = [mapped_to for _, mapped_to in rule.enum_map]
                if len(set(values)) != len(values):
                    raise MigrationPlanError(
                        "enum_not_one_to_one",
                        f"enum map for field {rule.target!r} maps two source values to "
                        "the same target value",
                        rctx,
                    )
            elif rule.conversion is not None:
                expected = {
                    CONVERT_TEXT_TO_INT64: ("text", "int64"),
                    CONVERT_INT64_TO_TEXT: ("int64", "text"),
                }[rule.conversion]
                if (source_type, target_type) != expected:
                    raise MigrationPlanError(
                        "conversion_type_mismatch",
                        f"conversion {rule.conversion!r} requires {expected[0]}->{expected[1]}, "
                        f"got {source_type}->{target_type}",
                        rctx,
                    )
            elif source_type != target_type:
                raise MigrationPlanError(
                    "type_change_without_conversion",
                    f"field {rule.source!r}->{rule.target!r} changes type "
                    f"{source_type}->{target_type} without a declared conversion",
                    rctx,
                )
            # Nullability combinations are all plannable: a tightening
            # (nullable source -> non-nullable target) is validated per row at
            # conversion time and fails there on actual NULLs.

    @staticmethod
    def _validate_default(rule: FieldRule, target_type: str, nullable: bool, ctx) -> None:
        value = rule.default_value
        if value is None:
            if not nullable:
                raise MigrationPlanError(
                    "invalid_default",
                    f"default for non-nullable field {rule.target!r} cannot be null",
                    ctx,
                )
            return
        if target_type == "bool":
            if not isinstance(value, bool):
                raise MigrationPlanError(
                    "invalid_default",
                    f"default for bool field {rule.target!r} must be a bool",
                    ctx,
                )
        elif target_type == "int64":
            if isinstance(value, bool) or not isinstance(value, int):
                raise MigrationPlanError(
                    "invalid_default",
                    f"default for int64 field {rule.target!r} must be an integer",
                    ctx,
                )
            if not (INT64_MIN <= value <= INT64_MAX):
                raise MigrationPlanError(
                    "invalid_default",
                    f"default for int64 field {rule.target!r} is out of int64 range",
                    ctx,
                )
        elif target_type == "text":
            if not isinstance(value, str):
                raise MigrationPlanError(
                    "invalid_default",
                    f"default for text field {rule.target!r} must be a string",
                    ctx,
                )

    def _check_acyclic(self) -> None:
        edges: dict[str, list[str]] = {}
        for step in self.steps:
            edges.setdefault(step.from_version, []).append(step.to_version)
        WHITE, GRAY, BLACK = 0, 1, 2
        color = {name: WHITE for name in self.versions}
        stack: list[str] = []

        def visit(node: str) -> None:
            color[node] = GRAY
            stack.append(node)
            for nxt in edges.get(node, []):
                if color.get(nxt) == GRAY:
                    cycle = stack[stack.index(nxt):] + [nxt]
                    raise MigrationPlanError(
                        "catalog_cycle",
                        f"migration steps form a cycle: {' -> '.join(cycle)}",
                        {"cycle": cycle},
                    )
                if color.get(nxt) == WHITE:
                    visit(nxt)
            stack.pop()
            color[node] = BLACK

        for name in sorted(self.versions):
            if color[name] == WHITE:
                visit(name)

    # -------------------------------------------------------------- queries

    def steps_from(self, version: str) -> list[StepDef]:
        return [step for step in self.steps if step.from_version == version]

    def find_unique_path(self, source: str, target: str) -> list[StepDef]:
        """The one acyclic step path from ``source`` to ``target``.

        Raises :class:`MigrationPlanError` with code ``no_path`` when no route
        exists and ``ambiguous_path`` when more than one does; the planner
        never chooses a route on its own.
        """

        if source not in self.versions:
            raise MigrationPlanError(
                "unknown_source_version",
                f"source version {source!r} is not declared in the catalog",
                {"version": source},
            )
        if target not in self.versions:
            raise MigrationPlanError(
                "unknown_target_version",
                f"target version {target!r} is not declared in the catalog",
                {"version": target},
            )
        if source == target:
            return []
        paths: list[list[StepDef]] = []

        def dfs(node: str, acc: list[StepDef], seen: set[str]) -> None:
            if node == target:
                paths.append(list(acc))
                return
            for step in self.steps_from(node):
                if step.to_version in seen:
                    continue
                seen.add(step.to_version)
                acc.append(step)
                dfs(step.to_version, acc, seen)
                acc.pop()
                seen.discard(step.to_version)

        dfs(source, [], {source})
        if not paths:
            raise MigrationPlanError(
                "no_path",
                f"no migration path from {source!r} to {target!r}",
                {"source_version": source, "target_version": target},
            )
        if len(paths) > 1:
            routes = [[step.label for step in path] for path in paths]
            raise MigrationPlanError(
                "ambiguous_path",
                f"multiple migration paths from {source!r} to {target!r}: {routes}",
                {"source_version": source, "target_version": target, "routes": routes},
            )
        return paths[0]


def compute_transform_id(
    catalog_id: str, source_version: str, target_version: str, path: Iterable[StepDef]
) -> str:
    """Deterministic identity of one end's transformation.

    Binds the catalog identity and the source/target versions (hence the
    resolved path); identical inputs always yield the same identity, however
    often the migration is re-run.
    """

    steps = list(path)
    material = bytearray()
    material += encoding._lp(DOMAIN_TRANSFORM)
    material += encoding._lp(bytes.fromhex(catalog_id))
    material += encoding._lp(source_version.encode("utf-8"))
    material += encoding._lp(target_version.encode("utf-8"))
    material += struct.pack(">I", len(steps))
    for step in steps:
        material += encoding._lp(step.from_version.encode("utf-8"))
        material += encoding._lp(step.to_version.encode("utf-8"))
    return encoding.sha256(bytes(material)).hex()


# ---------------------------------------------------------------------------
# Planning


@dataclass(frozen=True)
class EndPlan:
    """One end's resolved migration plan (no data has been read yet)."""

    source_version: str
    target_version: str
    steps: tuple[StepDef, ...]
    target_tables: Mapping[str, Schema]
    target_manifest: RelationManifest
    transform_id: str
    source_group_id: str
    source_batch: str

    @property
    def path_versions(self) -> list[str]:
        versions = [self.source_version]
        versions.extend(step.to_version for step in self.steps)
        return versions

    def to_dict(self) -> dict[str, Any]:
        return {
            "source_version": self.source_version,
            "target_version": self.target_version,
            "path": self.path_versions,
            "steps": [step.label for step in self.steps],
            "transform_id": self.transform_id,
            "source_group_id": self.source_group_id,
            "source_batch": self.source_batch,
        }


def plan_end_migration(
    catalog: MigrationCatalog, group: GroupStore, target_version: str
) -> EndPlan:
    """Validate one group against the catalog and resolve its migration path.

    Everything here is structural: the group's declared schema version must
    be known to the catalog, its actual per-table schemas and relationship
    manifest must match the catalog's declaration for that version, and the
    path to the target version must exist and be unique.  No row is read for
    conversion.
    """

    if group.dataset_id != catalog.dataset_id:
        raise MigrationPlanError(
            "dataset_mismatch",
            f"group dataset {group.dataset_id!r} != catalog dataset {catalog.dataset_id!r}",
            {"group_dataset": group.dataset_id, "catalog_dataset": catalog.dataset_id},
        )
    source = group.schema_version
    if source is None:
        raise MigrationPlanError(
            "schema_version_missing",
            f"group at {group.path} declares no schema_version",
            {"group": str(group.path)},
        )
    if source not in catalog.versions:
        raise MigrationPlanError(
            "unknown_source_version",
            f"group declares schema version {source!r}, which the catalog does not know",
            {"version": source, "known_versions": sorted(catalog.versions)},
        )
    declared = catalog.versions[source]
    actual_tables = set(group.tables)
    declared_tables = set(declared.tables)
    if actual_tables != declared_tables:
        raise MigrationPlanError(
            "version_schema_mismatch",
            f"group tables {sorted(actual_tables)} do not match version {source!r} "
            f"tables {sorted(declared_tables)}",
            {
                "version": source,
                "only_in_group": sorted(actual_tables - declared_tables),
                "only_in_catalog": sorted(declared_tables - actual_tables),
            },
        )
    for name in sorted(actual_tables):
        actual_sig = group.tables[name].schema.signature()
        declared_sig = declared.tables[name].signature()
        if actual_sig != declared_sig:
            raise MigrationPlanError(
                "version_schema_mismatch",
                f"table {name!r} schema does not match the catalog's declaration "
                f"for version {source!r}",
                {
                    "version": source,
                    "table": name,
                    "declared": [list(item) for item in declared_sig],
                    "actual": [list(item) for item in actual_sig],
                },
            )
    if group.manifest.content_hash() != declared.manifest.content_hash():
        raise MigrationPlanError(
            "version_manifest_mismatch",
            f"group manifest does not match the catalog's manifest for version {source!r}",
            {"version": source},
        )
    steps = tuple(catalog.find_unique_path(source, target_version))
    target = catalog.versions[target_version]
    return EndPlan(
        source_version=source,
        target_version=target_version,
        steps=steps,
        target_tables=target.tables,
        target_manifest=target.manifest,
        transform_id=compute_transform_id(catalog.catalog_id(), source, target_version, steps),
        source_group_id=group.group_id,
        source_batch=group.batch_id,
    )


def plan_migrations(
    catalog: MigrationCatalog,
    left_dir: str | Path,
    right_dir: str | Path,
    target_version: str,
) -> tuple[EndPlan, EndPlan]:
    """Plan both ends' migrations before any data is converted."""

    with GroupStore.open(left_dir) as left:
        left_plan = plan_end_migration(catalog, left, target_version)
    with GroupStore.open(right_dir) as right:
        right_plan = plan_end_migration(catalog, right, target_version)
    return left_plan, right_plan


# ---------------------------------------------------------------------------
# Execution statistics


@dataclass
class FieldStat:
    """Per-field conversion counters within one step and table."""

    rule: str
    source: str | None
    renamed: bool
    counts: dict[str, int] = field(default_factory=dict)

    def bump(self, key: str) -> None:
        self.counts[key] = self.counts.get(key, 0) + 1

    def to_dict(self) -> dict[str, Any]:
        return {
            "rule": self.rule,
            "source": self.source,
            "renamed": self.renamed,
            "counts": dict(sorted(self.counts.items())),
        }


@dataclass
class TableStepStat:
    table: str
    input_rows: int = 0
    output_rows: int = 0
    fields: dict[str, FieldStat] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "input_rows": self.input_rows,
            "output_rows": self.output_rows,
            "fields": {name: self.fields[name].to_dict() for name in sorted(self.fields)},
        }


@dataclass
class StepStat:
    from_version: str
    to_version: str
    tables: dict[str, TableStepStat] = field(default_factory=dict)

    @property
    def label(self) -> str:
        return f"{self.from_version}->{self.to_version}"

    def to_dict(self) -> dict[str, Any]:
        return {
            "from": self.from_version,
            "to": self.to_version,
            "tables": {name: self.tables[name].to_dict() for name in sorted(self.tables)},
        }


@dataclass
class EndMigration:
    """The outcome of migrating one end's group to the target version."""

    plan: EndPlan
    migrated_dir: Path
    migrated_group_id: str
    migrated_batch: str
    steps: list[StepStat]

    def to_dict(self) -> dict[str, Any]:
        return {
            **self.plan.to_dict(),
            "migrated_group_id": self.migrated_group_id,
            "migrated_batch": self.migrated_batch,
            "step_stats": [stat.to_dict() for stat in self.steps],
        }


# ---------------------------------------------------------------------------
# Row conversion


class _RowFailure(Exception):
    def __init__(self, category: str, detail: str) -> None:
        super().__init__(detail)
        self.category = category
        self.detail = detail


def _convert_decimal_text(value: str) -> int:
    """Strict decimal-text -> int64: no whitespace, signs other than a leading
    '-', fractions, exponents, or out-of-range values."""

    if not _DECIMAL_TEXT_RE.match(value):
        raise _RowFailure(
            "bad_decimal_format",
            f"{value!r} is not a canonical decimal integer text",
        )
    number = int(value)
    if not (INT64_MIN <= number <= INT64_MAX):
        raise _RowFailure(
            "int64_out_of_range",
            f"{value!r} is outside the int64 range [{INT64_MIN}, {INT64_MAX}]",
        )
    return number


def _apply_rule(
    rule: FieldRule,
    values: Mapping[str, Any],
    target_schema: Schema,
    stat: FieldStat,
    enum_lookup: Mapping[str, str] | None,
) -> Any:
    """Derive one target field's value for one row, strictly."""

    if rule.has_default:
        stat.bump("defaulted")
        return rule.default_value
    assert rule.source is not None
    value = values[rule.source]
    if value is None:
        if not target_schema.is_nullable(rule.target):
            raise _RowFailure(
                "null_violation",
                f"source field {rule.source!r} is null but target field "
                f"{rule.target!r} is not nullable",
            )
        stat.bump("nulls")
        return None
    if enum_lookup is not None:
        if value not in enum_lookup:
            raise _RowFailure(
                "enum_unmapped",
                f"value {value!r} is not listed in the enum map",
            )
        stat.bump("mapped")
        return enum_lookup[value]
    if rule.conversion == CONVERT_TEXT_TO_INT64:
        stat.bump("converted")
        return _convert_decimal_text(value)
    if rule.conversion == CONVERT_INT64_TO_TEXT:
        stat.bump("converted")
        return str(value)
    stat.bump("copied")
    return value


def _migrate_table(
    store,
    rules: tuple[FieldRule, ...],
    source_schema: Schema,
    target_schema: Schema,
    step_label: str,
    table: str,
    stat: TableStepStat,
    failures: list[dict[str, Any]],
) -> list[tuple[str, dict[str, Any]]]:
    """Convert every row of one table; failures are collected, not hidden."""

    out_rows: list[tuple[str, dict[str, Any]]] = []
    stat.input_rows = store.identity.row_count
    enum_lookups = {
        rule.target: dict(rule.enum_map) for rule in rules if rule.enum_map is not None
    }
    for record in store.iter_records():
        # The store guarantees canonical records; decoding against the step's
        # declared input schema is the per-step input-type validation.
        row_id, values = decode_record(source_schema, record)
        new_values: dict[str, Any] = {}
        row_failed = False
        for rule in rules:
            field_stat = stat.fields[rule.target]
            try:
                new_values[rule.target] = _apply_rule(
                    rule, values, target_schema, field_stat, enum_lookups.get(rule.target)
                )
            except _RowFailure as exc:
                row_failed = True
                if len(failures) < FAILURE_CAP:
                    failures.append(
                        {
                            "step": step_label,
                            "table": table,
                            "row_id": row_id,
                            "field": rule.target,
                            "category": exc.category,
                            "detail": exc.detail,
                        }
                    )
        if row_failed:
            continue
        # Re-encode against the declared output schema: the per-step
        # output-type validation (types, non-null constraints).
        encode_record(target_schema, row_id, new_values)
        out_rows.append((row_id, new_values))
    stat.output_rows = len(out_rows)
    return out_rows


def _migrated_batch(source_batch: str, to_version: str, catalog_id: str) -> str:
    return f"{source_batch}=>{to_version}#{catalog_id[:12]}"


def execute_migration(
    group_dir: str | Path,
    plan: EndPlan,
    work_dir: str | Path,
    *,
    catalog: MigrationCatalog,
) -> EndMigration:
    """Run one end's plan, writing isolated temporary dataset groups.

    The source group is only ever opened read-only.  Each step's output is a
    brand-new dataset group under ``work_dir`` whose batch label derives
    deterministically from the source batch, the step's target version, and
    the catalog identity.  An empty plan (the end is already at the target
    version) uses the source group as-is.
    """

    work = Path(work_dir)
    stats: list[StepStat] = []
    if not plan.steps:
        with GroupStore.open(group_dir) as group:
            return EndMigration(
                plan=plan,
                migrated_dir=Path(group_dir),
                migrated_group_id=group.group_id,
                migrated_batch=group.batch_id,
                steps=stats,
            )

    current_dir = Path(group_dir)
    source_batch = plan.source_batch
    catalog_id = catalog.catalog_id()
    for step in plan.steps:
        step_stat = StepStat(step.from_version, step.to_version)
        out_dir = work / f"step-{step.from_version}-to-{step.to_version}"
        out_dir.mkdir(parents=True, exist_ok=False)
        source_version = catalog.versions[step.from_version]
        target_version = catalog.versions[step.to_version]
        failures: list[dict[str, Any]] = []
        truncated = False
        out_tables: dict[str, tuple[Schema, list[tuple[str, dict[str, Any]]]]] = {}
        with GroupStore.open(current_dir) as group:
            for table in sorted(step.tables):
                store = group.tables[table]
                source_schema = source_version.tables[table]
                target_schema = target_version.tables[table]
                if store.schema.signature() != source_schema.signature():
                    raise MigrationPlanError(
                        "version_schema_mismatch",
                        f"step {step.label} expects table {table!r} with schema "
                        f"of version {step.from_version!r} but the input differs",
                        {"step": step.label, "table": table},
                    )
                table_stat = TableStepStat(table)
                for rule in step.tables[table]:
                    table_stat.fields[rule.target] = FieldStat(
                        rule=rule.kind,
                        source=rule.source,
                        renamed=rule.source is not None and rule.source != rule.target,
                    )
                before = len(failures)
                rows = _migrate_table(
                    store,
                    step.tables[table],
                    source_schema,
                    target_schema,
                    step.label,
                    table,
                    table_stat,
                    failures,
                )
                if len(failures) >= FAILURE_CAP:
                    truncated = True
                if len(failures) == before:
                    out_tables[table] = (target_schema, rows)
                step_stat.tables[table] = table_stat
        stats.append(step_stat)
        if failures or truncated:
            raise MigrationConversionError(step.label, failures, truncated=truncated)
        batch = _migrated_batch(source_batch, step.to_version, catalog_id)
        create_group(
            out_dir,
            catalog.dataset_id,
            batch,
            out_tables,
            target_version.manifest,
            schema_version=step.to_version,
        )
        current_dir = out_dir

    with GroupStore.open(current_dir) as migrated:
        return EndMigration(
            plan=plan,
            migrated_dir=current_dir,
            migrated_group_id=migrated.group_id,
            migrated_batch=migrated.batch_id,
            steps=stats,
        )


def migrate_group(
    group_dir: str | Path,
    catalog: MigrationCatalog,
    target_version: str,
    work_dir: str | Path,
) -> EndMigration:
    """Plan and execute the migration of a single group (convenience wrapper)."""

    with GroupStore.open(group_dir) as group:
        plan = plan_end_migration(catalog, group, target_version)
    return execute_migration(group_dir, plan, work_dir, catalog=catalog)


# ---------------------------------------------------------------------------
# Orchestration: migrate both ends, reconcile, publish


@dataclass
class MigrationRunResult:
    """The full report of a migrate-then-reconcile run.

    Binds both ends' original group identities, the target version, the
    catalog identity, and the actual version paths; lists per-step, per-table
    input/output row counts and field conversion counters; and, on
    completion, links the underlying group reconciliation result and the
    published mirror generation.
    """

    complete: bool
    stage: str
    reason: str
    dataset_id: str
    target_version: str
    catalog_id: str
    left: EndMigration | None
    right: EndMigration | None
    error: dict[str, Any] | None
    reconcile: GroupRunResult | None
    generation_before: int | None
    generation_after: int | None
    temp_dir: str | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "complete": self.complete,
            "stage": self.stage,
            "reason": self.reason,
            "dataset_id": self.dataset_id,
            "target_version": self.target_version,
            "catalog_id": self.catalog_id,
            "left": self.left.to_dict() if self.left else None,
            "right": self.right.to_dict() if self.right else None,
            "error": self.error,
            "reconcile": self.reconcile.to_dict() if self.reconcile else None,
            "generation_before": self.generation_before,
            "generation_after": self.generation_after,
            "temp_dir": self.temp_dir,
        }


def reconcile_migrated_and_publish(
    left_dir: str | Path,
    right_dir: str | Path,
    *,
    catalog: MigrationCatalog | str | Path,
    target_version: str,
    mirror_dir: str | Path | None = None,
    initialise_mirror: bool = False,
    work_dir: str | Path | None = None,
    keep_temp: bool = False,
    params: SummaryParams | None = None,
    session_prefix: str | None = None,
) -> MigrationRunResult:
    """Migrate both ends to ``target_version``, reconcile, and publish.

    Planning for *both* ends completes before any row is converted; plan
    errors and conversion failures are reported in the result (stage
    ``plan``/``migrate``) without touching the mirror.  After migration both
    ends must present the same target table set, schemas, and relationship
    manifest (verified explicitly) before the existing group reconciliation
    runs.  The mirror publishes only when that reconciliation completes and
    its own commitment and final-state foreign-key checks pass.  Temporary
    migrated groups are removed unless ``keep_temp`` is set, so a fixed
    catalog can be retried cleanly.
    """

    if not isinstance(catalog, MigrationCatalog):
        catalog = MigrationCatalog.load(catalog)
    catalog_id = catalog.catalog_id()

    owns_work = work_dir is None
    work = Path(work_dir) if work_dir is not None else Path(
        tempfile.mkdtemp(prefix="recon-migrate-")
    )
    created: list[Path] = []

    def finish(
        *,
        complete: bool,
        stage: str,
        reason: str,
        left: EndMigration | None = None,
        right: EndMigration | None = None,
        error: dict[str, Any] | None = None,
        reconcile: GroupRunResult | None = None,
        generation_before: int | None = None,
        generation_after: int | None = None,
    ) -> MigrationRunResult:
        return MigrationRunResult(
            complete=complete,
            stage=stage,
            reason=reason,
            dataset_id=catalog.dataset_id,
            target_version=target_version,
            catalog_id=catalog_id,
            left=left,
            right=right,
            error=error,
            reconcile=reconcile,
            generation_before=generation_before,
            generation_after=generation_after,
            temp_dir=str(work) if keep_temp else None,
        )

    try:
        # 1. Plan both ends before any data is converted.
        try:
            left_plan, right_plan = plan_migrations(
                catalog, left_dir, right_dir, target_version
            )
        except MigrationPlanError as exc:
            return finish(
                complete=False,
                stage="plan",
                reason=str(exc),
                error=exc.to_dict(),
            )

        # 2. Migrate each end into its isolated temporary dataset.
        migrated: dict[str, EndMigration] = {}
        for side, plan, directory in (
            ("left", left_plan, left_dir),
            ("right", right_plan, right_dir),
        ):
            side_work = work / side
            created.append(side_work)
            try:
                migrated[side] = execute_migration(
                    directory, plan, side_work, catalog=catalog
                )
            except (MigrationConversionError, MigrationPlanError) as exc:
                return finish(
                    complete=False,
                    stage="migrate",
                    reason=str(exc),
                    left=migrated.get("left"),
                    right=migrated.get("right"),
                    error={**exc.to_dict(), "side": side},
                )
        left_mig, right_mig = migrated["left"], migrated["right"]

        # 3. Both ends must now present the same target structure before the
        #    existing reconciliation may run.
        try:
            with GroupStore.open(left_mig.migrated_dir) as lm, GroupStore.open(
                right_mig.migrated_dir
            ) as rm:
                check_compatible(lm, rm)
                if lm.schema_version != target_version or rm.schema_version != target_version:
                    raise MigrationPlanError(
                        "version_schema_mismatch",
                        "migrated groups do not both carry the target schema version",
                        {"target_version": target_version},
                    )
        except GroupError as exc:
            return finish(
                complete=False,
                stage="compat",
                reason=str(exc),
                left=left_mig,
                right=right_mig,
                error={"code": exc.code, "message": exc.message, "context": {}},
            )
        except MigrationPlanError as exc:
            return finish(
                complete=False,
                stage="compat",
                reason=str(exc),
                left=left_mig,
                right=right_mig,
                error=exc.to_dict(),
            )

        # 4. Optionally initialise the mirror from the migrated local state.
        mirror = None
        if mirror_dir is not None:
            mirror = Mirror(mirror_dir)
            if mirror.current_generation() is None:
                if not initialise_mirror:
                    return finish(
                        complete=False,
                        stage="publish",
                        reason="mirror_uninitialised",
                        left=left_mig,
                        right=right_mig,
                        error={
                            "code": "mirror_uninitialised",
                            "message": "mirror is not initialised and "
                            "initialise_mirror is False",
                            "context": {},
                        },
                    )
                with GroupStore.open(left_mig.migrated_dir) as source:
                    Mirror.initialize(mirror_dir, source)

        # 5. Reuse the existing group reconciliation and atomic publish.
        scratch = work / "scratch"
        created.append(scratch)
        prefix = session_prefix or f"mig-{uuid.uuid4().hex[:12]}"
        if mirror is not None:
            group_result = reconcile_and_publish(
                left_mig.migrated_dir,
                right_mig.migrated_dir,
                mirror.path,
                params=params,
                scratch_dir=scratch,
                session_prefix=prefix,
            )
        else:
            group_result = reconcile_group(
                left_mig.migrated_dir,
                right_mig.migrated_dir,
                params=params,
                scratch_dir=scratch,
                session_prefix=prefix,
            )
        stage = "done"
        complete = group_result.complete
        reason = group_result.reason
        if not complete:
            publish = group_result.publish
            attempted = publish is not None and not publish.reason.startswith("not_attempted:")
            stage = "publish" if attempted else "reconcile"
        return finish(
            complete=complete,
            stage=stage,
            reason=reason,
            left=left_mig,
            right=right_mig,
            reconcile=group_result,
            generation_before=group_result.generation_before,
            generation_after=group_result.generation_after,
        )
    finally:
        if not keep_temp:
            for path in created:
                shutil.rmtree(path, ignore_errors=True)
            if owns_work:
                shutil.rmtree(work, ignore_errors=True)
