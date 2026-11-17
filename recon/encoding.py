"""Schema declaration, canonical row encoding, and snapshot commitment.

Canonical encoding
------------------
Every field is encoded as ``<prefix><payload>`` with a type prefix byte.  The
prefix makes ``NULL``, the empty text, the integer ``0`` and ``FALSE`` distinct
without relying on SQLite's dynamic typing.  A NULL has no payload.  Integers
are encoded as an 8-byte big-endian two's-complement value.  Booleans are a
single 0x00/0x01 byte and can never be confused with integers.  Text is encoded
as ``<u32 big-endian byte length><UTF-8 bytes>``; text is never normalised and
may contain any Unicode scalar value, including embedded NUL.

Equality of canonical rows therefore means equality of all business fields,
while row identity is the separate ``row_id`` field.  Neighbouring fields are
unambiguous because every variable-length field carries its own length.

The *content commitment* commits to the schema declaration, the negotiated
parameters, the total row count, the sorted row-id interval (first and last
key) and every row's canonical bytes.  The schema is included in a
length-prefixed form so field names cannot collide with row contents.
"""

from __future__ import annotations

import hashlib
import struct
from dataclasses import dataclass
from typing import Iterable, Mapping, Sequence

MAGIC_COMMIT = b"recon-commit-v1"
MAGIC_RECORD = b"recon-record-v1"
MAGIC_SCHEMA = b"recon-schema-v1"

FIELD_TYPES = ("int64", "text", "bool")

INT64_MIN = -(2**63)
INT64_MAX = 2**63 - 1

# Domain separation for every hash use in the protocol.  Each domain string is
# hashed as its own length-prefixed element before any payload.
DOMAIN_COMMIT = b"RECON:COMMIT:v1"
DOMAIN_ITEM = b"RECON:ITEM:v1"
DOMAIN_GROUP = b"RECON:GROUP:v1"
DOMAIN_LEAF = b"RECON:LEAF:v1"
DOMAIN_MSG = b"RECON:MSG:v1"
DOMAIN_FINGERPRINT = b"RECON:FINGERPRINT:v1"
DOMAIN_DETAIL = b"RECON:DETAIL:v1"

ROW_ID_MAX_BYTES = 1 << 20
TEXT_MAX_BYTES = 1 << 30


class SchemaError(ValueError):
    """Schema declaration or a value does not conform to the schema."""


class CanonicalizationError(ValueError):
    """A row cannot be encoded canonically."""


def sha256(data: bytes) -> bytes:
    return hashlib.sha256(data).digest()


def _lp(data: bytes) -> bytes:
    """Length-prefix a byte string with a 4-byte big-endian length."""

    if not isinstance(data, bytes):
        raise TypeError("length-prefixed element must be bytes")
    if len(data) > 0xFFFFFFFF:
        raise ValueError("length-prefixed element exceeds 4 GiB")
    return struct.pack(">I", len(data)) + data


def encode_row_id(row_id: str) -> bytes:
    if not isinstance(row_id, str):
        raise CanonicalizationError("row_id must be a string")
    if row_id == "":
        raise CanonicalizationError("row_id must not be empty")
    raw = row_id.encode("utf-8", errors="strict")
    if len(raw) > ROW_ID_MAX_BYTES:
        raise CanonicalizationError("row_id too long")
    return _lp(raw)


@dataclass(frozen=True)
class FieldSpec:
    name: str
    type: str
    nullable: bool = False


@dataclass(frozen=True)
class Schema:
    """An ordered sequence of named, typed fields.

    Declare with ``(name, type)`` pairs, or with ``(name, type, nullable)``
    triples.  Mixing the two forms is rejected: a schema that declares any
    nullability must state it for every field, so the negotiated schema is
    unambiguous.
    """

    fields: tuple[FieldSpec, ...]

    def __post_init__(self) -> None:
        names = set()
        for spec in self.fields:
            if not isinstance(spec, FieldSpec):
                raise SchemaError("fields must be FieldSpec instances")
            if not isinstance(spec.name, str) or not spec.name:
                raise SchemaError(f"invalid field name {spec.name!r}")
            if spec.type not in FIELD_TYPES:
                raise SchemaError(f"unknown type {spec.type!r} for field {spec.name!r}")
            if not isinstance(spec.nullable, bool):
                raise SchemaError(f"nullability of {spec.name!r} must be bool")
            if spec.name in names:
                raise SchemaError(f"duplicate field name {spec.name!r}")
            names.add(spec.name)

    @staticmethod
    def declare(*fields: Sequence) -> "Schema":
        """Declare fields in schema order.

        Use ``(name, type)`` pairs for an all-non-nullable schema, or
        ``(name, type, nullable)`` triples when nullability matters.  Mixing
        the two forms is rejected, so there is never an *implicit* nullability
        default inside a schema that declares nullable columns.
        """

        if not fields:
            raise SchemaError("schema must declare at least one field")
        if any(len(f) not in (2, 3) for f in fields):
            raise SchemaError("field declarations must be (name, type) or (name, type, nullable)")
        if any(len(f) == 3 for f in fields) and any(len(f) != 3 for f in fields):
            raise SchemaError(
                "when any field declares nullability, every field must declare it"
            )
        specs = []
        for field in fields:
            name, ftype = field[0], field[1]
            nullable = field[2] if len(field) == 3 else False
            if not isinstance(nullable, bool):
                raise SchemaError(f"nullable flag for {name!r} must be bool")
            specs.append(FieldSpec(name, ftype, nullable))
        return Schema(tuple(specs))

    def field_names(self) -> tuple[str, ...]:
        return tuple(f.name for f in self.fields)

    def type_of(self, name: str) -> str:
        for f in self.fields:
            if f.name == name:
                return f.type
        raise SchemaError(f"unknown field {name!r}")

    def is_nullable(self, name: str) -> bool:
        for f in self.fields:
            if f.name == name:
                return f.nullable
        raise SchemaError(f"unknown field {name!r}")

    def signature(self) -> tuple[tuple[str, str, bool], ...]:
        return tuple((f.name, f.type, f.nullable) for f in self.fields)


def _encode_value(spec: FieldSpec, value, *, row_id: str) -> bytes:
    if value is None:
        if not spec.nullable:
            raise CanonicalizationError(
                f"field {spec.name!r} of row_id {row_id!r} is not nullable"
            )
        return b"N"
    if spec.type == "text":
        if not isinstance(value, str):
            raise CanonicalizationError(
                f"field {spec.name!r} of row_id {row_id!r} must be text, "
                f"got {type(value).__name__}"
            )
        raw = value.encode("utf-8", errors="strict")
        if len(raw) > TEXT_MAX_BYTES:
            raise CanonicalizationError(
                f"field {spec.name!r} of row_id {row_id!r} exceeds the text limit"
            )
        return b"T" + struct.pack(">I", len(raw)) + raw
    if spec.type == "bool":
        # bool is a subclass of int in Python, so test bool first and require
        # it exactly; an int is never accepted for a bool column.
        if not isinstance(value, bool):
            raise CanonicalizationError(
                f"field {spec.name!r} of row_id {row_id!r} must be bool, "
                f"got {type(value).__name__}"
            )
        return b"B" + (b"\x01" if value else b"\x00")
    if spec.type == "int64":
        if isinstance(value, bool) or not isinstance(value, int):
            raise CanonicalizationError(
                f"field {spec.name!r} of row_id {row_id!r} must be int, "
                f"got {type(value).__name__}"
            )
        if not (INT64_MIN <= value <= INT64_MAX):
            raise CanonicalizationError(
                f"field {spec.name!r} of row_id {row_id!r} is out of int64 range"
            )
        return b"I" + struct.pack(">q", value)
    raise SchemaError(f"unknown field type {spec.type!r}")


def encode_record(schema: Schema, row_id: str, values: Mapping[str, object]) -> bytes:
    """Encode one row into its canonical record bytes.

    The returned bytes contain the row id and every declared field, in schema
    order.  They are the unit of equality and of hashing in the summary layer.
    """

    if not isinstance(values, Mapping):
        raise CanonicalizationError("row values must be a mapping")
    extra = set(values) - set(schema.field_names())
    if extra:
        raise CanonicalizationError(f"unknown fields for row_id {row_id!r}: {sorted(extra)}")
    out = bytearray(MAGIC_RECORD)
    out += encode_row_id(row_id)
    for spec in schema.fields:
        if spec.name not in values:
            raise CanonicalizationError(
                f"field {spec.name!r} missing for row_id {row_id!r}"
            )
        out += _encode_value(spec, values[spec.name], row_id=row_id)
    return bytes(out)


def encode_row_id_key(row_id: str) -> bytes:
    """The row-id portion of a canonical record, for sorted iteration."""

    return encode_row_id(row_id)


def record_row_id_bytes(record: bytes) -> bytes:
    """Extract the raw UTF-8 row-id bytes from a canonical record."""

    if not isinstance(record, bytes) or not record.startswith(MAGIC_RECORD):
        raise CanonicalizationError("record does not start with the canonical magic")
    pos = len(MAGIC_RECORD)
    if len(record) < pos + 4:
        raise CanonicalizationError("truncated row-id length")
    (length,) = struct.unpack(">I", record[pos : pos + 4])
    start = pos + 4
    end = start + length
    if end > len(record):
        raise CanonicalizationError("truncated row-id payload")
    return record[start:end]


def decode_record(schema: Schema, record: bytes) -> tuple[str, dict[str, object]]:
    """Strictly decode a canonical record, returning ``(row_id, values)``.

    The decoded values are re-encoded and compared with the input, so a record
    with any non-canonical encoding, trailing bytes, wrong type prefix, or
    out-of-range value is rejected.
    """

    row_id_raw = record_row_id_bytes(record)
    try:
        row_id = row_id_raw.decode("utf-8", errors="strict")
    except UnicodeDecodeError as exc:
        raise CanonicalizationError("row_id is not valid UTF-8") from exc
    if row_id == "":
        raise CanonicalizationError("row_id must not be empty")
    pos = len(MAGIC_RECORD) + 4 + len(row_id_raw)
    values: dict[str, object] = {}
    for spec in schema.fields:
        if pos >= len(record):
            raise CanonicalizationError(f"truncated at field {spec.name!r}")
        prefix = record[pos : pos + 1]
        pos += 1
        if prefix == b"N":
            if not spec.nullable:
                raise CanonicalizationError(f"field {spec.name!r} is not nullable")
            values[spec.name] = None
        elif prefix == b"T" and spec.type == "text":
            if len(record) < pos + 4:
                raise CanonicalizationError(f"truncated text length at {spec.name!r}")
            (length,) = struct.unpack(">I", record[pos : pos + 4])
            pos += 4
            if len(record) < pos + length:
                raise CanonicalizationError(f"truncated text at {spec.name!r}")
            raw = record[pos : pos + length]
            pos += length
            try:
                values[spec.name] = raw.decode("utf-8", errors="strict")
            except UnicodeDecodeError as exc:
                raise CanonicalizationError(
                    f"text at {spec.name!r} is not valid UTF-8"
                ) from exc
        elif prefix == b"B" and spec.type == "bool":
            if pos >= len(record):
                raise CanonicalizationError(f"truncated bool at {spec.name!r}")
            byte = record[pos]
            pos += 1
            if byte not in (0, 1):
                raise CanonicalizationError(f"invalid bool at {spec.name!r}")
            values[spec.name] = bool(byte)
        elif prefix == b"I" and spec.type == "int64":
            if len(record) < pos + 8:
                raise CanonicalizationError(f"truncated int64 at {spec.name!r}")
            (values[spec.name],) = struct.unpack(">q", record[pos : pos + 8])
            pos += 8
        else:
            raise CanonicalizationError(
                f"type prefix mismatch at field {spec.name!r} (expected {spec.type})"
            )
    if pos != len(record):
        raise CanonicalizationError("trailing bytes after the canonical record")
    if encode_record(schema, row_id, values) != record:
        raise CanonicalizationError("record is not in canonical form")
    return row_id, values


def schema_wire(schema: Schema) -> bytes:
    out = bytearray(MAGIC_SCHEMA)
    out += struct.pack(">I", len(schema.fields))
    for spec in schema.fields:
        out += _lp(spec.name.encode("utf-8"))
        out += _lp(spec.type.encode("ascii"))
        out += b"\x01" if spec.nullable else b"\x00"
    return bytes(out)


def compute_commitment(
    schema: Schema,
    params: Mapping[str, object],
    records: Iterable[bytes],
) -> bytes:
    """SHA-256 commitment over a fixed snapshot's ordered canonical content.

    Elements, each length-prefixed: domain, schema wire form, negotiated
    parameters (sorted key/value pairs), row count, first record key, last
    record key, then every canonical record in sorted order.  The count and
    boundary keys make truncation, reordering and omission detectable rather
    than merely improbable under the collision assumption.
    """

    material = bytearray()
    material += _lp(DOMAIN_COMMIT)
    material += _lp(schema_wire(schema))
    for key in sorted(params):
        material += _lp(str(key).encode("utf-8"))
        value = params[key]
        if isinstance(value, bool):
            encoded = "bool:1" if value else "bool:0"
        elif isinstance(value, int):
            encoded = f"int:{value}"
        else:
            encoded = f"str:{value}"
        material += _lp(encoded.encode("utf-8"))

    ordered = []
    seen = set()
    for record in records:
        key = record_row_id_bytes(record)
        if key in seen:
            raise CanonicalizationError("duplicate row_id in snapshot content")
        seen.add(key)
        ordered.append((key, record))
    ordered.sort(key=lambda pair: pair[0])

    material += struct.pack(">Q", len(ordered))
    if ordered:
        material += _lp(ordered[0][0])
        material += _lp(ordered[-1][0])
    else:
        material += _lp(b"")
        material += _lp(b"")
    for _, record in ordered:
        material += _lp(record)
    return sha256(bytes(material))


def record_digest(record: bytes) -> bytes:
    """Domain-separated per-row digest used in summary buckets and details."""

    return sha256(_lp(DOMAIN_ITEM) + _lp(record))


def row_hash(row_id_raw: bytes) -> bytes:
    """32-byte domain-separated hash of the row id, used to place summary nodes."""

    return sha256(_lp(DOMAIN_FINGERPRINT) + _lp(row_id_raw))


# Fixed parameters committed by every snapshot.  Session options such as the
# summary bucket count are deliberately not part of snapshot identity: they
# affect only how the same fixed content is probed.
COMMIT_PARAMS = {"encoding": "recon-canonical-v1"}
