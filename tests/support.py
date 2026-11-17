"""Independent helpers for the tests.

These utilities read the two *inputs* (never the protocol's internal summary
state) and compute expectations by direct comparison.  They also provide the
"apply the difference set to a fresh structure" check required by the task.
"""

from __future__ import annotations

import hashlib
import sqlite3
from pathlib import Path
from typing import Any, Iterable, Mapping

from recon.encoding import (
    COMMIT_PARAMS,
    Schema,
    compute_commitment,
    decode_record,
    encode_record,
    record_row_id_bytes,
    schema_wire,
)
from recon.oracle import Expected, compare_inputs


def expected(left, right, schema: Schema) -> Expected:
    return compare_inputs(left, right, schema.field_names())


def canonical_bytes(schema: Schema, rows: Iterable[tuple[str, Mapping[str, Any]]]) -> dict[bytes, bytes]:
    """Map each row's raw UTF-8 id bytes to its canonical record."""

    return {
        row_id.encode("utf-8"): encode_record(schema, row_id, values)
        for row_id, values in rows
    }


def commitment_for(schema: Schema, rows: Iterable[tuple[str, Mapping[str, Any]]]) -> bytes:
    records = [
        encode_record(schema, row_id, values) for row_id, values in rows
    ]
    return compute_commitment(schema, COMMIT_PARAMS, records)


def apply_differences(
    schema: Schema,
    left_rows: Iterable[tuple[str, Mapping[str, Any]]],
    result_differences,
) -> dict[str, bytes]:
    """Apply the reported difference set to the left snapshot independently.

    This is a second, independent application used by tests to validate the
    commitent the initiator computed: it takes the left *input* rows, removes
    the reported only-left and changed-left records by row id, inserts the
    reported peer records, and returns the resulting canonical map.
    """

    left = canonical_bytes(schema, left_rows)
    removed = {record_row_id_bytes(r) for r in result_differences.only_left}
    removed |= {record_row_id_bytes(r) for r in result_differences.changed_left}
    for key in removed:
        if key not in left:
            raise AssertionError(f"reported only-left row {key!r} is absent from the left input")
        del left[key]
    for record in result_differences.only_right + result_differences.changed_right:
        key = record_row_id_bytes(record)
        if key in left:
            raise AssertionError(f"reported peer row {key!r} collides with a kept left row")
        left[key] = record
    # Every record must round-trip through the strict decoder.
    for record in left.values():
        decode_record(schema, record)
    return left


def commitment_of_map(schema: Schema, rows: Mapping[str, bytes]) -> bytes:
    return compute_commitment(schema, COMMIT_PARAMS, rows.values())


def read_sqlite_snapshot(path: str | Path) -> tuple[bytes, dict[str, bytes], dict[str, Any]]:
    """Read raw canonical rows and metadata straight from a snapshot file."""

    conn = sqlite3.connect(f"file:{Path(path).resolve().as_posix()}?mode=ro", uri=True)
    try:
        schema_blob, params_json, row_count, first_key, last_key, commitment = conn.execute(
            "SELECT schema_wire, params_json, row_count, first_key, last_key, commitment "
            "FROM snapshot_meta WHERE id=1"
        ).fetchone()
        rows = {
            bytes(k): bytes(r) for k, r in conn.execute("SELECT row_id, record FROM rows")
        }
    finally:
        conn.close()
    meta = {
        "row_count": row_count,
        "first_key": bytes(first_key),
        "last_key": bytes(last_key),
        "commitment": bytes(commitment),
        "params": params_json,
    }
    return bytes(schema_blob), rows, meta


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()
