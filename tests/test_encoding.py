"""Encoding, schema, and store tests.

These pin the properties the protocol relies on: null/empty-string/zero/false
separation, exact text preservation, bool never equal to int, row identity
separate from content, and commitments that bind schema, parameters, row count
and ordering.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from recon import encoding
from recon.encoding import (
    COMMIT_PARAMS,
    CanonicalizationError,
    Schema,
    SchemaError,
    compute_commitment,
    decode_record,
    encode_record,
    record_row_id_bytes,
    schema_wire,
)
from recon.store import SnapshotError, SnapshotStore, create_snapshot, parse_schema_wire


def test_null_empty_zero_false_are_distinct():
    schema = Schema.declare(
        ("t", "text", True), ("n", "int64", True), ("b", "bool", True)
    )
    variants = [
        {"t": None, "n": None, "b": None},
        {"t": "", "n": None, "b": None},
        {"t": None, "n": 0, "b": None},
        {"t": None, "n": None, "b": False},
        {"t": "0", "n": 0, "b": False},
        {"t": None, "n": None, "b": True},
    ]
    records = [encode_record(schema, "k", values) for values in variants]
    assert len(set(records)) == len(records)
    for record, values in zip(records, variants):
        assert decode_record(schema, record) == ("k", values)


def test_bool_is_not_int_and_int_is_not_bool():
    schema = Schema.declare(("b", "bool", True), ("n", "int64", True))
    for value in (0, 1, 2):
        with pytest.raises(CanonicalizationError):
            encode_record(schema, "k", {"b": value, "n": value})
    with pytest.raises(CanonicalizationError):
        encode_record(schema, "k", {"b": True, "n": True})


def test_text_is_preserved_exactly():
    schema = Schema.declare(("t", "text", True))
    samples = [
        "",
        " leading and trailing ",
        "nul\x00inside",
        "line\nbreak\ttab",
        "ünïcode ✓ 🧪",
        "é",          # decomposed
        "é",           # precomposed (must differ)
        "a" * 1000,
    ]
    records = [encode_record(schema, "r", {"t": s}) for s in samples]
    assert len(set(records)) == len(samples)
    for record, sample in zip(records, samples):
        row_id, values = decode_record(schema, record)
        assert values["t"] == sample


def test_field_boundaries_are_unambiguous():
    schema = Schema.declare(("a", "text"), ("b", "text"))
    # The classic concatenation collision: ("ab","c") vs ("a","bc").
    left = encode_record(schema, "x", {"a": "ab", "b": "c"})
    right = encode_record(schema, "x", {"a": "a", "b": "bc"})
    assert left != right
    assert decode_record(schema, left)[1]["a"] == "ab"
    assert decode_record(schema, right)[1]["a"] == "a"


def test_same_business_content_different_row_id_is_two_rows():
    schema = Schema.declare(("v", "text"))
    one = encode_record(schema, "a", {"v": "same"})
    two = encode_record(schema, "b", {"v": "same"})
    assert one != two
    assert record_row_id_bytes(one) == b"a"
    assert record_row_id_bytes(two) == b"b"


def test_schema_declaration_rules():
    with pytest.raises(SchemaError):
        Schema.declare(("a", "text", True), ("b", "text"))
    with pytest.raises(SchemaError):
        Schema.declare(("a", "varchar"))
    with pytest.raises(SchemaError):
        Schema.declare(("a", "text"), ("a", "text"))
    with pytest.raises(SchemaError):
        Schema.declare()
    ok = Schema.declare(("a", "text"), ("b", "int64"), ("c", "bool"))
    assert ok.field_names() == ("a", "b", "c")


def test_schema_wire_round_trip():
    schema = Schema.declare(("a", "text", True), ("b", "bool", False))
    parsed = parse_schema_wire(schema_wire(schema))
    assert parsed.signature() == schema.signature()
    with pytest.raises(SchemaError):
        parse_schema_wire(schema_wire(schema) + b"junk")


def test_commitment_binds_schema_params_count_and_boundaries():
    schema = Schema.declare(("v", "text"))
    records = [encode_record(schema, "a", {"v": "1"}), encode_record(schema, "b", {"v": "2"})]
    base = compute_commitment(schema, COMMIT_PARAMS, records)
    assert base == compute_commitment(schema, COMMIT_PARAMS, list(reversed(records)))
    assert base != compute_commitment(schema, COMMIT_PARAMS, records[:1])
    assert base != compute_commitment(schema, {**COMMIT_PARAMS, "x": 1}, records)
    other_schema = Schema.declare(("v", "text", True))
    assert base != compute_commitment(other_schema, COMMIT_PARAMS, records)
    with pytest.raises(CanonicalizationError):
        compute_commitment(schema, COMMIT_PARAMS, records + records[:1])


def test_commitment_rejects_duplicate_row_ids():
    schema = Schema.declare(("v", "text"))
    record = encode_record(schema, "a", {"v": "1"})
    with pytest.raises(CanonicalizationError):
        compute_commitment(schema, COMMIT_PARAMS, [record, record])


def test_decode_rejects_non_canonical_forms():
    schema = Schema.declare(("v", "text"))
    good = encode_record(schema, "a", {"v": "x"})
    with pytest.raises(CanonicalizationError):
        decode_record(schema, good + b"\x00")
    # A text prefix whose declared length runs past the buffer end.
    truncated_payload = good[:-1]
    with pytest.raises(CanonicalizationError):
        decode_record(schema, truncated_payload)
    # Wrong type prefix for the declared column.
    wrong_prefix = bytearray(good)
    wrong_prefix[-1 - 1] = ord("I")
    with pytest.raises(CanonicalizationError):
        decode_record(schema, bytes(wrong_prefix))


def test_sqlite_snapshot_round_trip(tmp_path: Path):
    schema = Schema.declare(("v", "text", True), ("n", "int64", True), ("b", "bool", True))
    rows = [
        ("k2", {"v": None, "n": None, "b": None}),
        ("k1", {"v": "", "n": 0, "b": False}),
        ("k3", {"v": "x", "n": 1, "b": True}),
    ]
    path = tmp_path / "snap.sqlite"
    identity = create_snapshot(path, schema, rows, created_from="unit")
    assert identity.row_count == 3
    store = SnapshotStore.open(path)
    try:
        assert store.schema.signature() == schema.signature()
        assert store.identity.commitment == identity.commitment
        by_key = store.records_by_key()
        assert set(by_key) == {b"k1", b"k2", b"k3"}
        for key, record in by_key.items():
            assert record_row_id_bytes(record) == key
            decode_record(schema, record)
    finally:
        store.close()


def test_snapshot_is_read_only_on_disk(tmp_path: Path):
    schema = Schema.declare(("v", "text"))
    path = tmp_path / "snap.sqlite"
    create_snapshot(path, schema, [("a", {"v": "1"})], created_from="ro")
    assert not (path.stat().st_mode & 0o222), "snapshot file must be read-only"
    store = SnapshotStore.open(path)
    try:
        with pytest.raises(sqlite3.OperationalError):
            store._conn.execute("INSERT INTO rows(row_id, record) VALUES (?, ?)", (b"z", b"x"))
    finally:
        store.close()


def test_tampered_snapshot_is_refused(tmp_path: Path):
    import os

    schema = Schema.declare(("v", "text"))
    path = tmp_path / "snap.sqlite"
    create_snapshot(path, schema, [("a", {"v": "1"})], created_from="tamper")
    os.chmod(path, 0o644)
    conn = sqlite3.connect(str(path))
    with conn:
        conn.execute(
            "UPDATE rows SET record = ? WHERE row_id = ?",
            (encode_record(schema, "a", {"v": "2"}), b"a"),
        )
    conn.close()
    with pytest.raises(SnapshotError):
        SnapshotStore.open(path)


def test_snapshot_refuses_overwrite(tmp_path: Path):
    schema = Schema.declare(("v", "text"))
    path = tmp_path / "snap.sqlite"
    create_snapshot(path, schema, [("a", {"v": "1"})], created_from="once")
    with pytest.raises(SnapshotError):
        create_snapshot(path, schema, [("b", {"v": "2"})], created_from="twice")


def test_empty_snapshot_commitment(tmp_path: Path):
    schema = Schema.declare(("v", "text"))
    path = tmp_path / "empty.sqlite"
    identity = create_snapshot(path, schema, [], created_from="empty")
    assert identity.row_count == 0
    assert identity.first_key == b"" and identity.last_key == b""
    store = SnapshotStore.open(path)
    try:
        records = list(store.iter_records())
        assert records == []
        assert compute_commitment(schema, COMMIT_PARAMS, records) == identity.commitment
    finally:
        store.close()
