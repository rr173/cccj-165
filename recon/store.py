"""Immutable SQLite snapshot stores.

Each endpoint owns exactly one SQLite file and can touch no other.  The store
is written once by a loader; afterwards it is opened read-only for every
protocol run.  Rows are stored as canonical bytes (BLOB), never as a
re-typed SQL projection, so SQLite's dynamic typing cannot make ``0``, ``'0'``,
``FALSE`` or ``NULL`` look alike.

Layout (authoritative and fully verifiable at load time):

* ``snapshot_meta(schema_wire, params_json, row_count, first_key, last_key,
  commitment, created_from)`` holds exactly one row, the snapshot identity.
* ``rows(row_id BLOB PRIMARY KEY, record BLOB NOT NULL UNIQUE)`` holds every
  canonical record, keyed by the raw UTF-8 row-id bytes.
* ``messages(message_id TEXT PRIMARY KEY, request_key TEXT NOT NULL UNIQUE,
  payload BLOB NOT NULL)`` is the peer's stored-response cache.  It is not part
  of the snapshot content or commitment.

The commitment is recomputed from the stored rows when a store is opened, and
any mismatch is a hard error.  A snapshot that fails verification is never
served.
"""

from __future__ import annotations

import json
import os
import sqlite3
import struct
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Iterator, Mapping

from . import encoding
from .encoding import (
    CanonicalizationError,
    COMMIT_PARAMS,
    Schema,
    SchemaError,
    decode_record,
    encode_record,
    record_row_id_bytes,
    schema_wire,
)

PAGE_MARGIN = 0


class SnapshotError(RuntimeError):
    """The on-disk snapshot is corrupt, inconsistent, or not immutable."""


_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS snapshot_meta (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    schema_wire BLOB NOT NULL,
    params_json TEXT NOT NULL,
    row_count INTEGER NOT NULL,
    first_key BLOB NOT NULL,
    last_key BLOB NOT NULL,
    commitment BLOB NOT NULL,
    created_from TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS rows (
    row_id BLOB PRIMARY KEY,
    record BLOB NOT NULL UNIQUE
) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS messages (
    message_id TEXT PRIMARY KEY,
    request_key TEXT NOT NULL UNIQUE,
    payload BLOB NOT NULL
);
"""


@dataclass(frozen=True)
class SnapshotIdentity:
    """The public identity of one fixed snapshot.

    Hashing this pair (schema wire + params + commitment + row count + boundary
    keys) yields the ``snapshot_id`` carried by every protocol message.
    """

    schema_wire: bytes
    commitment: bytes
    row_count: int
    first_key: bytes
    last_key: bytes
    params: Mapping[str, object]

    def snapshot_id(self) -> str:
        material = bytearray()
        material += encoding._lp(b"RECON:SNAPSHOT-ID:v1")
        material += encoding._lp(self.schema_wire)
        material += encoding._lp(self.commitment)
        material += struct.pack(">Q", self.row_count)
        material += encoding._lp(self.first_key)
        material += encoding._lp(self.last_key)
        for key in sorted(self.params):
            material += encoding._lp(str(key).encode("utf-8"))
            material += encoding._lp(repr(self.params[key]).encode("utf-8"))
        return encoding.sha256(bytes(material)).hex()

    def short(self) -> str:
        return self.snapshot_id()[:16]


def create_snapshot(
    path: str | os.PathLike[str],
    schema: Schema,
    rows: Iterable[tuple[str, Mapping[str, object]]],
    *,
    created_from: str,
    params: Mapping[str, object] | None = None,
) -> SnapshotIdentity:
    """Write a brand-new immutable snapshot.

    The target file must not already exist.  Every row is validated against the
    schema, encoded canonically, and de-duplicated by row id before anything is
    written.  The commitment is computed from exactly the bytes that are
    stored.
    """

    target = Path(path)
    if target.exists():
        raise SnapshotError(f"refusing to overwrite existing snapshot: {target}")
    target.parent.mkdir(parents=True, exist_ok=True)

    # A snapshot may only carry the fixed commitment parameters.  Negotiation
    # accepts exactly this set, so allowing extras here would produce a
    # snapshot that can never be reconciled.
    effective_params = dict(COMMIT_PARAMS)
    if params:
        extra = set(params) - set(COMMIT_PARAMS)
        if extra:
            raise SnapshotError(f"unsupported commitment parameters: {sorted(extra)}")
        effective_params.update(params)

    encoded: dict[bytes, bytes] = {}
    row_ids: set[str] = set()
    for row_id, values in rows:
        if row_id in row_ids:
            raise SnapshotError(f"duplicate row_id in input: {row_id!r}")
        row_ids.add(row_id)
        record = encode_record(schema, row_id, values)
        encoded[record_row_id_bytes(record)] = record

    ordered_keys = sorted(encoded)
    ordered_records = [encoded[k] for k in ordered_keys]
    first_key = ordered_keys[0] if ordered_keys else b""
    last_key = ordered_keys[-1] if ordered_keys else b""
    commitment = encoding.compute_commitment(schema, effective_params, ordered_records)

    tmp = target.with_name(target.name + ".tmp")
    if tmp.exists():
        tmp.unlink()
    conn = sqlite3.connect(str(tmp))
    try:
        conn.execute("PRAGMA journal_mode=DELETE")
        conn.execute("PRAGMA synchronous=FULL")
        conn.executescript(_SCHEMA_SQL)
        with conn:
            conn.executemany(
                "INSERT INTO rows(row_id, record) VALUES (?, ?)",
                ((k, encoded[k]) for k in ordered_keys),
            )
            conn.execute(
                "INSERT INTO snapshot_meta(id, schema_wire, params_json, row_count, "
                "first_key, last_key, commitment, created_from) "
                "VALUES (1, ?, ?, ?, ?, ?, ?, ?)",
                (
                    schema_wire(schema),
                    json.dumps(effective_params, sort_keys=True, separators=(",", ":")),
                    len(ordered_records),
                    first_key,
                    last_key,
                    commitment,
                    created_from,
                ),
            )
    finally:
        conn.close()

    # Verify what was just written, including the commitment recomputed from
    # stored bytes, before publishing the file.  Then make the snapshot
    # read-only at the filesystem level so accidental writes fail there too.
    store = SnapshotStore.open(tmp)
    identity = store.identity
    if identity.commitment != commitment:
        raise SnapshotError("written snapshot did not verify against its commitment")
    store.close()

    os.chmod(tmp, 0o444)
    if target.exists():
        target.unlink()
    os.replace(tmp, target)
    os.chmod(target, 0o444)
    return identity


class SnapshotStore:
    """Read-only access to one verified snapshot plus a message cache."""

    def __init__(self, conn: sqlite3.Connection, path: Path, schema: Schema,
                 identity: SnapshotIdentity):
        self._conn = conn
        self.path = path
        self.schema = schema
        self.identity = identity
        self._cache: dict[bytes, bytes] | None = None

    @staticmethod
    def open(path: str | os.PathLike[str]) -> "SnapshotStore":
        target = Path(path)
        if not target.is_file():
            raise SnapshotError(f"snapshot does not exist: {target}")
        uri = f"file:{target.resolve().as_posix()}?mode=ro"
        conn = sqlite3.connect(uri, uri=True, check_same_thread=False)
        conn.execute("PRAGMA query_only=ON")
        try:
            store = SnapshotStore._verify(conn, target)
        except Exception:
            conn.close()
            raise
        return store

    @staticmethod
    def _verify(conn: sqlite3.Connection, path: Path) -> "SnapshotStore":
        row = conn.execute(
            "SELECT schema_wire, params_json, row_count, first_key, last_key, "
            "commitment, created_from FROM snapshot_meta WHERE id = 1"
        ).fetchone()
        if row is None:
            raise SnapshotError(f"snapshot has no metadata: {path}")
        (
            schema_blob,
            params_json,
            row_count,
            first_key,
            last_key,
            commitment,
            created_from,
        ) = row
        schema = parse_schema_wire(bytes(schema_blob))
        params = json.loads(params_json)

        stored_count = conn.execute("SELECT COUNT(*) FROM rows").fetchone()[0]
        if stored_count != row_count:
            raise SnapshotError(
                f"snapshot row count mismatch: meta says {row_count}, rows table has {stored_count}"
            )

        bounds = conn.execute(
            "SELECT MIN(row_id), MAX(row_id) FROM rows"
        ).fetchone()
        actual_first, actual_last = bounds
        actual_first = actual_first or b""
        actual_last = actual_last or b""
        if actual_first != bytes(first_key) or actual_last != bytes(last_key):
            raise SnapshotError("snapshot boundary keys do not match the stored rows")

        identity = SnapshotIdentity(
            schema_wire=bytes(schema_blob),
            commitment=bytes(commitment),
            row_count=row_count,
            first_key=actual_first,
            last_key=actual_last,
            params=params,
        )

        records: list[bytes] = []
        seen: set[bytes] = set()
        for stored_key, stored_record in conn.execute(
            "SELECT row_id, record FROM rows ORDER BY row_id"
        ):
            key = bytes(stored_key)
            record = bytes(stored_record)
            if key in seen:
                raise SnapshotError("duplicate row id in stored rows")
            seen.add(key)
            try:
                row_id, values = decode_record(schema, record)
            except CanonicalizationError as exc:
                raise SnapshotError(f"stored record is not canonical: {exc}") from exc
            if record_row_id_bytes(record) != key:
                raise SnapshotError("stored row key does not match its record")
            records.append(record)

        recomputed = encoding.compute_commitment(schema, params, records)
        if recomputed != bytes(commitment):
            raise SnapshotError(
                "snapshot content does not match its commitment; refusing to serve it"
            )
        store = SnapshotStore(conn, path, schema, identity)
        store._created_from = created_from
        return store

    # ------------------------------------------------------------------ rows

    def iter_records(self) -> Iterator[bytes]:
        for (record,) in self._conn.execute("SELECT record FROM rows ORDER BY row_id"):
            yield bytes(record)

    def records_by_key(self) -> dict[bytes, bytes]:
        if self._cache is None:
            self._cache = {
                bytes(k): bytes(r)
                for k, r in self._conn.execute("SELECT row_id, record FROM rows ORDER BY row_id")
            }
        return self._cache

    def get_record(self, row_key: bytes) -> bytes | None:
        row = self._conn.execute(
            "SELECT record FROM rows WHERE row_id = ?", (row_key,)
        ).fetchone()
        return bytes(row[0]) if row else None

    def get_records(self, row_keys: Iterable[bytes]) -> tuple[list[bytes], list[bytes]]:
        """Return (found records, missing keys), both in the requested order."""

        found: list[bytes] = []
        missing: list[bytes] = []
        for key in row_keys:
            record = self.get_record(key)
            if record is None:
                missing.append(key)
            else:
                found.append(record)
        return found, missing

    def records_in_range(
        self,
        start: bytes | None,
        end: bytes | None,
        *,
        inclusive_end: bool = False,
        limit: int | None = None,
    ) -> list[bytes]:
        """Records with ``start <= row_id < end`` (or ``<= end`` when inclusive)."""

        clauses = []
        args: list[bytes] = []
        if start is not None:
            clauses.append("row_id >= ?")
            args.append(start)
        if end is not None:
            clauses.append("row_id <= ?" if inclusive_end else "row_id < ?")
            args.append(end)
        sql = "SELECT record FROM rows"
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        sql += " ORDER BY row_id"
        if limit is not None:
            sql += f" LIMIT {int(limit)}"
        return [bytes(r) for (r,) in self._conn.execute(sql, args)]

    def __len__(self) -> int:
        return self.identity.row_count

    def close(self) -> None:
        self._conn.close()

    def __enter__(self) -> "SnapshotStore":
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    # -------------------------------------------------------------- messages

    def find_cached_answer(self, request_key: str) -> tuple[str, bytes] | None:
        row = self._conn.execute(
            "SELECT message_id, payload FROM messages WHERE request_key = ?",
            (request_key,),
        ).fetchone()
        if row is None:
            return None
        return row[0], bytes(row[1])

    def messages_count(self) -> int:
        return self._conn.execute("SELECT COUNT(*) FROM messages").fetchone()[0]

    def store_answer(self, message_id: str, request_key: str, payload: bytes) -> None:
        """Persist a freshly produced answer.

        This is only possible for snapshots opened read-write; read-only
        serving uses an :class:`AnswerCache` sidecar instead.  Duplicate
        delivery is handled by the protocol engine before reaching here; the
        UNIQUE request_key is the storage-level backstop.
        """

        with self._conn:
            self._conn.execute(
                "INSERT INTO messages(message_id, request_key, payload) VALUES (?, ?, ?)",
                (message_id, request_key, payload),
            )


class AnswerCache:
    """Durable replay cache kept in a sidecar SQLite file next to a read-only
    snapshot.

    Snapshot content is never written: the snapshot database is opened with
    ``mode=ro`` and SQLite enforces that.  Answers live in a separate file
    owned by the serving endpoint, so a replay after restart is served from
    disk rather than recomputed.
    """

    def __init__(self, conn: sqlite3.Connection) -> None:
        self._conn = conn

    @staticmethod
    def open(path: str | os.PathLike[str]) -> "AnswerCache":
        target = Path(path)
        target.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(str(target), check_same_thread=False)
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS messages (
                message_id TEXT PRIMARY KEY,
                request_key TEXT NOT NULL UNIQUE,
                payload BLOB NOT NULL
            );
            """
        )
        conn.commit()
        return AnswerCache(conn)

    def find(self, request_key: str) -> tuple[str, bytes] | None:
        row = self._conn.execute(
            "SELECT message_id, payload FROM messages WHERE request_key = ?",
            (request_key,),
        ).fetchone()
        if row is None:
            return None
        return row[0], bytes(row[1])

    def store(self, message_id: str, request_key: str, payload: bytes) -> None:
        with self._conn:
            self._conn.execute(
                "INSERT OR IGNORE INTO messages(message_id, request_key, payload) "
                "VALUES (?, ?, ?)",
                (message_id, request_key, payload),
            )

    def count(self) -> int:
        return self._conn.execute("SELECT COUNT(*) FROM messages").fetchone()[0]

    def close(self) -> None:
        self._conn.close()


def open_readonly(path: str | os.PathLike[str]) -> SnapshotStore:
    """Verify and open a snapshot read-only (the normal serving mode)."""

    return SnapshotStore.open(path)


def parse_schema_wire(blob: bytes) -> Schema:
    """Parse a schema declaration received from the peer, strictly."""

    if not blob.startswith(encoding.MAGIC_SCHEMA):
        raise SchemaError("schema wire form has the wrong magic")
    pos = len(encoding.MAGIC_SCHEMA)
    if len(blob) < pos + 4:
        raise SchemaError("truncated schema field count")
    (count,) = struct.unpack(">I", blob[pos : pos + 4])
    pos += 4
    if count == 0 or count > 1024:
        raise SchemaError(f"implausible schema field count {count}")
    specs: list[encoding.FieldSpec] = []
    for _ in range(count):
        name, pos = _read_lp(blob, pos)
        ftype, pos = _read_lp(blob, pos)
        if pos >= len(blob):
            raise SchemaError("truncated schema nullability flag")
        flag = blob[pos]
        pos += 1
        if flag not in (0, 1):
            raise SchemaError("invalid schema nullability flag")
        try:
            name_s = name.decode("utf-8", errors="strict")
            type_s = ftype.decode("ascii", errors="strict")
        except UnicodeDecodeError as exc:
            raise SchemaError("schema contains invalid text") from exc
        specs.append(encoding.FieldSpec(name_s, type_s, bool(flag)))
    if pos != len(blob):
        raise SchemaError("trailing bytes after schema declaration")
    return Schema(tuple(specs))


def _read_lp(blob: bytes, pos: int) -> tuple[bytes, int]:
    if len(blob) < pos + 4:
        raise SchemaError("truncated length-prefixed element")
    (length,) = struct.unpack(">I", blob[pos : pos + 4])
    start = pos + 4
    end = start + length
    if end > len(blob):
        raise SchemaError("truncated length-prefixed element")
    return blob[start:end], end
