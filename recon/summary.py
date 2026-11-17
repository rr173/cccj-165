"""Layered key-hash summaries, node proofs, and exact local comparison.

Summary model
-------------
Every row is placed in a *position* derived only from its canonical row id: the
top ``position_bits`` bits (default 64) of
``sha256("RECON:FINGERPRINT:v1", len(row_id), row_id)``.  A summary node fixes
the top ``gen`` position bits to ``tag``; generation 0 is the whole snapshot.
Splitting a node yields exactly ``tag << 1`` and ``(tag << 1) | 1``.

A *region* is a node plus an optional row-key interval ``[lo, hi)``.  Hash
splitting refines the node; key splitting refines the interval.  Both sides
interpret a region identically, so no side guesses where differences live.
Placement never depends on field values, on the peer's view, or on test data.

An aggregate summary is small: row count, XOR of 40-byte
``position || row_digest`` fingerprints, a SHA-256 digest of those
accumulators, and the region's median row key.  It is an order-independent
detector.  Equal aggregates mean equal multisets under the SHA-256 collision
assumption; a differing aggregate only says "look closer".

A cord is the decodable form: ``<u64 count><(u64 position, 32-byte digest)>*``
sorted.  Comparing cords yields the exact set of positions that may differ.
A cord is never treated as content: candidate positions are resolved by
fetching the peer's actual canonical records and comparing by row id.

Proofs bind a summary payload to its region (generation, tag, width, bounds)
and kind with domain separation, so a summary cannot be replayed under another
node or region.  A cord that will not decode, fails its proof, or exceeds the
configured byte budget triggers refinement or a logged exact fallback; it is
never taken as "equal" and never silently dropped.
"""

from __future__ import annotations

import bisect
import struct
from dataclasses import dataclass
from typing import Iterable, Sequence

from . import encoding
from .encoding import record_digest, sha256

DEFAULT_POSITION_BITS = 64
POSITION_BITS_MAX = 64
DIGEST_BYTES = 32
FP_BYTES = 8 + DIGEST_BYTES

AGGREGATE = struct.Struct(">Q40s")      # (count, xor of fingerprints)
CORD_HEADER = struct.Struct(">Q")       # count
CORD_ENTRY = struct.Struct(">Q32s")     # (position, full SHA-256 row digest)
CORD_ENTRY_BYTES = CORD_ENTRY.size
AGGREGATE_BYTES = AGGREGATE.size

PROOF_DOMAIN = b"RECON:NODE-PROOF:v1"
NODE_DIGEST_DOMAIN = b"RECON:NODE-DIGEST:v1"


class SummaryError(ValueError):
    """A summary cord, aggregate, node, region, or proof is invalid."""


@dataclass(frozen=True)
class NodeId:
    gen: int
    tag: int
    bits: int = DEFAULT_POSITION_BITS

    def __post_init__(self) -> None:
        if not 1 <= self.bits <= POSITION_BITS_MAX:
            raise SummaryError(f"position width {self.bits} out of range")
        if not 0 <= self.gen <= self.bits:
            raise SummaryError(f"generation {self.gen} out of range for width {self.bits}")
        if self.gen == 0:
            if self.tag != 0:
                raise SummaryError("generation 0 has exactly one node, tag 0")
        elif not 0 <= self.tag < (1 << self.gen):
            raise SummaryError(f"tag {self.tag} out of range for generation {self.gen}")

    def children(self) -> tuple["NodeId", "NodeId"]:
        if self.gen >= self.bits:
            raise SummaryError(f"node {self.label()} cannot be split at width {self.bits}")
        base = self.tag << 1
        return (
            NodeId(self.gen + 1, base, self.bits),
            NodeId(self.gen + 1, base | 1, self.bits),
        )

    def position_range(self) -> tuple[int, int]:
        if self.gen == 0:
            return 0, 1 << self.bits
        shift = self.bits - self.gen
        lo = self.tag << shift
        return lo, lo + (1 << shift)

    def contains(self, position: int) -> bool:
        if not 0 <= position < (1 << self.bits):
            return False
        if self.gen == 0:
            return True
        return position >> (self.bits - self.gen) == self.tag

    def label(self) -> str:
        if self.gen == 0:
            return f"g0:root@{self.bits}"
        return f"g{self.gen}:{self.tag:0{self.gen}b}@{self.bits}"


@dataclass(frozen=True)
class Region:
    """A hash node plus an optional row-key interval with explicit inclusivity.

    ``lo``/``hi`` are raw row-id bytes; ``None`` means unbounded.  Bounds are
    part of every summary proof, so the two sides always interpret a region
    identically.  Key splits produce ``[lo..pivot]`` and ``(pivot..hi]``;
    explicit inclusivity avoids a degenerate split when the pivot equals an
    endpoint.
    """

    node: NodeId
    lo: bytes | None = None
    lo_incl: bool = True
    hi: bytes | None = None
    hi_incl: bool = False

    def __post_init__(self) -> None:
        if self.lo is None and not self.lo_incl:
            raise SummaryError("unbounded lower bound cannot be exclusive")
        if self.hi is None and self.hi_incl:
            raise SummaryError("unbounded upper bound cannot be inclusive")
        if self.lo is not None and self.hi is not None:
            if self.lo > self.hi:
                raise SummaryError("region lower bound is above its upper bound")
            if self.lo == self.hi and not (self.lo_incl and self.hi_incl):
                raise SummaryError("region interval is empty")

    def contains_key(self, key: bytes) -> bool:
        if not isinstance(key, bytes):
            raise SummaryError("row key must be bytes")
        if self.lo is not None:
            if key < self.lo or (key == self.lo and not self.lo_incl):
                return False
        if self.hi is not None:
            if key > self.hi or (key == self.hi and not self.hi_incl):
                return False
        return True

    def contains(self, position: int, key: bytes) -> bool:
        return self.node.contains(position) and self.contains_key(key)

    def hash_children(self) -> tuple["Region", "Region"]:
        a, b = self.node.children()
        return (
            Region(a, self.lo, self.lo_incl, self.hi, self.hi_incl),
            Region(b, self.lo, self.lo_incl, self.hi, self.hi_incl),
        )

    def key_children(self, pivot: bytes) -> tuple["Region", "Region"]:
        """Split into ``[lo..pivot]`` and ``(pivot..hi]``.

        The pivot must be a row key of this region, so the left child is
        non-empty and the right child is non-empty whenever the region holds a
        key above the pivot.  If the pivot is the region's upper bound the
        right child is exactly empty, which callers treat as no progress.
        """

        if not isinstance(pivot, bytes) or not pivot:
            raise SummaryError("key pivot must be non-empty bytes")
        if not self.contains_key(pivot):
            raise SummaryError("key pivot lies outside the region")
        left = Region(self.node, self.lo, self.lo_incl, pivot, True)
        if self.hi is not None and pivot >= self.hi:
            return left, None  # type: ignore[return-value]
        right = Region(self.node, pivot, False, self.hi, self.hi_incl)
        return left, right

    def label(self) -> str:
        lo = "-" if self.lo is None else ("[" if self.lo_incl else "(") + self.lo.hex()
        hi = "-" if self.hi is None else self.hi.hex() + ("]" if self.hi_incl else ")")
        return f"{self.node.label()}{lo}:{hi}"

    def to_wire(self) -> dict:
        return {
            "gen": self.node.gen,
            "tag": self.node.tag,
            "bits": self.node.bits,
            "lo": self.lo.hex() if self.lo is not None else None,
            "lo_incl": self.lo_incl,
            "hi": self.hi.hex() if self.hi is not None else None,
            "hi_incl": self.hi_incl,
        }

    @staticmethod
    def from_wire(obj: dict) -> "Region":
        try:
            gen = int(obj["gen"])
            tag = int(obj["tag"])
            bits = int(obj["bits"])
            lo = bytes.fromhex(obj["lo"]) if obj.get("lo") is not None else None
            hi = bytes.fromhex(obj["hi"]) if obj.get("hi") is not None else None
            lo_incl = bool(obj["lo_incl"])
            hi_incl = bool(obj["hi_incl"])
        except (KeyError, TypeError, ValueError) as exc:
            raise SummaryError(f"malformed region: {exc}") from exc
        return Region(NodeId(gen, tag, bits), lo, lo_incl, hi, hi_incl)

    @staticmethod
    def root(bits: int = DEFAULT_POSITION_BITS) -> "Region":
        return Region(NodeId(0, 0, bits))


def position_of_row_key(row_key: bytes, bits: int = DEFAULT_POSITION_BITS) -> int:
    if not 1 <= bits <= POSITION_BITS_MAX:
        raise SummaryError(f"position width {bits} out of range")
    digest = encoding.row_hash(row_key)
    return int.from_bytes(digest[:8], "big") >> (64 - bits)


def digest_of(record: bytes) -> bytes:
    """Full domain-separated SHA-256 digest of a canonical record."""

    return record_digest(record)


def fingerprint(position: int, digest: bytes) -> bytes:
    return struct.pack(">Q", position) + digest


@dataclass(frozen=True)
class Entry:
    position: int
    digest: bytes

    def __post_init__(self) -> None:
        if not isinstance(self.digest, bytes) or len(self.digest) != DIGEST_BYTES:
            raise SummaryError("summary entry digest must be 32 bytes")

    def fingerprint(self) -> bytes:
        return fingerprint(self.position, self.digest)


@dataclass(frozen=True)
class Aggregate:
    count: int
    xor: bytes
    digest: bytes

    def equals(self, other: "Aggregate") -> bool:
        return (
            self.count == other.count
            and self.xor == other.xor
            and self.digest == other.digest
        )


def build_aggregate(rows: Iterable["IndexedRecord"], *, domain: bytes = NODE_DIGEST_DOMAIN) -> Aggregate:
    """Count, XOR, and modular sum of row fingerprints, then a domain-separated
    digest.

    Works on the precomputed ``fingerprint`` of each index entry.  Both
    accumulators are order-independent, so a node summary is linear in its
    rows.  They are difference detectors only: the final content commitment
    is what decides equality of full snapshots.
    """

    count = 0
    xor = bytearray(FP_BYTES)
    total = 0
    modulus = 1 << (8 * FP_BYTES)
    for row in rows:
        count += 1
        total += row.fingerprint_int
        if total >= (1 << 120):
            total %= modulus
        fp = row.fingerprint
        for i in range(FP_BYTES):
            xor[i] ^= fp[i]
    total %= modulus
    digest = sha256(
        encoding._lp(domain)
        + encoding._lp(b"aggregate-v1")
        + struct.pack(">Q", count)
        + encoding._lp(bytes(xor))
        + encoding._lp(total.to_bytes(FP_BYTES, "big"))
    )
    return Aggregate(count, bytes(xor), digest)


@dataclass(frozen=True)
class NodeSummary:
    region: Region
    count: int
    xor: bytes
    digest: bytes
    median: bytes | None

    def aggregate(self) -> Aggregate:
        return Aggregate(self.count, self.xor, self.digest)

    def payload(self, kind: str) -> bytes:
        median_field = b"\x01" + encoding._lp(self.median) if self.median is not None else b"\x00"
        return (
            kind.encode("ascii")
            + struct.pack(">Q", self.count)
            + self.xor
            + self.digest
            + median_field
        )

    def to_wire(self, kind: str) -> dict:
        return {
            "region": self.region.to_wire(),
            "count": self.count,
            "xor": self.xor.hex(),
            "digest": self.digest.hex(),
            "median": self.median.hex() if self.median is not None else None,
            "proof": node_proof(kind, self.region, self.payload(kind)).hex(),
        }

    @staticmethod
    def from_wire(obj: dict, kind: str) -> "NodeSummary":
        try:
            region = Region.from_wire(obj["region"])
            count = int(obj["count"])
            xor = bytes.fromhex(obj["xor"])
            digest = bytes.fromhex(obj["digest"])
            median = bytes.fromhex(obj["median"]) if obj.get("median") is not None else None
            proof_hex = obj["proof"]
        except (KeyError, TypeError, ValueError) as exc:
            raise SummaryError(f"malformed node summary: {exc}") from exc
        if count < 0 or len(xor) != FP_BYTES or len(digest) != 32:
            raise SummaryError("node summary has wrong-sized fields")
        summary = NodeSummary(region, count, xor, digest, median)
        verify_proof(kind, region, summary.payload(kind), proof_hex)
        return summary


def build_cord(entries: Iterable[Entry]) -> bytes:
    pairs = sorted((e.position, e.digest) for e in entries)
    out = bytearray(CORD_HEADER.pack(len(pairs)))
    for position, digest in pairs:
        out += CORD_ENTRY.pack(position, digest)
    return bytes(out)


def cord_size(count: int) -> int:
    return CORD_HEADER.size + count * CORD_ENTRY_BYTES


def decode_cord(cord: bytes, region: Region, *, expected_count: int | None = None) -> list[Entry]:
    """Strictly decode a region cord.

    Rejects truncation, count/length disagreement, positions outside the node,
    unsorted entries, and a count that disagrees with the summary.  Duplicate
    positions are permitted because real hash collisions can put several rows
    at one position; details resolve them by row id.
    """

    if len(cord) < CORD_HEADER.size:
        raise SummaryError("cord is shorter than its header")
    (count,) = CORD_HEADER.unpack_from(cord, 0)
    expected_len = cord_size(count)
    if len(cord) != expected_len:
        raise SummaryError(
            f"cord byte length {len(cord)} disagrees with declared count {count} "
            f"(expected {expected_len})"
        )
    if expected_count is not None and count != expected_count:
        raise SummaryError(
            f"cord count {count} disagrees with the node summary count {expected_count}"
        )
    entries: list[Entry] = []
    previous: tuple[int, bytes] | None = None
    for i in range(count):
        position, digest = CORD_ENTRY.unpack_from(cord, CORD_HEADER.size + i * CORD_ENTRY_BYTES)
        if not region.node.contains(position):
            raise SummaryError(f"cord entry {i} lies outside node {region.node.label()}")
        current = (position, digest)
        if previous is not None and current < previous:
            raise SummaryError("cord entries are not sorted")
        previous = current
        entries.append(Entry(position, digest))
    return entries


def node_proof(kind: str, region: Region, payload: bytes) -> bytes:
    """Domain-separated proof binding a summary payload to its region."""

    lo = b"\x00" if region.lo is None else b"\x01" + region.lo
    hi = b"\x00" if region.hi is None else b"\x01" + region.hi
    material = (
        encoding._lp(PROOF_DOMAIN)
        + encoding._lp(kind.encode("ascii"))
        + struct.pack(">BQB", region.node.gen, region.node.tag, region.node.bits)
        + bytes([1 if region.lo_incl else 0, 1 if region.hi_incl else 0])
        + encoding._lp(lo)
        + encoding._lp(hi)
        + encoding._lp(payload)
    )
    return sha256(material)


def verify_proof(kind: str, region: Region, payload: bytes, proof_hex: str) -> None:
    try:
        proof = bytes.fromhex(proof_hex)
    except (TypeError, ValueError) as exc:
        raise SummaryError("proof is not valid hex") from exc
    if len(proof) != 32:
        raise SummaryError("proof has the wrong length")
    if proof != node_proof(kind, region, payload):
        raise SummaryError("summary proof does not match its region and payload")


@dataclass(frozen=True)
class CordComparison:
    positions_with_differences: tuple[int, ...]
    equal_entries: int
    left_entries: int
    right_entries: int

    @property
    def empty(self) -> bool:
        return not self.positions_with_differences


def compare_cords(left: Sequence[Entry], right: Sequence[Entry]) -> CordComparison:
    """Compare cords as multisets of ``(position, digest_prefix)`` pairs.

    Equal pairs cancel.  A colliding position that holds different rows is
    still resolved by fetching actual records afterwards.
    """

    left_by_pos: dict[int, list[bytes]] = {}
    right_by_pos: dict[int, list[bytes]] = {}
    for entry in left:
        left_by_pos.setdefault(entry.position, []).append(entry.digest)
    for entry in right:
        right_by_pos.setdefault(entry.position, []).append(entry.digest)
    differing: list[int] = []
    equal_entries = 0
    for position in sorted(set(left_by_pos) | set(right_by_pos)):
        ldigests = sorted(left_by_pos.get(position, ()))
        rdigests = sorted(right_by_pos.get(position, ()))
        if ldigests == rdigests:
            equal_entries += len(ldigests)
        else:
            differing.append(position)
    return CordComparison(
        positions_with_differences=tuple(differing),
        equal_entries=equal_entries,
        left_entries=len(left),
        right_entries=len(right),
    )


# ---------------------------------------------------------------------------
# Local index


class IndexedRecord:
    """One row of a local snapshot with its precomputed placement data."""

    __slots__ = (
        "position",
        "digest",
        "record",
        "row_key",
        "fingerprint",
        "fingerprint_int",
        "_entry",
    )

    def __init__(self, position: int, digest: bytes, record: bytes, row_key: bytes) -> None:
        self.position = position
        self.digest = digest
        self.record = record
        self.row_key = row_key
        self.fingerprint = position.to_bytes(8, "big") + digest
        self.fingerprint_int = int.from_bytes(self.fingerprint, "big")
        self._entry = Entry(position, digest)

    def entry(self) -> Entry:
        return self._entry


class LocalIndex:
    """One side's own snapshot indexed by position and row key."""

    def __init__(self, records: Iterable[bytes], bits: int = DEFAULT_POSITION_BITS) -> None:
        if not 1 <= bits <= POSITION_BITS_MAX:
            raise ValueError("position width out of range")
        self.bits = bits
        entries: list[IndexedRecord] = []
        by_key: dict[bytes, IndexedRecord] = {}
        for record in records:
            row_key = encoding.record_row_id_bytes(record)
            if row_key in by_key:
                raise ValueError("duplicate row id in local index")
            entry = IndexedRecord(
                position_of_row_key(row_key, bits),
                digest_of(record),
                record,
                row_key,
            )
            entries.append(entry)
            by_key[row_key] = entry
        entries.sort(key=lambda e: (e.position, e.record))
        self._entries = entries
        self._positions = [e.position for e in entries]
        self._by_key = by_key
        # Position-sorted order is also row-key sorted, because the canonical
        # record bytes begin with a length-prefixed row id and positions only
        # group rows.  Re-sort by row key explicitly for clarity and safety.
        self._sorted = sorted(entries, key=lambda e: e.row_key)
        self._sorted_keys = [e.row_key for e in self._sorted]
        self._by_position: dict[int, list[IndexedRecord]] = {}
        for e in entries:
            self._by_position.setdefault(e.position, []).append(e)

    def __len__(self) -> int:
        return len(self._entries)

    def get(self, row_key: bytes) -> IndexedRecord | None:
        return self._by_key.get(row_key)

    def node_slice(self, region: Region) -> list[IndexedRecord]:
        lo, hi = region.node.position_range()
        start = bisect.bisect_left(self._positions, lo)
        end = bisect.bisect_left(self._positions, hi)
        if region.lo is None and region.hi is None:
            return self._entries[start:end]
        return [e for e in self._entries[start:end] if region.contains_key(e.row_key)]

    def node_entries(self, region: Region) -> list[Entry]:
        return [e.entry() for e in self.node_slice(region)]

    def node_aggregate(self, region: Region) -> Aggregate:
        return build_aggregate(self.node_slice(region))

    def node_cord(self, region: Region) -> bytes:
        return build_cord(self.node_entries(region))

    def node_count(self, region: Region) -> int:
        lo, hi = region.node.position_range()
        start = bisect.bisect_left(self._positions, lo)
        end = bisect.bisect_left(self._positions, hi)
        if region.lo is None and region.hi is None:
            return end - start
        return sum(1 for e in self._entries[start:end] if region.contains_key(e.row_key))

    def node_by_key(self, region: Region) -> dict[bytes, bytes]:
        return {e.row_key: e.record for e in self.node_slice(region)}

    def node_sorted_by_key(self, region: Region) -> list[IndexedRecord]:
        return sorted(self.node_slice(region), key=lambda e: e.row_key)

    def records_at_position(self, region: Region, position: int) -> list[IndexedRecord]:
        if not region.node.contains(position):
            raise SummaryError("position lies outside the node")
        return [e for e in self._by_position.get(position, ()) if region.contains_key(e.row_key)]

    def keys_at_positions(self, region: Region, positions: Iterable[int]) -> list[bytes]:
        keys: list[bytes] = []
        for position in positions:
            keys.extend(e.row_key for e in self.records_at_position(region, position))
        keys.sort()
        return keys

    def median_key(self, region: Region) -> bytes | None:
        keys = [e.row_key for e in self.node_slice(region)]
        if not keys:
            return None
        keys.sort()
        return keys[(len(keys) - 1) // 2]

    def node_summary(self, region: Region) -> NodeSummary:
        rows = self.node_slice(region)
        agg = build_aggregate(rows)
        median = None
        if rows:
            # The median must be an actual row key of this region.
            median = sorted(rows, key=lambda e: e.row_key)[(len(rows) - 1) // 2].row_key
        return NodeSummary(
            region=region,
            count=agg.count,
            xor=agg.xor,
            digest=agg.digest,
            median=median,
        )

    def all_keys(self) -> list[bytes]:
        return self._sorted_keys

    def records_in_key_range(
        self, region: Region, after: bytes | None, limit: int
    ) -> list[IndexedRecord]:
        """Region rows with row key strictly greater than ``after``, in key order."""

        if limit < 1:
            raise ValueError("limit must be positive")
        slice_ = sorted(self.node_slice(region), key=lambda e: e.row_key)
        keys = [e.row_key for e in slice_]
        start = bisect.bisect_right(keys, after) if after is not None else 0
        return slice_[start : start + limit]

    def rows_at_positions_page(
        self,
        region: Region,
        positions: set[int],
        after: bytes | None,
        limit: int,
    ) -> list[IndexedRecord]:
        rows = [e for e in self.node_slice(region) if e.position in positions]
        rows.sort(key=lambda e: e.row_key)
        keys = [e.row_key for e in rows]
        start = bisect.bisect_right(keys, after) if after is not None else 0
        return rows[start : start + limit]

    def remaining_rows_at_positions(
        self, region: Region, positions: set[int], after: bytes | None
    ) -> int:
        rows = [e for e in self.node_slice(region) if e.position in positions]
        if after is None:
            return len(rows)
        return sum(1 for e in rows if e.row_key > after)

    def count_rows_at_positions(self, region: Region, positions: set[int]) -> int:
        return sum(1 for e in self.node_slice(region) if e.position in positions)


# ---------------------------------------------------------------------------
# Negotiated parameters


@dataclass(frozen=True)
class SummaryParams:
    """Session budget.  None of these depend on the data or its differences.

    ``position_bits``       hash-position width (64 by default; smaller values
                            make real bucket collisions easy to exercise).
    ``max_gen``             deepest hash split allowed.
    ``leaf_limit``          prefer splitting while either side has more rows.
    ``cord_max_bytes``      largest cord accepted; larger triggers refinement.
    ``rows_per_chunk``      page size for detail and fallback rows.
    ``max_nodes_per_query`` batch size for summary probes.
    ``max_key_splits``      safety limit on key-interval bisection.
    """

    position_bits: int = DEFAULT_POSITION_BITS
    max_gen: int | None = None
    leaf_limit: int = 64
    cord_max_bytes: int = 1 << 16
    rows_per_chunk: int = 4096
    max_nodes_per_query: int = 512
    max_key_splits: int = 96

    def __post_init__(self) -> None:
        if not 1 <= self.position_bits <= POSITION_BITS_MAX:
            raise ValueError("position_bits out of range")
        if self.max_gen is None:
            object.__setattr__(self, "max_gen", self.position_bits)
        if not 0 <= self.max_gen <= self.position_bits:
            raise ValueError("max_gen must be within 0..position_bits")
        if self.leaf_limit < 1:
            raise ValueError("leaf_limit must be positive")
        if self.cord_max_bytes < CORD_HEADER.size:
            raise ValueError("cord_max_bytes must fit at least a cord header")
        if self.rows_per_chunk < 1:
            raise ValueError("rows_per_chunk must be positive")
        if self.max_nodes_per_query < 1:
            raise ValueError("max_nodes_per_query must be positive")
        if self.max_key_splits < 1:
            raise ValueError("max_key_splits must be positive")

    def to_wire(self) -> dict[str, int]:
        return {
            "position_bits": self.position_bits,
            "max_gen": int(self.max_gen),
            "leaf_limit": self.leaf_limit,
            "cord_max_bytes": self.cord_max_bytes,
            "rows_per_chunk": self.rows_per_chunk,
            "max_nodes_per_query": self.max_nodes_per_query,
            "max_key_splits": self.max_key_splits,
        }

    @staticmethod
    def from_wire(obj: dict) -> "SummaryParams":
        try:
            return SummaryParams(
                position_bits=int(obj["position_bits"]),
                max_gen=int(obj["max_gen"]),
                leaf_limit=int(obj["leaf_limit"]),
                cord_max_bytes=int(obj["cord_max_bytes"]),
                rows_per_chunk=int(obj["rows_per_chunk"]),
                max_nodes_per_query=int(obj["max_nodes_per_query"]),
                max_key_splits=int(obj["max_key_splits"]),
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise ValueError(f"invalid summary parameters: {exc}") from exc
