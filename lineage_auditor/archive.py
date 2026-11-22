"""History archive for completed audit packages: content-addressed evidence store.

Why this exists
---------------
Every weekly release used to be kept as another full ``copy-package`` copy, so a
release that changed only a handful of records still cost a whole new copy of
every evidence table. The archive stores the five core evidence tables
(``occ``, ``edges``, ``rejects``, ``participation``, ``node_stats``) as
*content-addressed* objects shared across every archived version:

* Each evidence row is canonically encoded (typed, length-prefixed, independent
  of DuckDB page layout) and named by the SHA-256 of that encoding. Row order
  and physical storage therefore never affect identity.
* Rows are kept in immutable **bucket chunks**: for each table, row hashes
  are split into content-defined buckets whose count grows with the table
  (small tables keep coarse 256-bucket chunks, large tables up to 65536 fine
  ones so a typical chunk holds about four rows). A bucket chunk contains one
  copy of each *distinct* row that belongs to the bucket, plus a per-hash
  count; the counts make repeated rows (two edges with every field equal, two
  scan rows carrying the same values) survive losslessly as a multiset.
* A chunk file is named after the hash of its own payload, so a bucket whose
  rows did not change maps to the same file name and is never written again.
* A version is therefore only: its own ``flow.json`` + ``manifest.json`` and a
  small manifest naming the chunks it uses (hashes and row counts, never row
  payloads). Re-archiving an unchanged version writes zero new evidence bytes;
  archiving a version that changed few rows writes only the few affected
  buckets. Different version names may point at exactly the same evidence.

Visibility rule: all chunk writes are content-addressed and immutable, so an
interrupted archive leaves at worst orphan temp files. A version exists only
when its ``versions/<name>/`` directory is atomically renamed into place; until
that rename the version is not visible, and after it every chunk it names is
already on disk and it restores completely. Old versions are never rewritten.

The archive never compresses or rewraps a whole ``evidence.duckdb``: the
deduplicated unit is the evidence row, and the stored payload is plain,
length-prefixed canonical bytes so the byte accounting stays auditable.
"""
from __future__ import annotations

import hashlib
import json
import os
import shutil
import tempfile
from typing import Any, Dict, List, Optional, Sequence, Tuple

from .package import (
    AuditPackage,
    EVIDENCE_DDL,
    EVIDENCE_INDEXES,
    PackageError,
)

ARCHIVE_FORMAT = 1
ARCHIVE_MARKER = "archive.json"
CHUNKS_DIR = "chunks"
VERSIONS_DIR = "versions"
TMP_SUFFIX = ".tmp"

# The five core evidence tables, in restore order. Each entry carries the
# columns read out of (and inserted back into) a package database, in the exact
# physical column order of package.EVIDENCE_DDL.
EVIDENCE_TABLES: Dict[str, Tuple[str, ...]] = {
    "occ": ("node_id", "occ_id", "data_json", "padded"),
    "edges": ("from_node", "from_occ", "to_node", "to_occ", "role"),
    "rejects": ("reject_id", "node_id", "input_node", "input_occ",
                "reason_code", "detail_json"),
    "participation": ("node_id", "group_occ", "member_node", "member_occ",
                      "agg_name", "in_count", "in_sum", "contribution"),
    "node_stats": ("node_id", "op", "input_occ", "output_occ",
                   "rejected", "expanded", "merged"),
}
# Rows are grouped into content-addressed bucket chunks. The bucket count is
# chosen per archived table so that a typical chunk holds TARGET_ROWS_PER_CHUNK
# rows: small tables keep few, coarse chunks; large tables get fine-grained
# chunks, which is what lets a small evidence change in a large package touch
# only a small fraction of the stored payload (the touched fraction tracks
# changed rows / total rows, since row hashes distribute uniformly). The chosen
# count is recorded in every chunk header and in the version document, so
# restore never has to guess.
MIN_BUCKETS = 256
MAX_BUCKETS = 1 << 16
TARGET_ROWS_PER_CHUNK = 4

# --------------------------------------------------------------------------- #
# typed canonical row encoding
# --------------------------------------------------------------------------- #
# Evidence columns are only ever BIGINT, VARCHAR or BOOLEAN (see EVIDENCE_DDL),
# so a small explicit type-tagged encoding is enough. Every scalar is
# length-prefixed; strings go in as UTF-8; floats cannot appear.
_TAG_NULL = 0
_TAG_INT = 1
_TAG_TEXT = 2
_TAG_BOOL = 3


def _encode_scalar(value: Any) -> bytes:
    if value is None:
        return bytes([_TAG_NULL])
    if isinstance(value, bool):
        return bytes([_TAG_BOOL, 1 if value else 0])
    if isinstance(value, int):
        return bytes([_TAG_INT]) + str(value).encode("ascii") + b"\x00"
    if isinstance(value, str):
        raw = value.encode("utf-8")
        return bytes([_TAG_TEXT]) + len(raw).to_bytes(8, "big") + raw
    raise ArchiveError(
        f"evidence value {value!r} has unsupported type {type(value).__name__}; "
        f"the archive only stores the typed evidence columns (BIGINT/VARCHAR/BOOLEAN)"
    )


def encode_row(table: str, row: Sequence[Any]) -> bytes:
    """Canonical bytes for one evidence row.

    The table name is part of the encoding, so identical tuples in two tables
    never alias. Column order is fixed by EVIDENCE_TABLES, not by whatever
    physical order a particular DuckDB file happens to expose.
    """
    parts = [table.encode("ascii"), b"\x00"]
    for value in row:
        parts.append(_encode_scalar(value))
    return b"".join(parts)


def row_hash(table: str, row: Sequence[Any]) -> str:
    return hashlib.sha256(encode_row(table, row)).hexdigest()


# --------------------------------------------------------------------------- #
# bucket chunk file format
# --------------------------------------------------------------------------- #
# A chunk stores the DISTINCT rows of one (table, bucket). Layout (plain bytes,
# no compression, so payload size equals real evidence bytes):
#
#   magic "LAEV1\n"
#   u8    table name length, then table name bytes
#   u32   bucket count of the table's current scheme
#   u32   bucket index
#   u32   entry count
#   repeated entries:
#     u8   32 (hash length), 32-byte row hash
#     u64  multiset count (how many times this row occurs in the version)
#     u64  payload length, canonical row encoding bytes
#     ...  canonical row encoding
def _u32(n: int) -> bytes:
    return n.to_bytes(4, "big")


def _chunk_bytes(table: str, bucket_count: int, bucket: int,
                 entries: List[Tuple[str, int, bytes]]) -> bytes:
    out = [b"LAEV1\n"]
    name = table.encode("ascii")
    out.append(bytes([len(name)]) + name)
    out.append(_u32(bucket_count))
    out.append(_u32(bucket))
    out.append(_u32(len(entries)))
    for rhash, count, payload in entries:
        out.append(bytes([32]) + bytes.fromhex(rhash))
        out.append(count.to_bytes(8, "big"))
        out.append(len(payload).to_bytes(8, "big"))
        out.append(payload)
    return b"".join(out)


def _read_chunk(path: str) -> Dict[str, Any]:
    with open(path, "rb") as f:
        blob = f.read()
    pos = 0

    def take(n: int) -> bytes:
        nonlocal pos
        if pos + n > len(blob):
            raise ArchiveError(f"corrupt chunk {path}: truncated")
        part = blob[pos:pos + n]
        pos += n
        return part

    if take(6) != b"LAEV1\n":
        raise ArchiveError(f"corrupt chunk {path}: bad magic")
    n = take(1)[0]
    table = take(n).decode("ascii")
    bucket_count = int.from_bytes(take(4), "big")
    bucket = int.from_bytes(take(4), "big")
    count = int.from_bytes(take(4), "big")
    entries: List[Tuple[str, int, bytes]] = []
    for _ in range(count):
        hlen = take(1)[0]
        rhash = take(hlen).hex()
        mult = int.from_bytes(take(8), "big")
        plen = int.from_bytes(take(8), "big")
        payload = take(plen)
        if hashlib.sha256(payload).hexdigest() != rhash:
            raise ArchiveError(f"corrupt chunk {path}: row hash mismatch")
        entries.append((rhash, mult, payload))
    if pos != len(blob):
        raise ArchiveError(f"corrupt chunk {path}: trailing bytes")
    return {"table": table, "bucket_count": bucket_count,
            "bucket": bucket, "entries": entries}


def _bucket_scheme(row_count: int) -> int:
    """Choose a power-of-two bucket count so chunks hold ~TARGET rows each."""
    count = MIN_BUCKETS
    while row_count // count > TARGET_ROWS_PER_CHUNK and count < MAX_BUCKETS:
        count *= 2
    return count


def _bucket_of(rhash: str, bucket_count: int) -> int:
    # Low bits of the hash integer: uniform and valid for any power of two,
    # so bucket schemes can double smoothly instead of jumping 16x.
    return int(rhash, 16) & (bucket_count - 1)


def _chunk_name(blob: bytes) -> str:
    return hashlib.sha256(blob).hexdigest()


# --------------------------------------------------------------------------- #
# errors
# --------------------------------------------------------------------------- #
class ArchiveError(RuntimeError):
    """Refusal or integrity failure of an archive/restore operation."""


# --------------------------------------------------------------------------- #
# archive object
# --------------------------------------------------------------------------- #
class HistoryArchive:
    def __init__(self, library_dir: str):
        self.root = os.path.abspath(library_dir)
        self.chunks_dir = os.path.join(self.root, CHUNKS_DIR)
        self.versions_dir = os.path.join(self.root, VERSIONS_DIR)
        self.marker_path = os.path.join(self.root, ARCHIVE_MARKER)

    # -- lifecycle ------------------------------------------------------- #
    def init_library(self) -> None:
        os.makedirs(self.chunks_dir, exist_ok=True)
        os.makedirs(self.versions_dir, exist_ok=True)
        if not os.path.exists(self.marker_path):
            self._atomic_json(self.marker_path, {
                "format": ARCHIVE_FORMAT,
                "kind": "lineage-auditor-evidence-history",
                "tables": list(EVIDENCE_TABLES),
                "min_buckets": MIN_BUCKETS,
                "max_buckets": MAX_BUCKETS,
                "target_rows_per_chunk": TARGET_ROWS_PER_CHUNK,
            })

    def _require_library(self) -> None:
        if not os.path.isdir(self.root) or not os.path.exists(self.marker_path):
            raise ArchiveError(
                f"{self.root} is not an evidence history library "
                f"(missing {ARCHIVE_MARKER}); create one with `archive`"
            )
        with open(self.marker_path, encoding="utf-8") as f:
            marker = json.load(f)
        if marker.get("format") != ARCHIVE_FORMAT:
            raise ArchiveError(
                f"library format {marker.get('format')!r} unsupported "
                f"(this auditor supports {ARCHIVE_FORMAT})"
            )

    def list_versions(self) -> List[str]:
        self._require_library()
        if not os.path.isdir(self.versions_dir):
            return []
        return sorted(
            name for name in os.listdir(self.versions_dir)
            if os.path.isfile(os.path.join(self.versions_dir, name, "version.json"))
        )

    # -- source package validation --------------------------------------- #
    @staticmethod
    def _read_source(package_path: str) -> AuditPackage:
        pkg = AuditPackage(package_path)
        try:
            pkg.require_completed()  # refuses running/failed/cancelled
        except PackageError as exc:
            raise ArchiveError(str(exc)) from exc
        if not os.path.exists(pkg.flow_path):
            raise ArchiveError(
                f"package {pkg.path} is missing flow.json; cannot archive"
            )
        con = pkg.connect(read_only=True)
        try:
            present = {
                r[0] for r in con.execute(
                    "SELECT table_name FROM information_schema.tables "
                    "WHERE table_schema='main'"
                ).fetchall()
            }
            missing = [t for t in EVIDENCE_TABLES if t not in present]
            if missing:
                raise ArchiveError(
                    f"package {pkg.path} is missing required evidence table(s) "
                    f"{missing}; only packages with all five core evidence "
                    f"tables can be archived"
                )
            for table, cols in EVIDENCE_TABLES.items():
                actual = tuple(
                    r[0] for r in con.execute(
                        "SELECT column_name FROM information_schema.columns "
                        "WHERE table_name=? ORDER BY ordinal_position",
                        [table],
                    ).fetchall()
                )
                if actual != cols:
                    raise ArchiveError(
                        f"package {pkg.path} table {table!r} has columns {actual}, "
                        f"expected {cols}; refusing to archive an incompatible "
                        f"evidence layout"
                    )
        finally:
            con.close()
        return pkg

    # -- archive --------------------------------------------------------- #
    def archive(self, package_path: str, version: str) -> Dict[str, Any]:
        self._validate_version_name(version)
        self.init_library()
        pkg = self._read_source(package_path)

        with open(pkg.flow_path, "rb") as f:
            flow_bytes = f.read()
        with open(pkg.manifest_path, "rb") as f:
            manifest_bytes = f.read()
        flow_hash = hashlib.sha256(flow_bytes).hexdigest()
        manifest_hash = hashlib.sha256(manifest_bytes).hexdigest()

        # Build the full row-multiset fingerprint and collect bucket chunks.
        # Everything is computed before any version directory is touched, so a
        # failure leaves no partial version behind.
        con = pkg.connect(read_only=True)
        try:
            version_tables: Dict[str, Dict[str, Any]] = {}
            chunks_to_write: Dict[str, bytes] = {}
            row_total = 0
            distinct_total = 0
            for table, cols in EVIDENCE_TABLES.items():
                rows = con.execute(
                    f"SELECT {', '.join(cols)} FROM {table}"
                ).fetchall()
                row_total += len(rows)
                bucket_count = _bucket_scheme(len(rows))
                buckets: Dict[int, Dict[str, Tuple[int, bytes]]] = {}
                for row in rows:
                    payload = encode_row(table, row)
                    rhash = hashlib.sha256(payload).hexdigest()
                    b = _bucket_of(rhash, bucket_count)
                    entry = buckets.get(b, {}).get(rhash)
                    if entry is None:
                        buckets.setdefault(b, {})[rhash] = (1, payload)
                    else:
                        buckets[b][rhash] = (entry[0] + 1, entry[1])
                table_entry: Dict[str, Any] = {
                    "row_count": len(rows),
                    "bucket_count": bucket_count,
                    "chunks": [],
                }
                for b in sorted(buckets):
                    entries = [
                        (rhash, count, payload)
                        for rhash, (count, payload) in sorted(buckets[b].items())
                    ]
                    distinct_total += len(entries)
                    blob = _chunk_bytes(table, bucket_count, b, entries)
                    cname = _chunk_name(blob)
                    chunks_to_write[cname] = blob
                    table_entry["chunks"].append({
                        "bucket": b,
                        "chunk": cname,
                        "row_count": sum(c for _, c, _ in entries),
                        "distinct_rows": len(entries),
                    })
                version_tables[table] = table_entry
        finally:
            con.close()

        evidence_fingerprint = hashlib.sha256()
        for table in EVIDENCE_TABLES:
            for ch in version_tables[table]["chunks"]:
                evidence_fingerprint.update(table.encode("ascii"))
                evidence_fingerprint.update(bytes.fromhex(ch["chunk"]))
                evidence_fingerprint.update(str(ch["row_count"]).encode("ascii"))
        evidence_hash = evidence_fingerprint.hexdigest()

        version_doc = {
            "format": ARCHIVE_FORMAT,
            "version": version,
            "flow_sha256": flow_hash,
            "manifest_sha256": manifest_hash,
            "evidence_sha256": evidence_hash,
            "row_count": row_total,
            "distinct_row_count": distinct_total,
            "tables": version_tables,
        }

        existing = self._version_doc(version)
        if existing is not None:
            self._assert_same(version, existing, version_doc, flow_bytes,
                              manifest_bytes)
            # Idempotent retry: evidence is content-addressed, so nothing is
            # written again. Re-verify the chunks it names still verify.
            self._verify_version_chunks(existing)
            return {
                "status": "already_archived",
                "library": self.root,
                "version": version,
                "evidence_sha256": evidence_hash,
                "note": "identical re-archive; no evidence bytes were written",
            }

        bytes_before = self._directory_bytes(self.chunks_dir)
        written = self._write_new_chunks(chunks_to_write)
        bytes_after = self._directory_bytes(self.chunks_dir)
        payload_added = bytes_after - bytes_before

        # Test hook (hard-kill simulation for the interruption drill): the
        # archive is still invisible at this point because the version directory
        # has not been published. LINEAGE_AUDITOR_ARCHIVE_KILL=after_chunks
        # kills the process without running cleanup, like SIGKILL.
        if os.environ.get("LINEAGE_AUDITOR_ARCHIVE_KILL") == "after_chunks":
            os._exit(2)

        self._publish_version(version, version_doc, flow_bytes, manifest_bytes)
        if os.environ.get("LINEAGE_AUDITOR_ARCHIVE_KILL") == "after_publish":
            os._exit(2)
        return {
            "status": "archived",
            "library": self.root,
            "version": version,
            "evidence_sha256": evidence_hash,
            "row_count": row_total,
            "distinct_row_count": distinct_total,
            "chunks_written": written,
            "evidence_payload_bytes_added": payload_added,
        }

    def _write_new_chunks(self, chunks: Dict[str, bytes]) -> int:
        """Write immutable chunks that are not already present.

        Chunk files are content-addressed and never modified in place, so they
        are written in one batch and the chunks directory is fsynced a single
        time afterwards; per-file fsync of thousands of tiny files would make
        archiving I/O-bound without adding any safety (the version directory
        rename below remains the single visibility point).
        """
        written = 0
        tmp_names = []
        try:
            for cname, blob in chunks.items():
                final = os.path.join(self.chunks_dir, cname)
                if os.path.exists(final):
                    # A chunk is content-addressed: same name means same bytes.
                    continue
                fd, tmp = tempfile.mkstemp(prefix=cname[:8], suffix=TMP_SUFFIX,
                                           dir=self.chunks_dir)
                with os.fdopen(fd, "wb") as f:
                    f.write(blob)
                tmp_names.append(tmp)
                os.replace(tmp, final)
                written += 1
            self._fsync_dir(self.chunks_dir)
        except BaseException:
            for tmp in tmp_names:
                if os.path.exists(tmp):
                    os.unlink(tmp)
            raise
        return written

    @staticmethod
    def _fsync_dir(path: str) -> None:
        try:
            fd = os.open(path, os.O_RDONLY)
            try:
                os.fsync(fd)
            finally:
                os.close(fd)
        except OSError:
            # Filesystems without directory fsync (some containers/tmpfs):
            # atomic rename still gives the all-or-nothing visibility rule.
            pass

    def _publish_version(self, version: str, version_doc: Dict[str, Any],
                         flow_bytes: bytes, manifest_bytes: bytes) -> None:
        """Stage the whole version directory, then make it visible atomically."""
        final_dir = os.path.join(self.versions_dir, version)
        parent_tmp = os.path.join(
            self.versions_dir, f".staging-{version}-{os.getpid()}"
        )
        if os.path.exists(parent_tmp):
            shutil.rmtree(parent_tmp)
        os.makedirs(parent_tmp)
        try:
            self._atomic_bytes(os.path.join(parent_tmp, "flow.json"), flow_bytes)
            self._atomic_bytes(os.path.join(parent_tmp, "manifest.json"),
                               manifest_bytes)
            self._atomic_json(os.path.join(parent_tmp, "version.json"), version_doc)
            # Single rename is the visibility point: every named chunk already
            # exists and is immutable, so a visible version always restores.
            os.rename(parent_tmp, final_dir)
            self._fsync_dir(self.versions_dir)
        except BaseException:
            shutil.rmtree(parent_tmp, ignore_errors=True)
            raise

    def _assert_same(self, version: str, existing: Dict[str, Any],
                     new_doc: Dict[str, Any], flow_bytes: bytes,
                     manifest_bytes: bytes) -> None:
        diffs = []
        for key in ("flow_sha256", "manifest_sha256", "evidence_sha256"):
            if existing.get(key) != new_doc.get(key):
                diffs.append(key)
        with open(os.path.join(self.versions_dir, version, "flow.json"),
                  "rb") as f:
            if hashlib.sha256(f.read()).hexdigest() != new_doc["flow_sha256"]:
                diffs.append("stored_flow_bytes")
        with open(os.path.join(self.versions_dir, version, "manifest.json"),
                  "rb") as f:
            if hashlib.sha256(f.read()).hexdigest() != new_doc["manifest_sha256"]:
                diffs.append("stored_manifest_bytes")
        if diffs:
            raise ArchiveError(
                f"version {version!r} already exists in {self.root} with "
                f"different content ({', '.join(sorted(set(diffs)))}); "
                f"archived versions are immutable, choose another version name"
            )

    def _verify_version_chunks(self, version_doc: Dict[str, Any]) -> None:
        for table in EVIDENCE_TABLES:
            for ch in version_doc["tables"][table]["chunks"]:
                path = os.path.join(self.chunks_dir, ch["chunk"])
                if not os.path.exists(path):
                    raise ArchiveError(
                        f"library is missing chunk {ch['chunk']} named by an "
                        f"archived version; the library was modified externally"
                    )
                parsed = _read_chunk(path)
                if parsed["table"] != table or parsed["bucket"] != ch["bucket"]:
                    raise ArchiveError(
                        f"chunk {ch['chunk']} contents do not match its manifest entry"
                    )

    # -- restore --------------------------------------------------------- #
    def restore(self, version: str, dest: str) -> Dict[str, Any]:
        self._validate_version_name(version)
        self._require_library()
        version_doc = self._version_doc(version)
        if version_doc is None:
            raise ArchiveError(
                f"version {version!r} is not archived in {self.root}; "
                f"known versions: {self.list_versions()}"
            )
        dest = os.path.abspath(dest)
        if os.path.exists(dest):
            raise ArchiveError(
                f"restore destination {dest} already exists; restore never "
                f"overwrites a directory"
            )

        version_dir = os.path.join(self.versions_dir, version)
        with open(os.path.join(version_dir, "flow.json"), "rb") as f:
            flow_bytes = f.read()
        with open(os.path.join(version_dir, "manifest.json"), "rb") as f:
            manifest_bytes = f.read()
        if hashlib.sha256(flow_bytes).hexdigest() != version_doc["flow_sha256"]:
            raise ArchiveError("stored flow.json does not match its manifest hash")
        if hashlib.sha256(manifest_bytes).hexdigest() != \
                version_doc["manifest_sha256"]:
            raise ArchiveError("stored manifest.json does not match its hash")

        # Stage everything under a sibling temp dir; only the final rename makes
        # a package appear, and any failure is removed (never left as completed).
        parent = os.path.dirname(dest)
        os.makedirs(parent, exist_ok=True)
        staging = tempfile.mkdtemp(
            prefix=f".restore-{os.path.basename(dest)}-", dir=parent
        )
        completed = False
        try:
            with open(os.path.join(staging, "flow.json"), "wb") as f:
                f.write(flow_bytes)
            with open(os.path.join(staging, "manifest.json"), "wb") as f:
                f.write(manifest_bytes)

            from .package import duckdb as _duckdb
            if _duckdb is None:
                raise ArchiveError("duckdb is not available")
            db_path = os.path.join(staging, "evidence.duckdb")
            con = _duckdb.connect(db_path)
            try:
                con.execute(EVIDENCE_DDL)
                # One transaction for the whole rebuild, with multi-row VALUES
                # batches (same bulk-load pattern as the executor): row-by-row
                # executemany would make restore I/O-bound.
                con.execute("BEGIN TRANSACTION")
                restored_rows = 0
                for table, cols in EVIDENCE_TABLES.items():
                    expected = version_doc["tables"][table]["row_count"]
                    got = 0
                    batch: List[Sequence[Any]] = []

                    def flush() -> None:
                        if not batch:
                            return
                        body = ",".join(
                            "(" + ",".join(_sql_literal(v) for v in row) + ")"
                            for row in batch
                        )
                        con.execute(
                            f"INSERT INTO {table} ({', '.join(cols)}) "
                            f"VALUES {body}"
                        )
                        batch.clear()

                    for ch in version_doc["tables"][table]["chunks"]:
                        parsed = _read_chunk(
                            os.path.join(self.chunks_dir, ch["chunk"])
                        )
                        if parsed["table"] != table or \
                                parsed["bucket"] != ch["bucket"] or \
                                parsed["bucket_count"] != \
                                version_doc["tables"][table]["bucket_count"]:
                            raise ArchiveError(
                                f"chunk {ch['chunk']} does not match its "
                                f"manifest entry for table {table}"
                            )
                        for rhash, count, payload in parsed["entries"]:
                            if hashlib.sha256(payload).hexdigest() != rhash:
                                raise ArchiveError("chunk row hash mismatch")
                            row = _decode_row(table, payload)
                            # multiset: duplicates are re-materialized count times
                            for _ in range(count):
                                batch.append(row)
                                got += 1
                            if len(batch) >= 5000:
                                flush()
                    flush()
                    if got != expected:
                        raise ArchiveError(
                            f"restore integrity failure for {table}: "
                            f"rebuilt {got} rows, manifest expects {expected}"
                        )
                    restored_rows += got
                con.execute("COMMIT")
                con.execute(EVIDENCE_INDEXES)
                con.execute("CHECKPOINT")
            except BaseException:
                try:
                    con.execute("ROLLBACK")
                except Exception:
                    pass
                raise
            finally:
                con.close()
            os.rename(staging, dest)
            completed = True
        finally:
            if not completed:
                shutil.rmtree(staging, ignore_errors=True)

        return {
            "status": "restored",
            "library": self.root,
            "version": version,
            "destination": dest,
            "row_count": version_doc["row_count"],
            "preview_support": "absent: restored packages serve trace-source/"
                               "trace-output/node-balance/participation but are "
                               "not claimable for preview",
        }

    # -- helpers --------------------------------------------------------- #
    def _version_doc(self, version: str) -> Optional[Dict[str, Any]]:
        path = os.path.join(self.versions_dir, version, "version.json")
        if not os.path.exists(path):
            return None
        with open(path, encoding="utf-8") as f:
            return json.load(f)

    @staticmethod
    def _validate_version_name(version: str) -> None:
        if not isinstance(version, str) or not version:
            raise ArchiveError("version name must be a non-empty string")
        if version in (".", "..") or "/" in version or os.sep in version \
                or version.startswith("."):
            raise ArchiveError(
                f"version name {version!r} must be a plain path component "
                f"(no '/', no leading dot)"
            )

    @staticmethod
    def _directory_bytes(path: str) -> int:
        if not os.path.isdir(path):
            return 0
        total = 0
        for name in os.listdir(path):
            full = os.path.join(path, name)
            if os.path.isfile(full) and not name.endswith(TMP_SUFFIX):
                total += os.path.getsize(full)
        return total

    @staticmethod
    def _atomic_bytes(path: str, blob: bytes) -> None:
        tmp = path + TMP_SUFFIX
        with open(tmp, "wb") as f:
            f.write(blob)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)

    @staticmethod
    def _atomic_json(path: str, doc: Dict[str, Any]) -> None:
        HistoryArchive._atomic_bytes(
            path, json.dumps(doc, ensure_ascii=False, indent=2).encode("utf-8")
        )


# --------------------------------------------------------------------------- #
# row decoding (mirror of encode_row; used only on restore)
# --------------------------------------------------------------------------- #
def _sql_literal(value: Any) -> str:
    """Inline a restored scalar into a multi-row VALUES statement.

    Inputs come from hash-verified chunk payloads that the archive itself
    produced from the typed evidence columns, so only BIGINT/VARCHAR/BOOLEAN
    values and NULL can appear.
    """
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, int):
        return str(value)
    return "'" + value.replace("'", "''") + "'"


def _decode_row(table: str, payload: bytes) -> Tuple[Any, ...]:
    pos = 0

    def take(n: int) -> bytes:
        nonlocal pos
        if pos + n > len(payload):
            raise ArchiveError("truncated row payload while restoring")
        part = payload[pos:pos + n]
        pos += n
        return part

    name = payload.index(0)
    if payload[:name].decode("ascii") != table:
        raise ArchiveError("row payload belongs to another table")
    pos = name + 1
    out: List[Any] = []
    while pos < len(payload):
        tag = take(1)[0]
        if tag == _TAG_NULL:
            out.append(None)
        elif tag == _TAG_BOOL:
            out.append(bool(take(1)[0]))
        elif tag == _TAG_INT:
            end = payload.index(0, pos)
            out.append(int(payload[pos:end].decode("ascii")))
            pos = end + 1
        elif tag == _TAG_TEXT:
            n = int.from_bytes(take(8), "big")
            out.append(take(n).decode("utf-8"))
        else:
            raise ArchiveError(f"unknown type tag {tag} in row payload")
    if pos != len(payload):
        raise ArchiveError("trailing bytes in row payload")
    return tuple(out)
