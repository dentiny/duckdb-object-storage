#!/usr/bin/env bash
# End-to-end test of the azure backend against live Microsoft OneLake.
#
# Requires ONELAKE_TOKEN (bearer token for https://storage.azure.com). The database lives under
# ${ONELAKE_WORKSPACE}/${ONELAKE_ROOT} (OneLake: container = workspace, root = <item>.Lakehouse/Files/...).
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
DUCKDB_BIN="${DUCKDB_BIN:-${ROOT_DIR}/build/release/duckdb}"
WORKSPACE="${ONELAKE_WORKSPACE:?set ONELAKE_WORKSPACE}"
ROOT="${ONELAKE_ROOT:?set ONELAKE_ROOT}"
: "${ONELAKE_TOKEN:?set ONELAKE_TOKEN}"
ROWS="${DUCKDB_OBJFS_E2E_ROWS:-1000000}"
WORK_DIR="$(mktemp -d)"
trap 'rm -rf "${WORK_DIR}"' EXIT

if [[ ! -x "${DUCKDB_BIN}" ]]; then
	echo "DuckDB executable not found at ${DUCKDB_BIN}" >&2
	exit 1
fi

# Every phase is a separate process, so each one initializes the filesystem from scratch.
preamble() {
	cat <<EOF
.timer on
INSTALL azure;
LOAD azure;
LOAD duckdb_object_storage;
CREATE OR REPLACE SECRET onelake (TYPE AZURE, PROVIDER ACCESS_TOKEN, ACCESS_TOKEN getenv('ONELAKE_TOKEN'));
SET duckdb_objfs_backend = 'azure';
SET duckdb_objfs_bucket = '${WORKSPACE}';
SET duckdb_objfs_root = '${ROOT}';
EOF
}

stats() {
	cat <<'EOF'
SELECT * FROM duckdb_objfs_io_stats();
SELECT * FROM duckdb_objfs_cache_stats();
EOF
}

run_phase() {
	local name="$1"
	local sql="${WORK_DIR}/${name}.sql"
	{
		preamble
		cat
		stats
	} >"${sql}"
	echo "=== phase: ${name}"
	local start=${SECONDS}
	"${DUCKDB_BIN}" -bail -f "${sql}"
	echo "=== phase ${name} took $((SECONDS - start))s"
}

run_phase write <<EOF
ATTACH 'duckdb_objfs://e2e.db' AS obj;
CREATE TABLE obj.items AS SELECT range AS i, md5(range::VARCHAR) AS payload FROM range(${ROWS});
CHECKPOINT obj;
DETACH obj;
EOF

run_phase read_only <<EOF
ATTACH 'duckdb_objfs://e2e.db' AS obj (READ_ONLY);
SELECT CASE WHEN count(*) = ${ROWS} AND sum(i) = ${ROWS}::HUGEINT * (${ROWS} - 1) / 2 THEN 'read_only ok'
            ELSE error('unexpected row count/sum after reopen') END
FROM obj.items;
SELECT payload FROM obj.items WHERE i = 4242;
DETACH obj;
EOF

# Crash consistency: commit rows to the WAL (no checkpoint), then kill -9 the process mid-query.
CRASH_SQL="${WORK_DIR}/crash.sql"
MARKER="${WORK_DIR}/committed.csv"
{
	preamble
	cat <<EOF
SET checkpoint_threshold = '10GB';
ATTACH 'duckdb_objfs://e2e.db' AS obj;
INSERT INTO obj.items SELECT range AS i, 'crash' FROM range(${ROWS}, ${ROWS} + 1000);
COPY (SELECT 1) TO '${MARKER}';
SELECT count(*) FROM range(1000000000000);
EOF
} >"${CRASH_SQL}"
echo "=== phase: crash"
"${DUCKDB_BIN}" -f "${CRASH_SQL}" &
CRASH_PID=$!
for _ in $(seq 1 600); do
	[[ -f "${MARKER}" ]] && break
	if ! kill -0 "${CRASH_PID}" 2>/dev/null; then
		echo "crash phase exited before committing" >&2
		exit 1
	fi
	sleep 0.5
done
[[ -f "${MARKER}" ]] || { echo "crash phase never committed" >&2; exit 1; }
kill -9 "${CRASH_PID}"
wait "${CRASH_PID}" 2>/dev/null || true
echo "=== killed duckdb after commit"

run_phase recover <<EOF
ATTACH 'duckdb_objfs://e2e.db' AS obj;
SELECT CASE WHEN count(*) = ${ROWS} + 1000 THEN 'wal replay ok'
            ELSE error('committed rows lost after kill -9: ' || count(*)) END
FROM obj.items;
CHECKPOINT obj;
DETACH obj;
ATTACH 'duckdb_objfs://e2e.db' AS obj (READ_ONLY);
SELECT CASE WHEN count(*) = ${ROWS} + 1000 THEN 'post-checkpoint ok' ELSE error('rows lost after checkpoint') END
FROM obj.items;
DETACH obj;
EOF

echo "=== OneLake objects under ${WORKSPACE}/${ROOT}"
curl -sf -H "Authorization: Bearer ${ONELAKE_TOKEN}" -H "x-ms-version: 2023-11-03" \
	"https://onelake.blob.fabric.microsoft.com/${WORKSPACE}?restype=container&comp=list&prefix=${ROOT}/" |
	python3 -c '
import sys, xml.etree.ElementTree as ET
blobs = [(b.findtext("Name"), int(b.findtext("Properties/Content-Length") or 0))
         for b in ET.parse(sys.stdin).getroot().iter("Blob")]
for name, size in blobs:
    print(f"{size:>12} {name}")
print(f"{len(blobs)} objects, {sum(s for _, s in blobs) / 1048576:.1f} MiB (first page)")'

echo "=== OneLake e2e passed"
