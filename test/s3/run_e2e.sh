#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
DUCKDB_BIN="${DUCKDB_BIN:-${ROOT_DIR}/build/reldebug/duckdb}"
MINIO_PORT="${DUCKDB_OBJFS_MINIO_PORT:-19000}"
S3_BUCKET="${DUCKDB_OBJFS_S3_BUCKET:-duckdb-objfs-e2e}"
S3_ROOT="${DUCKDB_OBJFS_S3_ROOT:-e2e}"
MINIO_IMAGE="${DUCKDB_OBJFS_MINIO_IMAGE:-quay.io/minio/minio}"
MC_IMAGE="${DUCKDB_OBJFS_MC_IMAGE:-quay.io/minio/mc}"
CONTAINER_NAME="duckdb-objfs-minio-$$"
SQL_FILE="$(mktemp)"

cleanup() {
	rm -f "${SQL_FILE}"
	docker rm -f "${CONTAINER_NAME}" >/dev/null 2>&1 || true
}
trap cleanup EXIT

if [[ ! -x "${DUCKDB_BIN}" ]]; then
	echo "DuckDB executable not found at ${DUCKDB_BIN}; run 'make reldebug' first" >&2
	exit 1
fi

docker run --detach --rm --name "${CONTAINER_NAME}" -p "${MINIO_PORT}:9000" \
	-e MINIO_ROOT_USER=minioadmin \
	-e MINIO_ROOT_PASSWORD=minioadmin \
	"${MINIO_IMAGE}" server /data >/dev/null

until curl --fail --silent "http://127.0.0.1:${MINIO_PORT}/minio/health/ready" >/dev/null; do
	sleep 0.2
done

docker run --rm --network "container:${CONTAINER_NAME}" \
	-e MC_HOST_local=http://minioadmin:minioadmin@127.0.0.1:9000 \
	"${MC_IMAGE}" \
	mb --ignore-existing "local/${S3_BUCKET}" >/dev/null

sed -e "s|__S3_ENDPOINT__|127.0.0.1:${MINIO_PORT}|g" \
	-e "s|__S3_BUCKET__|${S3_BUCKET}|g" \
	-e "s|__S3_ROOT__|${S3_ROOT}|g" \
	"${ROOT_DIR}/test/s3/duckdb_objfs_e2e.sql.in" >"${SQL_FILE}"

export AWS_ACCESS_KEY_ID=minioadmin
export AWS_SECRET_ACCESS_KEY=minioadmin
"${DUCKDB_BIN}" -bail -f "${SQL_FILE}"
