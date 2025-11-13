#!/bin/sh

MINIO_ENDPOINT=${MINIO_ENDPOINT:-http://minio:9000}
MINIO_ACCESS_KEY=${MINIO_ACCESS_KEY:-admin123}
MINIO_SECRET_KEY=${MINIO_SECRET_KEY:-admin123}
BUCKET_NAME=${BUCKET_NAME:-uber-data-analysis}


sleep 10
mc alias set local "${MINIO_ENDPOINT}" "${MINIO_ACCESS_KEY}" "${MINIO_SECRET_KEY}"
mc admin user add local uberanalysis uberanalysis || true
mc admin policy attach local readwrite -u uberanalysis || true
mc mb "local/${BUCKET_NAME}" || true
