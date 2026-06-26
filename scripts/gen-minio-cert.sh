#!/usr/bin/env bash
# Generate the self-signed TLS cert MinIO serves so SDC's AWS SDK (v2.30+) will
# accept streaming uploads. Without HTTPS the S3 destination fails with
# "Content hash must be present in the 'x-amz-content-sha256' header".
#
# SANs include 'minio' (the in-network hostname the SDC uses), plus qtp-minio,
# localhost and 127.0.0.1. Run from the repo root:  bash scripts/gen-minio-cert.sh
# Then trust the cert in the SDC JVM (see README "Trust the MinIO certificate").
set -euo pipefail

cd "$(dirname "$0")/.."
mkdir -p certs

MSYS_NO_PATHCONV=1 openssl req -x509 -newkey rsa:2048 -days 3650 -nodes \
  -keyout certs/private.key -out certs/public.crt \
  -subj "/CN=minio" \
  -addext "subjectAltName=DNS:minio,DNS:qtp-minio,DNS:localhost,IP:127.0.0.1" \
  -addext "basicConstraints=critical,CA:TRUE"

echo "Generated certs/public.crt + certs/private.key"
openssl x509 -in certs/public.crt -noout -ext subjectAltName
