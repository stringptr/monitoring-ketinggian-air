#!/bin/sh

# Idempotently ensure the configured Garage buckets exist and that the shared
# access key has read/write/owner permissions on each of them.

# Driven by the Garage administration API (v2). Requires:
#   GARAGE_ADMIN_TOKEN        admin API bearer token
#   GARAGE_DEFAULT_ACCESS_KEY access key id that should be granted on the buckets
#   GARAGE_BUCKETS            space-separated list of bucket names

set -eu

API="http://garage:3903"
AUTH="Authorization: Bearer ${GARAGE_ADMIN_TOKEN:?GARAGE_ADMIN_TOKEN is required}"
KEY="${GARAGE_DEFAULT_ACCESS_KEY:?GARAGE_DEFAULT_ACCESS_KEY is required}"
BUCKETS="${GARAGE_BUCKETS:-bronze silver gold}"

echo "garage-init: waiting for the Garage admin API at ${API} ..."
i=0
until curl -fsS -H "$AUTH" "$API/v2/ListBuckets" >/dev/null 2>&1; do
  i=$((i + 1))
  if [ "$i" -ge 60 ]; then
    echo "garage-init: admin API did not become ready in time" >&2
    exit 1
  fi
  sleep 2
done

for bucket in $BUCKETS; do
  echo "garage-init: ensuring bucket '${bucket}'"

  # CreateBucket returns an error if the bucket already exists; ignore that and
  # fall back to looking the bucket up so the step stays idempotent.
  create_response=$(curl -sS -X POST "$API/v2/CreateBucket" \
    -H "$AUTH" -H "Content-Type: application/json" \
    -d "{\"globalAlias\":\"${bucket}\"}" 2>/dev/null || true)

  bucket_id=$(printf '%s' "$create_response" | jq -r '.id // empty' 2>/dev/null || true)

  if [ -z "$bucket_id" ]; then
    bucket_id=$(curl -sS -G "$API/v2/GetBucketInfo" \
      -H "$AUTH" --data-urlencode "globalAlias=${bucket}" |
      jq -r '.id // empty')
  fi

  if [ -z "$bucket_id" ]; then
    echo "garage-init: could not determine bucket id for '${bucket}'" >&2
    exit 1
  fi

  curl -fsS -X POST "$API/v2/AllowBucketKey" \
    -H "$AUTH" -H "Content-Type: application/json" \
    -d "{\"bucketId\":\"${bucket_id}\",\"accessKeyId\":\"${KEY}\",\"permissions\":{\"read\":true,\"write\":true,\"owner\":true}}" \
    >/dev/null

  echo "garage-init: '${bucket}' ready (id=${bucket_id}); key ${KEY} granted read/write/owner"
done

echo "garage-init: all buckets ensured: ${BUCKETS}"
