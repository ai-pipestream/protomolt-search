#!/bin/sh
# Sync CourtListener bulk data from the Free Law Project's PUBLIC S3
# bucket into the rustfs object store. Anonymous read upstream
# (--no-sign-request), credentialed write to rustfs. Data flow:
#
#   s3://com-courtlistener-storage/bulk-data/$BULK_FILE  (public, quarterly)
#     -> local scratch
#     -> s3://$S3_BUCKET/bulk-data/$BULK_FILE            (rustfs)
#
# The downstream pipeline pulls the file back OUT of rustfs, so the
# object store is the single source of truth for the corpus.
set -eu

: "${BULK_FILE:=opinions-2024-12-31.csv.bz2}"
: "${CL_BUCKET:=com-courtlistener-storage}"
: "${S3_BUCKET:=courtlistener}"
: "${S3_ENDPOINT:?set S3_ENDPOINT}"
: "${SCRATCH:=/corpus}"
: "${AWS_ACCESS_KEY_ID:?set AWS_ACCESS_KEY_ID}"
: "${AWS_SECRET_ACCESS_KEY:?set AWS_SECRET_ACCESS_KEY}"
export AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY

echo "== ensure bucket s3://$S3_BUCKET on $S3_ENDPOINT"
aws --endpoint-url "$S3_ENDPOINT" s3 mb "s3://$S3_BUCKET" 2>/dev/null || true

echo "== download s3://$CL_BUCKET/bulk-data/$BULK_FILE (anonymous, $(date))"
if [ -f "$SCRATCH/$BULK_FILE" ]; then
  echo "already have $SCRATCH/$BULK_FILE; skipping download"
else
  aws s3 cp --no-sign-request --only-show-errors \
    "s3://$CL_BUCKET/bulk-data/$BULK_FILE" "$SCRATCH/$BULK_FILE"
fi

echo "== upload to s3://$S3_BUCKET/bulk-data/$BULK_FILE ($(date))"
if aws --endpoint-url "$S3_ENDPOINT" s3api head-object \
    --bucket "$S3_BUCKET" --key "bulk-data/$BULK_FILE" >/dev/null 2>&1; then
  echo "already in s3://$S3_BUCKET/bulk-data/$BULK_FILE; skipping upload"
else
  aws --endpoint-url "$S3_ENDPOINT" s3 cp --only-show-errors \
    "$SCRATCH/$BULK_FILE" "s3://$S3_BUCKET/bulk-data/$BULK_FILE"
fi

echo "SEED DONE"
