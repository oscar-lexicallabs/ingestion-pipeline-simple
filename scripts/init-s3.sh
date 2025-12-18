#!/usr/bin/env bash
# set -e

bucket_name=test-bucket
awslocal s3 mb s3://$bucket_name || true

DIR=$(realpath ./test_bucket/org/usr/files)
for file in $DIR/* ; do
    awslocal s3 cp $file s3://$bucket_name/org/usr/files/$(basename -- $file) ;
done
