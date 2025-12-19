#!/usr/bin/env bash

DIR=$(realpath ./test_bucket/org/usr/files)
bucket_name=test-bucket
for file in $DIR/*; do
    if [[ -f $file ]]; then
        awslocal s3 cp $file s3://$bucket_name/org/usr/files/$(basename -- $file)
    fi
done
