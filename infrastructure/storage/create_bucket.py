"""Create the lakehouse bucket in the local S3-compatible store (RustFS).

Safe to run repeatedly: an existing bucket is left unchanged.
"""

import argparse
import os
import sys

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--endpoint", default=os.getenv("S3_ENDPOINT", "http://127.0.0.1:9000"))
    parser.add_argument("--bucket", default=os.getenv("WAREHOUSE_BUCKET", "warehouse"))
    args = parser.parse_args()

    s3 = boto3.client(
        "s3",
        endpoint_url=args.endpoint,
        aws_access_key_id=os.getenv("S3_ACCESS_KEY", "quakeflow"),
        aws_secret_access_key=os.getenv("S3_SECRET_KEY", "quakeflow-local-secret"),
        region_name="us-east-1",
        config=Config(s3={"addressing_style": "path"}),
    )
    try:
        s3.head_bucket(Bucket=args.bucket)
        print(f"exists   {args.bucket}")
        return 0
    except ClientError as exc:
        if exc.response["Error"]["Code"] not in ("404", "NoSuchBucket", "NotFound"):
            print(f"FAILED   {args.bucket}: {exc}", file=sys.stderr)
            return 1
    try:
        s3.create_bucket(Bucket=args.bucket)
    except ClientError as exc:
        print(f"FAILED   {args.bucket}: {exc}", file=sys.stderr)
        return 1
    print(f"created  {args.bucket}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
