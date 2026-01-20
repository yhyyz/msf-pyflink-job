#!/usr/bin/env python3
"""
Setup Glue Database with LakeFormation permissions for MSF Iceberg jobs.
IAM Role is managed by flink_manager.py.
"""

import argparse
import logging

import boto3
from botocore.exceptions import ClientError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def create_glue_database(
    glue_client,
    lf_client,
    database_name: str,
    use_iam_only: bool,
    s3_bucket: str,
    account_id: str,
) -> None:
    location_uri = f"s3://{s3_bucket}/iceberg-warehouse/{database_name}.db/"

    try:
        glue_client.create_database(
            DatabaseInput={
                "Name": database_name,
                "Description": "Iceberg database for MSF jobs",
                "LocationUri": location_uri,
            }
        )
        logger.info(f"Created Glue Database: {database_name}")
    except ClientError as e:
        if e.response["Error"]["Code"] == "AlreadyExistsException":
            logger.info(f"Glue Database already exists: {database_name}")
        else:
            raise

    if use_iam_only:
        try:
            lf_client.grant_permissions(
                Principal={"DataLakePrincipalIdentifier": "IAM_ALLOWED_PRINCIPALS"},
                Resource={"Database": {"CatalogId": account_id, "Name": database_name}},
                Permissions=["ALL"],
            )
            logger.info("Granted IAM_ALLOWED_PRINCIPALS ALL permissions on database")
        except ClientError as e:
            if "already exists" in str(e).lower() or "InvalidInput" in str(e):
                logger.info("LakeFormation database permissions already configured")
            else:
                logger.warning(f"LakeFormation database grant failed: {e}")

        try:
            lf_client.grant_permissions(
                Principal={"DataLakePrincipalIdentifier": "IAM_ALLOWED_PRINCIPALS"},
                Resource={
                    "Table": {
                        "CatalogId": account_id,
                        "DatabaseName": database_name,
                        "TableWildcard": {},
                    }
                },
                Permissions=["ALL"],
            )
            logger.info("Granted IAM_ALLOWED_PRINCIPALS ALL permissions on all tables")
        except ClientError as e:
            if "already exists" in str(e).lower() or "InvalidInput" in str(e):
                logger.info("LakeFormation table permissions already configured")
            else:
                logger.warning(f"LakeFormation table grant failed: {e}")


def get_account_id(sts_client) -> str:
    return sts_client.get_caller_identity()["Account"]


def main():
    parser = argparse.ArgumentParser(
        description="Setup Glue Database with LakeFormation permissions",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python setup_database.py --region us-east-1 --s3-bucket pcd-ue1-01
  python setup_database.py --region us-east-1 --s3-bucket pcd-ue1-01 --database my_iceberg_db
  python setup_database.py --region us-east-1 --s3-bucket pcd-ue1-01 --no-iam-only
        """,
    )

    parser.add_argument("--region", required=True, help="AWS region (required)")
    parser.add_argument(
        "--database",
        default="test_iceberg_db",
        help="Glue database name (default: test_iceberg_db)",
    )
    parser.add_argument(
        "--s3-bucket", required=True, help="S3 bucket for Iceberg warehouse (required)"
    )
    parser.add_argument(
        "--no-iam-only",
        action="store_true",
        help="Disable 'Use only IAM' for new tables (default: enabled)",
    )

    args = parser.parse_args()
    use_iam_only = not args.no_iam_only

    logger.info("Glue Database Setup")
    logger.info(f"Region: {args.region}")
    logger.info(f"Database: {args.database}")
    logger.info(f"S3 Bucket: {args.s3_bucket}")
    logger.info(f"Use IAM Only: {use_iam_only}")

    session = boto3.Session(region_name=args.region)
    glue_client = session.client("glue")
    lf_client = session.client("lakeformation")
    sts_client = session.client("sts")

    account_id = get_account_id(sts_client)
    logger.info(f"AWS Account: {account_id}")

    logger.info("Creating Glue Database...")
    create_glue_database(
        glue_client,
        lf_client,
        args.database,
        use_iam_only,
        args.s3_bucket,
        account_id,
    )

    logger.info("Setup Complete")
    logger.info(f"Glue Database: {args.database}")
    logger.info(
        f"Iceberg Location: s3://{args.s3_bucket}/iceberg-warehouse/{args.database}.db/"
    )


if __name__ == "__main__":
    main()
