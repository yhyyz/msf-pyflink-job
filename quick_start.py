#!/usr/bin/env python3
from __future__ import annotations

import argparse
import logging
import os
import sys

import boto3

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".", "src"))

from flink_manager import FlinkManager

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

JOB_PROPERTY_KEYS: dict[str, list[str]] = {
    "main-kafka-iceberg.py": [
        "kafka.bootstrap",
        "kafka.topic",
        "iceberg.warehouse",
        "iceberg.database",
        "iceberg.table",
        "aws.region",
    ],
    "main-kafka-s3.py": [
        "kafka.bootstrap",
        "kafka.topic",
        "s3.output.path",
        "aws.region",
    ],
    "main-mysql.py": [
        "kafka.bootstrap",
        "kafka.topic",
        "mysql.host",
        "mysql.port",
        "mysql.database",
        "mysql.table",
        "mysql.user",
        "mysql.password",
    ],
    "main-debezium-mysql.py": [
        "kafka.bootstrap",
        "kafka.topic",
        "mysql.host",
        "mysql.port",
        "mysql.database",
        "mysql.table",
        "mysql.user",
        "mysql.password",
    ],
    "main-debezium-iceberg.py": [
        "kafka.bootstrap",
        "kafka.topic",
        "iceberg.warehouse",
        "iceberg.database",
        "iceberg.table",
        "aws.region",
    ],
    "main-debezium-doris.py": [
        "kafka.bootstrap",
        "kafka.topic",
        "doris.fenodes",
        "doris.database",
        "doris.table",
        "doris.user",
        "doris.password",
        "flink.operator-chaining.enabled",
    ],
    "main-simple.py": [],
}

# Required arguments by job type
JOB_REQUIRED_ARGS: dict[str, list[str]] = {
    "main-kafka-iceberg.py": ["iceberg_warehouse"],
    "main-debezium-iceberg.py": ["iceberg_warehouse"],
    "main-kafka-s3.py": ["s3_output_path"],
    "main-debezium-doris.py": ["doris_fenodes"],
    "main-mysql.py": ["mysql_host", "mysql_user"],
    "main-debezium-mysql.py": ["mysql_host", "mysql_user"],
    "main-simple.py": [],
}


def validate_args(args) -> None:
    """Validate required arguments based on operation mode and job type"""
    errors = []

    if args.delete:
        # Delete mode only requires aws_region
        if not args.aws_region:
            errors.append("--aws_region is required (eg. us-east-1)")
    else:
        # Deploy mode - common required args
        if not args.aws_region:
            errors.append("--aws_region is required (eg. us-east-1)")
        if not args.msk_cluster_name:
            errors.append("--msk_cluster_name is required (eg. msk-log-stream)")
        if not args.kafka_topic:
            errors.append("--kafka_topic is required (eg. test)")

        # Job-specific required args
        required_args = JOB_REQUIRED_ARGS.get(args.python_main, [])

        if "iceberg_warehouse" in required_args and not args.iceberg_warehouse:
            errors.append(
                "--iceberg_warehouse is required (eg. s3://your-bucket/iceberg-warehouse/)"
            )
        if "s3_output_path" in required_args and not args.s3_output_path:
            errors.append(
                "--s3_output_path is required (eg. s3://your-bucket/flink-output/)"
            )
        if "doris_fenodes" in required_args and not args.doris_fenodes:
            errors.append("--doris_fenodes is required (eg. 10.0.0.10:8030)")
        if "mysql_host" in required_args and not args.mysql_host:
            errors.append("--mysql_host is required (eg. your-mysql.rds.amazonaws.com)")
        if "mysql_user" in required_args and not args.mysql_user:
            errors.append("--mysql_user is required (eg. admin)")
        # mysql_password allows empty string, no validation

    if errors:
        for e in errors:
            logger.error(e)
        sys.exit(1)


def get_msk_cluster_info(cluster_name: str, region: str) -> dict[str, str] | None:
    """Get MSK cluster info: bootstrap servers, subnet, security group"""
    kafka_client = boto3.client("kafka", region_name=region)
    ec2_client = boto3.client("ec2", region_name=region)

    try:
        response = kafka_client.list_clusters_v2()
        cluster_arn = None
        for cluster in response.get("ClusterInfoList", []):
            if cluster.get("ClusterName") == cluster_name:
                cluster_arn = cluster.get("ClusterArn")
                break

        if not cluster_arn:
            logger.warning(f"MSK cluster '{cluster_name}' not found")
            return None

        cluster_info = kafka_client.describe_cluster_v2(ClusterArn=cluster_arn)
        provisioned = cluster_info["ClusterInfo"].get("Provisioned", {})

        client_subnets = provisioned.get("BrokerNodeGroupInfo", {}).get(
            "ClientSubnets", []
        )
        security_groups = provisioned.get("BrokerNodeGroupInfo", {}).get(
            "SecurityGroups", []
        )

        bootstrap_response = kafka_client.get_bootstrap_brokers(ClusterArn=cluster_arn)
        bootstrap_servers = (
            bootstrap_response.get("BootstrapBrokerString")
            or bootstrap_response.get("BootstrapBrokerStringSaslIam")
            or bootstrap_response.get("BootstrapBrokerStringTls")
        )

        subnet_id = client_subnets[0] if client_subnets else None
        sg_id = security_groups[0] if security_groups else None

        if subnet_id:
            subnet_info = ec2_client.describe_subnets(SubnetIds=[subnet_id])
            subnets = subnet_info.get("Subnets", [])
            if subnets:
                map_public = subnets[0].get("MapPublicIpOnLaunch", False)
                if map_public:
                    for sid in client_subnets[1:]:
                        subnet_check = ec2_client.describe_subnets(SubnetIds=[sid])
                        if not subnet_check["Subnets"][0].get(
                            "MapPublicIpOnLaunch", False
                        ):
                            subnet_id = sid
                            break

        logger.info(f"MSK cluster '{cluster_name}' found")
        logger.info(f"  Bootstrap: {bootstrap_servers}")
        logger.info(f"  Subnet: {subnet_id}")
        logger.info(f"  Security Group: {sg_id}")

        return {
            "bootstrap_servers": bootstrap_servers or "",
            "subnet_id": subnet_id or "",
            "sg_id": sg_id or "",
        }

    except Exception as e:
        logger.error(f"Failed to get MSK cluster info: {e}")
        return None


def build_app_properties(args) -> dict[str, str]:
    all_properties = {
        "kafka.bootstrap": args.kafka_bootstrap,
        "kafka.topic": args.kafka_topic,
        "iceberg.warehouse": args.iceberg_warehouse,
        "iceberg.database": args.iceberg_database,
        "iceberg.table": args.iceberg_table,
        "s3.output.path": args.s3_output_path,
        "mysql.host": args.mysql_host,
        "mysql.port": args.mysql_port,
        "mysql.database": args.mysql_database,
        "mysql.table": args.mysql_table,
        "mysql.user": args.mysql_user,
        "mysql.password": args.mysql_password,
        "doris.fenodes": args.doris_fenodes,
        "doris.database": args.doris_database,
        "doris.table": args.doris_table,
        "doris.user": args.doris_user,
        "doris.password": args.doris_password,
        "aws.region": args.aws_region,
        "flink.operator-chaining.enabled": "false"
        if args.disable_operator_chaining
        else "true",
    }

    required_keys = JOB_PROPERTY_KEYS.get(args.python_main, list(all_properties.keys()))
    return {k: v for k, v in all_properties.items() if k in required_keys and v}


def delete_application(args):
    """Delete a Flink application"""
    app_name = args.app_name
    aws_region = args.aws_region

    logger.info(f"Deleting Flink application: {app_name}")

    manager = FlinkManager(region=aws_region)

    try:
        result = manager.delete_application(app_name)
        if result:
            logger.info(f"Application '{app_name}' deleted successfully")
            return True
        else:
            logger.error(f"Failed to delete application '{app_name}'")
            return False
    except Exception as e:
        logger.error(f"Delete failed: {e}")
        return False


def quick_start(args):
    app_name = args.app_name
    s3_bucket = args.s3_bucket
    s3_key = args.s3_key
    aws_region = args.aws_region
    local_zip_path = args.local_dep_jar_path
    python_main = args.python_main

    subnet_id = args.subnet_id
    sg_id = args.sg_id
    kafka_bootstrap = args.kafka_bootstrap

    if args.msk_cluster_name:
        msk_info = get_msk_cluster_info(args.msk_cluster_name, aws_region)
        if msk_info:
            if not args.subnet_id:
                subnet_id = msk_info["subnet_id"]
            if not args.sg_id:
                sg_id = msk_info["sg_id"]
            if not args.kafka_bootstrap:
                kafka_bootstrap = msk_info["bootstrap_servers"]
        else:
            logger.warning("Using provided subnet_id, sg_id, kafka_bootstrap instead")

    if not subnet_id or not sg_id:
        logger.error(
            "subnet_id and sg_id are required. Provide them or use --msk_cluster_name"
        )
        return False

    args.kafka_bootstrap = kafka_bootstrap

    local_zip_file = os.path.join(os.path.dirname(__file__), local_zip_path)

    logger.info(f"Deploying PyFlink job: {app_name}")
    logger.info(f"Python main: {python_main}")
    logger.info(f"Subnet: {subnet_id}, SG: {sg_id}")
    logger.info(f"Kafka bootstrap: {kafka_bootstrap}")
    logger.info(f"Local zip: {local_zip_file}")

    manager = FlinkManager(region=aws_region, s3_bucket=s3_bucket)

    try:
        logger.info("Uploading to S3...")
        s3_bucket_arn = f"arn:aws:s3:::{s3_bucket}"
        manager.upload_to_s3(local_zip_file, s3_bucket, s3_key)

        app_properties = build_app_properties(args)
        logger.info(f"Application properties: {list(app_properties.keys())}")

        logger.info("Creating Flink application...")
        manager.create_application(
            app_name,
            s3_bucket_arn,
            s3_key,
            subnet_id,
            sg_id,
            python_main_file=python_main,
            app_properties=app_properties,
        )

        logger.info("Starting application...")
        manager.start_application(app_name)

        logger.info(f"Deployment complete: {app_name}")

    except Exception as e:
        logger.error(f"Deployment failed: {e}")
        return False

    return True


def main():
    parser = argparse.ArgumentParser(
        description="Deploy PyFlink job to AWS Managed Service for Apache Flink",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    parser.add_argument(
        "--app_name",
        default="flink-msk-iceberg-sink-demo",
        help="Application name",
    )
    parser.add_argument(
        "--delete",
        action="store_true",
        help="Delete the application instead of creating it",
    )
    parser.add_argument(
        "--s3_bucket",
        default="pcd-ue1-01",
        help="S3 bucket for code",
    )
    parser.add_argument(
        "--s3_key",
        default="flink-jobs/flink-msk-iceberg-sink-demo.zip",
        help="S3 object key",
    )
    parser.add_argument(
        "--subnet_id",
        default="",
        help="VPC subnet ID (auto-detect from MSK if not provided)",
    )
    parser.add_argument(
        "--sg_id",
        default="",
        help="Security group ID (auto-detect from MSK if not provided)",
    )
    parser.add_argument(
        "--msk_cluster_name",
        default="",
        help="MSK cluster name for auto-detect subnet/sg/bootstrap (eg. msk-log-stream)",
    )
    parser.add_argument(
        "--aws_region",
        default="",
        help="AWS region (eg. us-east-1)",
    )
    parser.add_argument(
        "--local_dep_jar_path",
        default="target/msf-pyflink-iceberg-1.0.0.zip",
        help="Local zip file path",
    )
    parser.add_argument(
        "--python_main",
        default="main-kafka-iceberg.py",
        help="Python entry file",
    )

    kafka_group = parser.add_argument_group("Kafka")
    kafka_group.add_argument(
        "--kafka_bootstrap",
        default="",
        help="Kafka bootstrap servers (auto-detect from MSK if not provided)",
    )
    kafka_group.add_argument(
        "--kafka_topic",
        default="",
        help="Kafka topic (eg. test)",
    )

    iceberg_group = parser.add_argument_group("Iceberg")
    iceberg_group.add_argument(
        "--iceberg_warehouse",
        default="",
        help="Iceberg warehouse path (eg. s3://your-bucket/iceberg-warehouse/)",
    )
    iceberg_group.add_argument(
        "--iceberg_database",
        default="test_iceberg_db",
        help="Iceberg database name (eg. test_iceberg_db)",
    )
    iceberg_group.add_argument(
        "--iceberg_table",
        default="kafka_agg_sink",
        help="Iceberg table name (eg. kafka_agg_sink)",
    )

    s3_group = parser.add_argument_group("S3 Sink")
    s3_group.add_argument(
        "--s3_output_path",
        default="",
        help="S3 output path (eg. s3://your-bucket/flink-output/)",
    )

    mysql_group = parser.add_argument_group("MySQL")
    mysql_group.add_argument(
        "--mysql_host",
        default="",
        help="MySQL host (eg. your-mysql.rds.amazonaws.com)",
    )
    mysql_group.add_argument(
        "--mysql_port",
        default="3306",
        help="MySQL port (eg. 3306)",
    )
    mysql_group.add_argument(
        "--mysql_database",
        default="test_db",
        help="MySQL database (eg. test_db)",
    )
    mysql_group.add_argument(
        "--mysql_table",
        default="kafka_sink_data",
        help="MySQL table (eg. kafka_sink_data)",
    )
    mysql_group.add_argument(
        "--mysql_user",
        default="",
        help="MySQL username (eg. admin)",
    )
    mysql_group.add_argument(
        "--mysql_password",
        default="",
        help="MySQL password",
    )

    doris_group = parser.add_argument_group("Doris")
    doris_group.add_argument(
        "--doris_fenodes",
        default="",
        help="Doris FE HTTP address (eg. 10.0.0.10:8030)",
    )
    doris_group.add_argument(
        "--doris_database",
        default="test_db",
        help="Doris database (eg. test_db)",
    )
    doris_group.add_argument(
        "--doris_table",
        default="cdc_sync_doris",
        help="Doris table (eg. cdc_sync_doris)",
    )
    doris_group.add_argument(
        "--doris_user",
        default="root",
        help="Doris username (eg. root)",
    )
    doris_group.add_argument(
        "--doris_password",
        default="",
        help="Doris password",
    )

    flink_group = parser.add_argument_group("Flink")
    flink_group.add_argument(
        "--disable_operator_chaining",
        action="store_true",
        help="Disable operator chaining",
    )

    args = parser.parse_args()
    validate_args(args)

    if args.delete:
        delete_application(args)
    else:
        quick_start(args)


if __name__ == "__main__":
    main()
