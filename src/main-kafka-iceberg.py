#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from dataclasses import dataclass

from pyflink.table import EnvironmentSettings, TableEnvironment

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

MSF_PROPERTIES_FILE = "/etc/flink/application_properties.json"
PROPERTY_GROUP_ID = "FlinkApplicationProperties"


@dataclass
class JobConfig:
    kafka_bootstrap: str
    kafka_topic: str
    iceberg_warehouse: str
    iceberg_database: str
    iceberg_table: str
    aws_region: str


def is_running_on_msf() -> bool:
    return os.path.isfile(MSF_PROPERTIES_FILE)


def load_msf_properties() -> dict[str, str]:
    with open(MSF_PROPERTIES_FILE, "r") as f:
        props = json.load(f)
    for group in props:
        if group["PropertyGroupId"] == PROPERTY_GROUP_ID:
            return group["PropertyMap"]
    raise ValueError(f"Property group '{PROPERTY_GROUP_ID}' not found")


def load_config_from_msf() -> JobConfig:
    props = load_msf_properties()
    return JobConfig(
        kafka_bootstrap=props["kafka.bootstrap"],
        kafka_topic=props["kafka.topic"],
        iceberg_warehouse=props["iceberg.warehouse"],
        iceberg_database=props.get("iceberg.database", "default_db"),
        iceberg_table=props.get("iceberg.table", "kafka_agg_sink"),
        aws_region=props.get("aws.region", "us-east-1"),
    )


def load_config_from_args() -> JobConfig:
    parser = argparse.ArgumentParser(
        description="Kafka to Iceberg Flink Job",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--kafka-bootstrap",
        default="boot-uaw.msklogstream.oee1gg.c16.kafka.us-east-1.amazonaws.com:9092",
        help="Kafka bootstrap servers",
    )
    parser.add_argument("--kafka-topic", default="test", help="Kafka topic")
    parser.add_argument(
        "--iceberg-warehouse",
        default="s3://pcd-ue1-01/iceberg-warehouse/",
        help="Iceberg warehouse path",
    )
    parser.add_argument(
        "--iceberg-database", default="test_iceberg_db", help="Iceberg database"
    )
    parser.add_argument(
        "--iceberg-table", default="kafka_agg_sink", help="Iceberg table"
    )
    parser.add_argument("--aws-region", default="us-east-1", help="AWS region")

    args = parser.parse_args()
    return JobConfig(
        kafka_bootstrap=args.kafka_bootstrap,
        kafka_topic=args.kafka_topic,
        iceberg_warehouse=args.iceberg_warehouse,
        iceberg_database=args.iceberg_database,
        iceberg_table=args.iceberg_table,
        aws_region=args.aws_region,
    )


def create_table_environment() -> TableEnvironment:
    env_settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    table_env = TableEnvironment.create(env_settings)
    table_env.get_config().get_configuration().set_string(
        "execution.checkpointing.interval", "60s"
    )

    if not is_running_on_msf():
        table_env.get_config().get_configuration().set_string("rest.port", "8081")
        current_dir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
        jar_path = f"file:///{current_dir}/target/pyflink-dependencies.jar"
        if os.path.exists(f"{current_dir}/target/pyflink-dependencies.jar"):
            table_env.get_config().get_configuration().set_string(
                "pipeline.jars", jar_path
            )
            logger.info(f"Using JAR: {jar_path}")
            logger.info("Flink Web UI: http://localhost:8081")
        else:
            logger.warning(
                f"JAR not found: {current_dir}/target/pyflink-dependencies.jar"
            )
            logger.warning("Run 'mvn clean package' first")

    return table_env


def run_job(config: JobConfig):
    logger.info(f"Kafka: {config.kafka_bootstrap} / {config.kafka_topic}")
    logger.info(
        f"Iceberg: {config.iceberg_warehouse}{config.iceberg_database}/{config.iceberg_table}"
    )
    logger.info(f"Region: {config.aws_region}")

    table_env = create_table_environment()
    iceberg_catalog = "iceberg_catalog"

    create_source_sql = f"""
        CREATE TABLE kafka_source (
            id BIGINT,
            username STRING,
            proc_time AS PROCTIME()
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{config.kafka_topic}',
            'properties.bootstrap.servers' = '{config.kafka_bootstrap}',
            'format' = 'json',
            'json.fail-on-missing-field' = 'false',
            'json.ignore-parse-errors' = 'true',
            'scan.startup.mode' = 'latest-offset'
        )
    """

    create_catalog_sql = f"""
        CREATE CATALOG {iceberg_catalog} WITH (
            'type' = 'iceberg',
            'catalog-impl' = 'org.apache.iceberg.aws.glue.GlueCatalog',
            'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO',
            'warehouse' = '{config.iceberg_warehouse}',
            'client.region' = '{config.aws_region}',
            's3.endpoint' = 'https://s3.{config.aws_region}.amazonaws.com'
        )
    """

    create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS `{iceberg_catalog}`.`{config.iceberg_database}`.`{config.iceberg_table}` (
            window_start TIMESTAMP(3),
            window_end TIMESTAMP(3),
            record_count BIGINT,
            unique_users BIGINT,
            created_at TIMESTAMP(3)
        ) PARTITIONED BY (window_start)
        WITH (
            'format-version' = '2',
            'write.format.default' = 'parquet',
            'write.target-file-size-bytes' = '536870912',
            'write.upsert.enabled' = 'false',
            'write.parquet.compression-codec' = 'zstd'
        )
    """

    insert_sql = f"""
        INSERT INTO `{iceberg_catalog}`.`{config.iceberg_database}`.`{config.iceberg_table}`
        SELECT
            TUMBLE_START(proc_time, INTERVAL '1' MINUTE) as window_start,
            TUMBLE_END(proc_time, INTERVAL '1' MINUTE) as window_end,
            COUNT(*) as record_count,
            COUNT(DISTINCT username) as unique_users,
            CURRENT_TIMESTAMP as created_at
        FROM default_catalog.default_database.kafka_source
        GROUP BY TUMBLE(proc_time, INTERVAL '1' MINUTE)
    """

    try:
        logger.info("Creating Kafka source table")
        table_env.execute_sql(create_source_sql)

        logger.info("Creating Iceberg catalog")
        table_env.execute_sql(create_catalog_sql)

        logger.info(f"Using catalog: {iceberg_catalog}")
        table_env.execute_sql(f"USE CATALOG `{iceberg_catalog}`")

        logger.info(f"Creating database: {config.iceberg_database}")
        table_env.execute_sql(
            f"CREATE DATABASE IF NOT EXISTS `{config.iceberg_database}`"
        )

        logger.info(f"Using database: {config.iceberg_database}")
        table_env.execute_sql(f"USE `{config.iceberg_database}`")

        logger.info(f"Creating Iceberg table: {config.iceberg_table}")
        table_env.execute_sql(create_table_sql)

        logger.info("Starting data processing")
        result = table_env.execute_sql(insert_sql)

        logger.info("Flink job started, processing data...")
        result.wait()

    except Exception as e:
        logger.error(f"Job failed: {e}")
        sys.exit(1)


def main():
    if is_running_on_msf():
        logger.info("Running on MSF")
        config = load_config_from_msf()
    else:
        logger.info("Running locally")
        config = load_config_from_args()

    run_job(config)


if __name__ == "__main__":
    main()
