#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from dataclasses import dataclass

from pyflink.common import Configuration
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
    doris_fenodes: str
    doris_database: str
    doris_table: str
    doris_user: str
    doris_password: str
    operator_chaining_enabled: bool = True


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
        doris_fenodes=props["doris.fenodes"],
        doris_database=props.get("doris.database", "test_db"),
        doris_table=props.get("doris.table", "cdc_sync_doris"),
        doris_user=props.get("doris.user", "root"),
        doris_password=props.get("doris.password", ""),
        operator_chaining_enabled=props.get(
            "flink.operator-chaining.enabled", "true"
        ).lower()
        == "true",
    )


def load_config_from_args() -> JobConfig:
    parser = argparse.ArgumentParser(
        description="Debezium CDC to Doris Flink Job",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--kafka-bootstrap",
        default="boot-uaw.msklogstream.oee1gg.c16.kafka.us-east-1.amazonaws.com:9092",
        help="Kafka bootstrap servers",
    )
    parser.add_argument(
        "--kafka-topic", default="test_prefix_all_data", help="Kafka CDC topic"
    )
    parser.add_argument(
        "--doris-fenodes",
        default="10.0.0.10:8030",
        help="Doris FE HTTP address (host:http_port)",
    )
    parser.add_argument("--doris-database", default="test_db", help="Doris database")
    parser.add_argument("--doris-table", default="cdc_sync_doris", help="Doris table")
    parser.add_argument("--doris-user", default="root", help="Doris username")
    parser.add_argument("--doris-password", default="", help="Doris password")
    parser.add_argument(
        "--disable-operator-chaining",
        action="store_true",
        help="Disable operator chaining",
    )

    args = parser.parse_args()
    return JobConfig(
        kafka_bootstrap=args.kafka_bootstrap,
        kafka_topic=args.kafka_topic,
        doris_fenodes=args.doris_fenodes,
        doris_database=args.doris_database,
        doris_table=args.doris_table,
        doris_user=args.doris_user,
        doris_password=args.doris_password,
        operator_chaining_enabled=not args.disable_operator_chaining,
    )


def create_table_environment(config: JobConfig) -> TableEnvironment:
    flink_config = Configuration()
    flink_config.set_string("execution.checkpointing.interval", "60s")

    if not config.operator_chaining_enabled:
        flink_config.set_boolean("pipeline.operator-chaining.enabled", False)
        logger.info("Operator chaining disabled")

    env_settings = (
        EnvironmentSettings.new_instance()
        .in_streaming_mode()
        .with_configuration(flink_config)
        .build()
    )
    table_env = TableEnvironment.create(env_settings)

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
            logger.warning("Run 'mvn clean package -P doris' first")

    return table_env


def run_job(config: JobConfig):
    logger.info(f"Kafka: {config.kafka_bootstrap} / {config.kafka_topic}")
    logger.info(
        f"Doris: {config.doris_fenodes} / {config.doris_database}.{config.doris_table}"
    )

    table_env = create_table_environment(config)

    # Debezium CDC source from Kafka
    create_source_sql = f"""
        CREATE TABLE debezium_source (
            id BIGINT,
            name STRING,
            email STRING,
            created_at STRING,
            PRIMARY KEY (id) NOT ENFORCED
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{config.kafka_topic}',
            'properties.bootstrap.servers' = '{config.kafka_bootstrap}',
            'properties.group.id' = 'flink-debezium-doris-consumer',
            'format' = 'debezium-json',
            'debezium-json.schema-include' = 'false',
            'debezium-json.ignore-parse-errors' = 'true',
            'scan.startup.mode' = 'earliest-offset'
        )
    """

    # Doris sink table
    # Note: Doris table must be created in advance with UNIQUE KEY model for CDC upsert/delete
    create_sink_sql = f"""
        CREATE TABLE doris_sink (
            id BIGINT,
            name STRING,
            email STRING,
            created_at STRING,
            PRIMARY KEY (id) NOT ENFORCED
        ) WITH (
            'connector' = 'doris',
            'fenodes' = '{config.doris_fenodes}',
            'table.identifier' = '{config.doris_database}.{config.doris_table}',
            'username' = '{config.doris_user}',
            'password' = '{config.doris_password}',
            'sink.label-prefix' = 'flink_cdc_doris',
            'sink.properties.format' = 'json',
            'sink.properties.read_json_by_line' = 'true',
            'sink.enable-delete' = 'true'
        )
    """

    insert_sql = """
        INSERT INTO doris_sink
        SELECT id, name, email, created_at
        FROM debezium_source
    """

    try:
        logger.info("Creating Debezium Kafka source table")
        table_env.execute_sql(create_source_sql)

        logger.info("Creating Doris sink table")
        table_env.execute_sql(create_sink_sql)

        logger.info("Starting CDC data sync to Doris")
        result = table_env.execute_sql(insert_sql)

        logger.info("Flink CDC job started, syncing data to Doris...")
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
