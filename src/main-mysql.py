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
    mysql_host: str
    mysql_port: str
    mysql_database: str
    mysql_table: str
    mysql_user: str
    mysql_password: str
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
        mysql_host=props["mysql.host"],
        mysql_port=props.get("mysql.port", "3306"),
        mysql_database=props["mysql.database"],
        mysql_table=props["mysql.table"],
        mysql_user=props["mysql.user"],
        mysql_password=props["mysql.password"],
        operator_chaining_enabled=props.get(
            "flink.operator-chaining.enabled", "true"
        ).lower()
        == "true",
    )


def load_config_from_args() -> JobConfig:
    parser = argparse.ArgumentParser(
        description="Kafka to MySQL Flink Job",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--kafka-bootstrap",
        default="boot-uaw.msklogstream.oee1gg.c16.kafka.us-east-1.amazonaws.com:9092",
        help="Kafka bootstrap servers",
    )
    parser.add_argument("--kafka-topic", default="test", help="Kafka topic")
    parser.add_argument(
        "--mysql-host",
        default="common-test.cpwuo9y53vjh.us-east-1.rds.amazonaws.com",
        help="MySQL host",
    )
    parser.add_argument("--mysql-port", default="3306", help="MySQL port")
    parser.add_argument("--mysql-database", default="test_db", help="MySQL database")
    parser.add_argument("--mysql-table", default="kafka_sink_data", help="MySQL table")
    parser.add_argument("--mysql-user", default="admin", help="MySQL user")
    parser.add_argument("--mysql-password", default="", help="MySQL password")
    parser.add_argument(
        "--disable-operator-chaining",
        action="store_true",
        help="Disable operator chaining",
    )

    args = parser.parse_args()
    return JobConfig(
        kafka_bootstrap=args.kafka_bootstrap,
        kafka_topic=args.kafka_topic,
        mysql_host=args.mysql_host,
        mysql_port=args.mysql_port,
        mysql_database=args.mysql_database,
        mysql_table=args.mysql_table,
        mysql_user=args.mysql_user,
        mysql_password=args.mysql_password,
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
            logger.warning("Run 'mvn clean package -P mysql' first")

    return table_env


def run_job(config: JobConfig):
    logger.info(f"Kafka: {config.kafka_bootstrap} / {config.kafka_topic}")
    logger.info(
        f"MySQL: {config.mysql_host}:{config.mysql_port}/{config.mysql_database}.{config.mysql_table}"
    )

    table_env = create_table_environment(config)

    create_source_sql = f"""
        CREATE TABLE kafka_source (
            id BIGINT,
            username STRING,
            email STRING,
            created_at TIMESTAMP(3),
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

    create_sink_sql = f"""
        CREATE TABLE mysql_sink (
            id BIGINT,
            username STRING,
            email STRING,
            created_at TIMESTAMP(3),
            updated_at TIMESTAMP(3),
            PRIMARY KEY (id) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:mysql://{config.mysql_host}:{config.mysql_port}/{config.mysql_database}?useSSL=false&allowPublicKeyRetrieval=true',
            'table-name' = '{config.mysql_table}',
            'username' = '{config.mysql_user}',
            'password' = '{config.mysql_password}',
            'sink.buffer-flush.max-rows' = '1000',
            'sink.buffer-flush.interval' = '10s',
            'sink.max-retries' = '3'
        )
    """

    insert_sql = """
        INSERT INTO mysql_sink
        SELECT
            id,
            username,
            email,
            created_at,
            CURRENT_TIMESTAMP as updated_at
        FROM kafka_source
    """

    try:
        logger.info("Creating Kafka source table")
        table_env.execute_sql(create_source_sql)

        logger.info("Creating MySQL sink table")
        table_env.execute_sql(create_sink_sql)

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
