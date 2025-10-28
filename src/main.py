#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from pyflink.table import EnvironmentSettings, TableEnvironment
from pyflink.table.expressions import col
import sys

def main():
    """
    Flink MSK to Iceberg Job
    从Kafka MSK读取数据并进行窗口聚合，输出到Iceberg表
    """
    # Iceberg配置
    iceberg_catalog_name = "iceberg_catalog"
    iceberg_database_name = "default_db"
    iceberg_table_name = "msk_stats"
    iceberg_warehouse_path = "s3://pcd-01/tmp/msf-test/"
    kafka_server="b-1.standardxlarge.f9l2in.c3.kafka.ap-southeast-1.amazonaws.com:9092"
    kafka_topic="test"

    # 创建表环境
    env_settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    table_env = TableEnvironment.create(env_settings)

    # 启用检查点，间隔60秒
    table_env.get_config().get_configuration().set_string("execution.checkpointing.interval", "60s")

    # 创建Kafka源表SQL
    create_source_table = f"""
        CREATE TABLE msk_source (
            id BIGINT,
            username STRING,
            proc_time AS PROCTIME()
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{kafka_topic}',
            'properties.bootstrap.servers' = '{kafka_server}',
            'format' = 'json',
            'json.fail-on-missing-field' = 'false',
            'json.ignore-parse-errors' = 'true',
            'scan.startup.mode' = 'latest-offset'
        )
    """

    # 创建Iceberg catalog
    create_catalog_sql = f"""
        CREATE CATALOG {iceberg_catalog_name} WITH (
            'type' = 'iceberg',
            'catalog-impl' = 'org.apache.iceberg.aws.glue.GlueCatalog',
            'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO',
            'warehouse' = '{iceberg_warehouse_path}'
            )
    """

    # 创建数据库SQL
    create_database_sql = f"CREATE DATABASE IF NOT EXISTS `{iceberg_database_name}`"

    # 创建Iceberg表SQL
    create_iceberg_table_sql = f"""
        CREATE TABLE IF NOT EXISTS `{iceberg_catalog_name}`.`{iceberg_database_name}`.`{iceberg_table_name}` (
            window_start TIMESTAMP(3),
            window_end TIMESTAMP(3),
            record_count BIGINT,
            unique_users BIGINT,
            created_at TIMESTAMP(3))
        PARTITIONED BY (window_start)
        WITH (
            'write.format.default' = 'parquet',
            'write.target-file-size-bytes' = '536870912',
            'write.upsert.enabled' = 'false',
            'write.metadata.delete-after-commit.enabled' = 'true',
            'write.metadata.previous-versions-max' = '3',
            'write.parquet.compression-codec' = 'zstd'
        )
    """

    # 数据处理和聚合SQL
    insert_sql = f"""
        INSERT INTO `{iceberg_catalog_name}`.`{iceberg_database_name}`.`{iceberg_table_name}`
        SELECT
            TUMBLE_START(proc_time, INTERVAL '1' MINUTE) as window_start,
            TUMBLE_END(proc_time, INTERVAL '1' MINUTE) as window_end,
            COUNT(*) as record_count,
            COUNT(DISTINCT username) as unique_users,
            CURRENT_TIMESTAMP as created_at
        FROM default_catalog.default_database.msk_source
        GROUP BY TUMBLE(proc_time, INTERVAL '1' MINUTE)
    """

    try:
        # 执行SQL语句
        print("创建Kafka源表...")
        table_env.execute_sql(create_source_table)

        print("创建Iceberg catalog...")
        table_env.execute_sql(create_catalog_sql)

        print("使用catalog...")
        table_env.execute_sql(f"USE CATALOG `{iceberg_catalog_name}`;")

        print("创建数据库...")
        table_env.execute_sql(create_database_sql)

        print("使用数据库...")
        table_env.execute_sql(f"USE `{iceberg_database_name}`;")

        print("创建Iceberg表...")
        table_env.execute_sql(create_iceberg_table_sql)

        print("开始数据处理...")
        table_result = table_env.execute_sql(insert_sql)

        print("Flink作业已启动，正在处理数据...")
        # 等待作业完成，保持程序运行
        table_result.wait()

    except Exception as e:
        print(f"作业执行失败: {e}", file=sys.stderr)
        raise e

if __name__ == "__main__":
    main()

