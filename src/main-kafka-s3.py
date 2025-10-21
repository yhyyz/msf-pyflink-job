#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from pyflink.table import EnvironmentSettings, TableEnvironment
from pyflink.table.expressions import col
import sys

def main():
    """
    Flink MSK to Iceberg Job
    从Kafka MSK读取数据并进行窗口聚合，输出到S3
    """
    # 创建表环境
    env_settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    table_env = TableEnvironment.create(env_settings)

    # 启用检查点，间隔60秒
    table_env.get_config().get_configuration().set_string("execution.checkpointing.interval", "60s")

    # 创建Kafka源表SQL
    create_source_table = """
        CREATE TABLE msk_source (
            id BIGINT,
            username STRING,
            proc_time AS PROCTIME()
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'test',
            'properties.bootstrap.servers' = 'boot-uaw.msklogstream.oee1gg.c16.kafka.us-east-1.amazonaws.com:9092',
            'format' = 'json',
            'json.fail-on-missing-field' = 'false',
            'json.ignore-parse-errors' = 'true',
            'scan.startup.mode' = 'latest-offset'
        )
    """

    # 创建输出表SQL（输出到S3）
    create_sink_table = """
        CREATE TABLE output_stats (
            window_start TIMESTAMP(3),
            window_end TIMESTAMP(3),
            record_count BIGINT,
            unique_users BIGINT,
            created_at TIMESTAMP(3)
        ) WITH (
            'connector' = 'filesystem',
            'path' = 's3://pcd-01/flink-output/',
            'format' = 'json',
            'sink.rolling-policy.rollover-interval' = '1min',
            'sink.rolling-policy.check-interval' = '30s'
        )
    """

    # 数据处理和聚合SQL
    insert_sql = """
        INSERT INTO output_stats
        SELECT
            TUMBLE_START(proc_time, INTERVAL '1' MINUTE) as window_start,
            TUMBLE_END(proc_time, INTERVAL '1' MINUTE) as window_end,
            COUNT(*) as record_count,
            COUNT(DISTINCT username) as unique_users,
            CURRENT_TIMESTAMP as created_at
        FROM msk_source
        GROUP BY TUMBLE(proc_time, INTERVAL '1' MINUTE)
    """

    try:
        # 执行SQL语句
        print("创建Kafka源表...")
        table_env.execute_sql(create_source_table)

        print("创建输出表...")
        table_env.execute_sql(create_sink_table)

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
