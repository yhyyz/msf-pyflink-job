#!/usr/bin/env python3

import logging
import sys
from pyflink.table import EnvironmentSettings, TableEnvironment

def main():
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    
    logger.info("Starting PyFlink SQL job...")
    
    # Create table environment for streaming
    env_settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    table_env = TableEnvironment.create(env_settings)
    
    # Set job name and configuration
    config = table_env.get_config()
    config.get_configuration().set_string("pipeline.name", "PyFlink-SQL-Demo")
    
    logger.info("Creating source table...")
    
    # Create source table with datagen connector
    table_env.execute_sql("""
        CREATE TABLE source_table (
            user_id BIGINT,
            item_id BIGINT,
            category_id BIGINT,
            behavior STRING,
            ts TIMESTAMP(3),
            WATERMARK FOR ts AS ts - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'datagen',
            'rows-per-second' = '1',
            'fields.user_id.min' = '1',
            'fields.user_id.max' = '100',
            'fields.item_id.min' = '1',
            'fields.item_id.max' = '1000',
            'fields.category_id.min' = '1',
            'fields.category_id.max' = '10',
            'fields.behavior.length' = '10'
        )
    """)
    
    logger.info("Creating sink table...")
    
    # Create sink table with print connector
    table_env.execute_sql("""
        CREATE TABLE sink_table (
            user_id BIGINT,
            item_id BIGINT,
            category_id BIGINT,
            behavior STRING,
            ts TIMESTAMP(3),
            window_start TIMESTAMP(3),
            window_end TIMESTAMP(3),
            cnt BIGINT
        ) WITH (
            'connector' = 'print'
        )
    """)
    
    logger.info("Executing streaming SQL query...")
    
    # Execute streaming SQL query with windowing
    result = table_env.execute_sql("""
        INSERT INTO sink_table
        SELECT 
            user_id,
            item_id,
            category_id,
            behavior,
            ts,
            window_start,
            window_end,
            COUNT(*) as cnt
        FROM TABLE(
            TUMBLE(TABLE source_table, DESCRIPTOR(ts), INTERVAL '10' SECOND)
        )
        WHERE category_id <= 5
        GROUP BY user_id, item_id, category_id, behavior, ts, window_start, window_end
    """)
    
    logger.info("Job started, waiting for completion...")
    
    # Wait for the job to finish (it will run indefinitely in streaming mode)
    result.wait()

if __name__ == '__main__':
    main()
