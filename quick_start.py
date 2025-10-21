#!/usr/bin/env python3
"""
快速开始脚本 - 最简单的PyFlink作业部署示例
"""

import sys
import os

# Add src directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '.', 'src'))

from flink_manager import FlinkManager

def quick_start():
    """快速部署一个PyFlink作业"""
    
    # 配置 - 请根据您的环境修改
    APP_NAME = 'flink-msk-iceberg-sink-demo'
    S3_BUCKET = 'pcd-01'  # 请修改为您的S3存储桶
    S3_KEY = 'flink-jobs/flink-msk-iceberg-sink-demo.zip'
    SUBNET_ID = "subnet-0f79e4471cfa74ced"
    SG_ID = "sg-f83dcdb3"
    
    src_dir = os.path.join(os.path.dirname(__file__), 'src')
    job_file = os.path.join(src_dir, 'main.py')
    current_file_path = os.path.dirname(__file__)
    
    LOCAL_ZIP_FILE = os.path.join(os.path.dirname(__file__), 'target/managed-flink-pyfink-msk-iceberg-1.0.0.zip')
    
    print(f"🚀 快速部署PyFlink作业: {APP_NAME}")
    #requirements_file = os.path.join(src_dir, 'requirements.txt')
    print(f"local zip file path: {LOCAL_ZIP_FILE}")
    
    # 初始化管理器
    manager = FlinkManager(region="ap-southeast-1")
    
    try:
        # 1. 创建应用程序包, 直接提供LOCAL_ZIP_FILE,不需要创建zip，如果要自己创建zip,将依赖放到src/lib/目录下
        #print("📦 创建应用程序包...")
        #zip_file = manager.create_python_application_zip(job_file, requirements_file)
        zip_file = LOCAL_ZIP_FILE
        # 2. 上传到S3
        print("☁️ 上传到S3...")
        s3_bucket_arn = f'arn:aws:s3:::{S3_BUCKET}'
        s3_url = manager.upload_to_s3(zip_file, S3_BUCKET, S3_KEY)
        
        # 3. 创建并启动应用程序
        print("🔧 创建Flink应用程序...")
        manager.create_application(APP_NAME, s3_bucket_arn, S3_KEY, SUBNET_ID,SG_ID)
        
        print("🚀 启动应用程序...")
        manager.start_application(APP_NAME)
        
        print("✅ 部署完成!")
        print(f"应用程序名称: {APP_NAME}")
        
    except Exception as e:
        print(f"❌ 部署失败: {e}")
        return False
    
    return True

if __name__ == '__main__':
    quick_start()
