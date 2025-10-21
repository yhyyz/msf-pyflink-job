#!/usr/bin/env python3
"""
快速开始脚本 - 最简单的PyFlink作业部署示例
"""

import sys
import os
import argparse

# Add src directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '.', 'src'))

from flink_manager import FlinkManager

def quick_start(args):
    
    # 配置 - 从命令行参数获取
    APP_NAME = args.app_name
    S3_BUCKET = args.s3_bucket
    S3_KEY = args.s3_key
    SUBNET_ID = args.subnet_id
    SG_ID = args.sg_id
    AWS_REGION = args.aws_region
    LOCAL_DEP_JAR_PATH = args.local_dep_jar_path
    
    src_dir = os.path.join(os.path.dirname(__file__), 'src')
    job_file = os.path.join(src_dir, 'main.py')
    current_file_path = os.path.dirname(__file__)
    
    LOCAL_ZIP_FILE = os.path.join(os.path.dirname(__file__), LOCAL_DEP_JAR_PATH)
    
    print(f"🚀 快速部署PyFlink作业: {APP_NAME}")
    #requirements_file = os.path.join(src_dir, 'requirements.txt')
    print(f"local zip file path: {LOCAL_ZIP_FILE}")
    
    # 初始化管理器
    manager = FlinkManager(region=AWS_REGION)
    
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
    parser = argparse.ArgumentParser(description='快速部署PyFlink作业到AWS Managed Service for Apache Flink')
    
    parser.add_argument('--app_name', default='flink-msk-iceberg-sink-demo', 
                       help='应用程序名称 (默认: flink-msk-iceberg-sink-demo)')
    parser.add_argument('--s3_bucket', default='pcd-01', 
                       help='S3存储桶名称 (默认: pcd-01)')
    parser.add_argument('--s3_key', default='flink-jobs/flink-msk-iceberg-sink-demo.zip', 
                       help='S3对象键 (默认: flink-jobs/flink-msk-iceberg-sink-demo.zip)')
    parser.add_argument('--subnet_id', default='subnet-0f79e4471cfa74ced', 
                       help='子网ID (默认: subnet-0f79e4471cfa74ced)')
    parser.add_argument('--sg_id', default='sg-f83dcdb3', 
                       help='安全组ID (默认: sg-f83dcdb3)')
    parser.add_argument('--aws_region', default='ap-southeast-1', 
                       help='AWS区域 (默认: ap-southeast-1)')
    parser.add_argument('--local_dep_jar_path', default='target/managed-flink-pyfink-msk-iceberg-1.0.0.zip', 
                       help='本地依赖JAR路径 (默认: target/managed-flink-pyfink-msk-iceberg-1.0.0.zip)')
    
    args = parser.parse_args()
    quick_start(args)
