#!/usr/bin/env python3
"""
快速开始脚本 - 最简单的PyFlink作业部署示例
"""

import sys
import os

# Add src directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from flink_manager import FlinkManager

def quick_start():
    """快速部署一个PyFlink作业"""
    
    # 配置 - 请根据您的环境修改
    APP_NAME = 'my-pyflink-app-02'
    S3_BUCKET = 'pcd-01'  # 请修改为您的S3存储桶
    S3_KEY = 'flink-jobs/my-app.zip'
    
    print(f"🚀 快速部署PyFlink作业: {APP_NAME}")
    print("-" * 50)
    
    # 获取源文件路径
    src_dir = os.path.join(os.path.dirname(__file__), '..', 'src')
    job_file = os.path.join(src_dir, 'pyflink_sql_job.py')
    requirements_file = os.path.join(src_dir, 'requirements.txt')
    
    # 初始化管理器
    manager = FlinkManager()
    
    try:
        # 1. 创建应用程序包
        print("📦 创建应用程序包...")
        zip_file = manager.create_python_application_zip(job_file, requirements_file)
        
        # 2. 上传到S3
        print("☁️ 上传到S3...")
        s3_bucket_arn = f'arn:aws:s3:::{S3_BUCKET}'
        s3_url = manager.upload_to_s3(zip_file, S3_BUCKET, S3_KEY)
        
        # 3. 创建并启动应用程序
        print("🔧 创建Flink应用程序...")
        manager.create_application(APP_NAME, s3_bucket_arn, S3_KEY)
        
        print("🚀 启动应用程序...")
        manager.start_application(APP_NAME)
        
        print("✅ 部署完成!")
        print(f"应用程序名称: {APP_NAME}")
        print(f"S3位置: {s3_url}")
        print("\n管理命令:")
        print(f"  查看状态: manager.get_application_status('{APP_NAME}')")
        print(f"  停止应用: manager.stop_application('{APP_NAME}')")
        print(f"  删除应用: manager.delete_application('{APP_NAME}')")
        
        # 清理本地文件
        os.remove(zip_file)
        
    except Exception as e:
        print(f"❌ 部署失败: {e}")
        return False
    
    return True

if __name__ == '__main__':
    quick_start()
