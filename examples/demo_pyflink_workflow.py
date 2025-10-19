#!/usr/bin/env python3
"""
AWS Managed Flink PyFlink 作业完整演示
这个脚本演示了如何创建、部署、管理和监控PyFlink作业
"""

import sys
import time
import os

# Add src directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from flink_manager import FlinkManager

def demo_workflow():
    print("🚀 AWS Managed Flink PyFlink 作业完整演示")
    print("="*60)
    
    # 配置
    APP_NAME = 'pyflink-demo-app'
    S3_BUCKET = 'pcd-01'
    S3_KEY = 'flink-jobs/pyflink-demo.zip'
    S3_BUCKET_ARN = f'arn:aws:s3:::{S3_BUCKET}'
    
    # 获取源文件路径
    src_dir = os.path.join(os.path.dirname(__file__), '..', 'src')
    job_file = os.path.join(src_dir, 'pyflink_sql_job.py')
    requirements_file = os.path.join(src_dir, 'requirements.txt')
    
    # 初始化管理器
    manager = FlinkManager()
    
    try:
        # 步骤1: IAM角色会在创建应用程序时自动检查和创建
        print("\n📋 步骤1: 检查/创建IAM服务角色...")
        role_arn = manager.ensure_service_role()
        if role_arn:
            print(f"✅ IAM角色准备就绪: {role_arn}")
        else:
            print("❌ IAM角色创建失败")
            return False
        
        # 步骤2: 创建Python应用程序包
        print("\n📦 步骤2: 创建Python应用程序ZIP包...")
        zip_file = manager.create_python_application_zip(
            job_file, 
            requirements_file, 
            'pyflink-demo.zip'
        )
        print(f"✅ 创建ZIP包: {zip_file}")
        
        # 步骤3: 上传到S3
        print("\n☁️ 步骤3: 上传到S3...")
        s3_url = manager.upload_to_s3(zip_file, S3_BUCKET, S3_KEY)
        if s3_url:
            print(f"✅ 上传成功: {s3_url}")
        else:
            print("❌ S3上传失败")
            return False
        
        # 步骤4: 创建Flink应用程序
        print("\n🔧 步骤4: 创建AWS Managed Flink应用程序...")
        create_response = manager.create_application(APP_NAME, S3_BUCKET_ARN, S3_KEY)
        if create_response:
            print(f"✅ 应用程序创建成功: {APP_NAME}")
        else:
            print("❌ 应用程序创建失败")
            return False
        
        # 步骤5: 等待应用程序就绪
        print("\n⏳ 步骤5: 等待应用程序就绪...")
        max_wait = 180
        wait_time = 0
        while wait_time < max_wait:
            status = manager.get_application_status(APP_NAME)
            if status == 'READY':
                print("✅ 应用程序已就绪")
                break
            time.sleep(5)
            wait_time += 5
        
        if wait_time >= max_wait:
            print("❌ 等待应用程序就绪超时")
            return False
        
        # 步骤6: 启动应用程序
        print("\n🚀 步骤6: 启动Flink应用程序...")
        start_response = manager.start_application(APP_NAME)
        if start_response:
            print("✅ 应用程序启动命令已发送")
        else:
            print("❌ 应用程序启动失败")
            return False
        
        # 步骤7: 监控应用程序状态
        print("\n📊 步骤7: 监控应用程序状态...")
        max_wait = 300
        wait_time = 0
        while wait_time < max_wait:
            status = manager.get_application_status(APP_NAME)
            if status == 'RUNNING':
                print("✅ 应用程序正在运行!")
                break
            elif status == 'STARTING':
                print("⏳ 应用程序正在启动中...")
            time.sleep(10)
            wait_time += 10
        
        if wait_time >= max_wait:
            print("❌ 应用程序启动超时")
            return False
        
        # 步骤8: 让应用程序运行一段时间
        print("\n⏰ 步骤8: 让应用程序运行60秒...")
        for i in range(6):
            time.sleep(10)
            status = manager.get_application_status(APP_NAME)
            print(f"   状态检查 {i+1}/6: {status}")
        
        # 步骤9: 演示应用程序管理功能
        print("\n🔧 步骤9: 演示应用程序管理功能...")
        
        # 获取详细信息
        details = manager.get_application_details(APP_NAME)
        if details:
            print(f"   应用程序版本: {details.get('ApplicationVersionId')}")
            print(f"   运行时环境: {details.get('RuntimeEnvironment')}")
            print(f"   应用程序模式: {details.get('ApplicationMode')}")
        
        # 步骤10: 停止应用程序
        print("\n🛑 步骤10: 停止应用程序...")
        stop_response = manager.stop_application(APP_NAME)
        if stop_response:
            print("✅ 应用程序停止命令已发送")
            
            # 等待停止
            print("   等待应用程序停止...")
            max_wait = 120
            wait_time = 0
            while wait_time < max_wait:
                status = manager.get_application_status(APP_NAME)
                if status == 'READY':
                    print("✅ 应用程序已停止")
                    break
                time.sleep(5)
                wait_time += 5
        
        # 步骤11: 清理资源
        print("\n🧹 步骤11: 清理资源...")
        delete_response = manager.delete_application(APP_NAME)
        if delete_response:
            print("✅ 应用程序已删除")
        
        # 清理本地文件
        os.remove(zip_file)
        print("✅ 本地文件已清理")
        
        # 最终总结
        print("\n" + "="*60)
        print("🎉 PyFlink on AWS Managed Flink 演示完成!")
        print("="*60)
        print("✅ 所有步骤都成功完成:")
        print("   • IAM角色自动管理")
        print("   • Python应用程序打包")
        print("   • S3上传")
        print("   • Flink应用程序创建")
        print("   • 应用程序启动和运行")
        print("   • 状态监控")
        print("   • 应用程序管理")
        print("   • 资源清理")
        print("\n🚀 您现在可以使用这些脚本来管理您自己的PyFlink作业!")
        
        return True
        
    except Exception as e:
        print(f"\n❌ 演示过程中出现错误: {e}")
        import traceback
        traceback.print_exc()
        
        # 尝试清理
        try:
            print("\n🧹 尝试清理资源...")
            manager.delete_application(APP_NAME)
        except:
            pass
        
        return False

def show_usage_examples():
    """显示使用示例"""
    print("\n📚 使用示例:")
    print("="*40)
    print("# 导入FlinkManager")
    print("from src.flink_manager import FlinkManager")
    print("manager = FlinkManager()")
    print("")
    print("# 创建应用程序 (IAM角色会自动创建)")
    print("manager.create_application('my-app', 's3-bucket-arn', 's3-key')")
    print("")
    print("# 启动应用程序")
    print("manager.start_application('my-app')")
    print("")
    print("# 检查状态")
    print("manager.get_application_status('my-app')")
    print("")
    print("# 停止应用程序")
    print("manager.stop_application('my-app')")
    print("")
    print("# 删除应用程序")
    print("manager.delete_application('my-app')")

if __name__ == '__main__':
    print("AWS Managed Flink PyFlink 作业管理演示")
    print("作者: Amazon Q")
    print("日期:", time.strftime("%Y-%m-%d %H:%M:%S"))
    
    success = demo_workflow()
    
    if success:
        show_usage_examples()
        print("\n✅ 演示成功完成!")
        sys.exit(0)
    else:
        print("\n❌ 演示失败!")
        sys.exit(1)
