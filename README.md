# AWS Managed Flink PyFlink 作业管理工具

这个项目提供了一套完整的工具来创建、部署、管理和监控AWS Managed Flink上的PyFlink作业。

## 🚀 功能特性

- ✅ **PyFlink SQL作业**: 创建基于SQL的流处理作业
- ✅ **自动化部署**: 使用boto3自动化应用程序生命周期管理
- ✅ **IAM角色自动管理**: 自动检查和创建所需的服务角色
- ✅ **S3集成**: 自动上传应用程序包到S3
- ✅ **状态监控**: 实时监控应用程序状态
- ✅ **完整生命周期**: 创建、启动、更新、停止、删除应用程序

## 📁 项目结构

```
q-msf/
├── src/                        # 源代码目录
│   ├── flink_manager.py       # Flink应用程序管理器 (核心组件)
│   ├── pyflink_sql_job.py     # PyFlink SQL作业示例
│   └── requirements.txt       # Python依赖
├── examples/                   # 示例和演示脚本
│   ├── demo_pyflink_workflow.py  # 完整演示脚本
│   └── quick_start.py         # 快速开始脚本
├── docs/                      # 文档目录 (预留)
└── README.md                  # 项目文档
```

### 文件说明

#### 核心组件 (`src/`)
- **`flink_manager.py`**: 核心管理类，包含所有Flink应用程序管理功能和IAM角色管理
- **`pyflink_sql_job.py`**: PyFlink SQL作业示例，演示数据生成、窗口聚合和输出
- **`requirements.txt`**: PyFlink依赖定义

#### 示例脚本 (`examples/`)
- **`demo_pyflink_workflow.py`**: 完整的演示脚本，展示从创建到删除的完整生命周期
- **`quick_start.py`**: 快速开始脚本，最简单的部署示例

## 🛠️ 安装和配置

### 1. 安装依赖

```bash
pip install boto3
```

### 2. 配置AWS凭证

确保您的AWS凭证已正确配置：

```bash
aws configure
```

### 3. 配置S3存储桶

修改脚本中的S3存储桶名称（默认: `pcd-01`）为您有权访问的存储桶。

## 📖 使用指南

### 快速开始

1. **运行完整演示**：
```bash
cd examples
python demo_pyflink_workflow.py
```

2. **快速部署**：
```bash
cd examples
python quick_start.py
```

### 编程方式使用

```python
import sys
import os
sys.path.append('src')

from flink_manager import FlinkManager

# 初始化管理器
manager = FlinkManager(region='us-east-1')

# 创建应用程序包
zip_file = manager.create_python_application_zip(
    'src/pyflink_sql_job.py', 
    'src/requirements.txt'
)

# 上传到S3
s3_url = manager.upload_to_s3(zip_file, 'your-bucket', 'flink-jobs/my-job.zip')

# 创建Flink应用程序 (IAM角色会自动创建)
manager.create_application(
    'my-app', 
    'arn:aws:s3:::your-bucket', 
    'flink-jobs/my-job.zip'
)

# 启动应用程序
manager.start_application('my-app')

# 检查状态
status = manager.get_application_status('my-app')

# 停止应用程序
manager.stop_application('my-app')

# 删除应用程序
manager.delete_application('my-app')
```

## 📋 核心功能

### FlinkManager 类方法

#### IAM管理
- `ensure_service_role()`: 自动检查和创建IAM服务角色

#### 应用程序包管理
- `create_python_application_zip()`: 创建Python应用程序ZIP包
- `upload_to_s3()`: 上传文件到S3

#### 应用程序生命周期管理
- `create_application()`: 创建Flink应用程序
- `start_application()`: 启动应用程序
- `stop_application()`: 停止应用程序
- `delete_application()`: 删除应用程序

#### 监控和管理
- `get_application_status()`: 获取应用程序状态
- `get_application_details()`: 获取详细应用程序信息
- `update_application()`: 更新应用程序代码

## 🔧 配置说明

### 应用程序配置

Python应用程序使用`EnvironmentProperties`指定文件位置：

```json
{
  "EnvironmentProperties": {
    "PropertyGroups": [
      {
        "PropertyGroupId": "kinesis.analytics.flink.run.options",
        "PropertyMap": {
          "python": "pyflink_sql_job.py"
        }
      }
    ]
  }
}
```

### 运行时环境

- **运行时**: FLINK-1_20
- **应用程序模式**: STREAMING
- **检查点**: 默认配置
- **监控**: 默认配置
- **并行度**: 默认配置

### IAM权限

自动创建的IAM角色包含以下权限：
- S3对象读取权限
- CloudWatch日志权限

## 📊 监控和日志

应用程序运行时，您可以：

1. **查看状态**: 使用`get_application_status()`
2. **CloudWatch日志**: 在AWS控制台查看应用程序日志
3. **CloudWatch指标**: 监控应用程序性能指标

## 🚨 故障排除

### 常见问题

1. **IAM权限不足**
   - IAM角色会自动创建，确保您有创建IAM角色的权限
   - 检查S3存储桶访问权限

2. **应用程序启动失败**
   - 检查Python代码语法
   - 验证requirements.txt中的依赖

3. **S3上传失败**
   - 确保存储桶存在且可访问
   - 检查AWS凭证配置

### 调试技巧

```python
# 获取详细应用程序信息
details = manager.get_application_details('my-app')
print(f"状态: {details['ApplicationStatus']}")
print(f"版本: {details['ApplicationVersionId']}")
print(f"运行时: {details['RuntimeEnvironment']}")
```

## 🔄 应用程序生命周期

```
创建 → 就绪 → 启动中 → 运行中 → 停止中 → 就绪 → 删除
  ↓      ↓       ↓        ↓        ↓       ↓      ↓
READY  READY  STARTING RUNNING STOPPING READY DELETED
```

## 📝 示例PyFlink作业

项目包含一个示例PyFlink SQL作业 (`src/pyflink_sql_job.py`)，演示：

- 数据生成（datagen连接器）
- 窗口聚合查询（TUMBLE窗口）
- 数据过滤
- 结果输出（print连接器）

## 🤝 贡献

欢迎提交问题和改进建议！

## 📄 许可证

本项目基于MIT许可证开源。

## 🙏 致谢

- AWS Managed Flink团队
- Apache Flink社区
- PyFlink开发者

---

**注意**: 这个项目是为了演示AWS Managed Flink与PyFlink的集成。在生产环境中使用前，请根据您的具体需求进行适当的修改和测试。
