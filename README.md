# AWS Managed Flink PyFlink 作业例子

### 版本
* MSF Flink 1.20
* Iceberg 1.9.0
* Doris 3.1.3 (Flink Doris Connector 25.1.0)

### 项目结构

```
msf-pyflink-job/
├── pom.xml                         # Maven 配置 (支持多 Profile)
├── assembly/
│   └── assembly.xml                # Maven Assembly 打包配置
│
├── src/                            # Flink 作业代码 (本地 + MSF 通用)
│   ├── main-kafka-iceberg.py       # Kafka → Iceberg
│   ├── main-simple.py              # 简单测试
│   ├── main-kafka-s3.py            # Kafka → S3
│   ├── main-mysql.py               # Kafka JSON → MySQL
│   ├── main-debezium-mysql.py      # Debezium CDC → MySQL
│   ├── main-debezium-iceberg.py    # Debezium CDC → Iceberg
│   ├── main-debezium-doris.py      # Debezium CDC → Doris
│   ├── flink_manager.py            # Flink 管理工具类
│   └── main/java/.../HadoopUtils.java  # Hadoop 配置工具类
│
├── setup_database.py               # 创建 Glue Database + LakeFormation 权限
├── quick_start.py                  # 快速创建 MSF 应用
│
└── target/                         # 编译输出目录
    ├── pyflink-dependencies.jar    # 依赖 JAR
    └── msf-pyflink-*.zip           # MSF 部署包
```

### 文件说明

#### 工具脚本

| 文件 | 说明 |
|------|------|
| `setup_database.py` | 创建 Glue Database 和 LakeFormation 权限 |
| `quick_start.py` | 快速创建 MSF 应用并上传代码 (自动创建 IAM Role) |

#### Flink 作业脚本 (src/)

所有脚本支持**本地运行**和 **MSF 部署**，自动检测运行环境。本地运行时自动启用 Flink Web UI (http://localhost:8081)。

| 文件 | 说明 | 依赖 Profile |
|------|------|--------------|
| `main-simple.py` | 简单测试，无外部依赖 | - |
| `main-kafka-iceberg.py` | Kafka JSON 聚合写入 Iceberg | `iceberg` |
| `main-kafka-s3.py` | Kafka JSON 写入 S3 | `kafka-s3` |
| `main-mysql.py` | Kafka JSON 写入 MySQL | `mysql` |
| `main-debezium-mysql.py` | Debezium CDC 写入 MySQL | `mysql` |
| `main-debezium-iceberg.py` | Debezium CDC 写入 Iceberg | `debezium` |
| `main-debezium-doris.py` | Debezium CDC 写入 Doris | `doris` |

### 功能列表

| 功能 | Python 文件 | Maven Profile | 说明 |
|------|-------------|---------------|------|
| Simple Test | `src/main-simple.py` | - | 简单测试 |
| Kafka → S3 | `src/main-kafka-s3.py` | `kafka-s3` | Kafka 数据写入 S3 |
| Kafka → Iceberg | `src/main-kafka-iceberg.py` | `iceberg` (默认) | Kafka 数据写入 Iceberg |
| Kafka → MySQL | `src/main-mysql.py` | `mysql` | Kafka JSON 数据写入 MySQL |
| Debezium CDC → MySQL | `src/main-debezium-mysql.py` | `mysql` | Debezium CDC 写入 MySQL |
| Debezium CDC → Iceberg | `src/main-debezium-iceberg.py` | `debezium` | Debezium CDC 写入 Iceberg |
| Debezium CDC → Doris | `src/main-debezium-doris.py` | `doris` | Debezium CDC 写入 Doris |

### 依赖编译

```bash
# 环境要求: JDK 11, Maven 3.9.x
git clone https://github.com/yhyyz/msf-pyflink-job.git
cd msf-pyflink-job

# Kafka → Iceberg (默认)
mvn clean package

# Kafka → S3
mvn clean package -P kafka-s3

# Kafka → MySQL / Debezium CDC → MySQL
mvn clean package -P mysql

# Debezium CDC → Iceberg (包含所有依赖)
mvn clean package -P debezium

# Debezium CDC → Doris
mvn clean package -P doris
```

编译输出：

| Profile | 输出文件 | 大小 |
|---------|----------|------|
| `kafka-s3` | `target/msf-pyflink-kafka-s3-1.0.0.zip` | ~54M |
| `mysql` | `target/msf-pyflink-mysql-1.0.0.zip` | ~55M |
| `iceberg` | `target/msf-pyflink-iceberg-1.0.0.zip` | ~143M |
| `debezium` | `target/msf-pyflink-debezium-1.0.0.zip` | ~146M |
| `doris` | `target/msf-pyflink-doris-1.0.0.zip` | ~86M |

### MSF 作业配置

#### 运行时配置 (kinesis.analytics.flink.run.options)

| Key | Value | 说明 |
|-----|-------|------|
| `python` | `main-kafka-iceberg.py` | Python 入口文件 |
| `jarfile` | `lib/pyflink-dependencies.jar` | 依赖 JAR 路径 |

#### 应用属性 (FlinkApplicationProperties)

通过 `quick_start.py` 部署时自动配置，或在 MSF 控制台手动添加：

**Kafka 配置**：
| Key | Value | 说明 |
|-----|-------|------|
| `kafka.bootstrap` | `your-kafka:9092` | Kafka bootstrap servers |
| `kafka.topic` | `test` | Kafka topic |

**Iceberg 配置** (main-kafka-iceberg.py, main-debezium-iceberg.py)：
| Key | Value | 说明 |
|-----|-------|------|
| `iceberg.warehouse` | `s3://bucket/warehouse/` | Iceberg warehouse 路径 |
| `iceberg.database` | `test_iceberg_db` | Iceberg 数据库名 |
| `iceberg.table` | `kafka_agg_sink` | Iceberg 表名 |
| `aws.region` | `us-east-1` | AWS 区域 |

**MySQL 配置** (main-mysql.py, main-debezium-mysql.py)：
| Key | Value | 说明 |
|-----|-------|------|
| `mysql.host` | `your-host.rds.amazonaws.com` | MySQL 主机 |
| `mysql.port` | `3306` | MySQL 端口 |
| `mysql.database` | `test_db` | MySQL 数据库 |
| `mysql.table` | `kafka_sink_data` | MySQL 表名 |
| `mysql.user` | `admin` | MySQL 用户名 |
| `mysql.password` | `xxx` | MySQL 密码 |

**S3 配置** (main-kafka-s3.py)：
| Key | Value | 说明 |
|-----|-------|------|
| `s3.output.path` | `s3://bucket/output/` | S3 输出路径 |

**Doris 配置** (main-debezium-doris.py)：
| Key | Value | 说明 |
|-----|-------|------|
| `doris.fenodes` | `10.0.0.10:8030` | Doris FE HTTP 地址 |
| `doris.database` | `test_db` | Doris 数据库名 |
| `doris.table` | `cdc_sync_doris` | Doris 表名 |
| `doris.user` | `root` | Doris 用户名 |
| `doris.password` | `` | Doris 密码 |

**Flink 配置** (所有作业通用)：
| Key | Value | 说明 |
|-----|-------|------|
| `flink.operator-chaining.enabled` | `true` / `false` | 是否启用 operator chaining (默认 true) |

### 前置条件设置

使用 `setup_database.py` 创建 Glue Database（IAM Role 由 `quick_start.py` 自动创建）：

```bash
# 安装依赖
pip install boto3

# 创建 Glue Database
python setup_database.py --region us-east-1 --s3-bucket your-bucket

# 自定义数据库名
python setup_database.py --region us-east-1 --s3-bucket your-bucket --database my_iceberg_db

# 查看帮助
python setup_database.py --help
```

脚本会创建：
- Glue Database (已配置 IAM_ALLOWED_PRINCIPALS 权限)

IAM Role 由 `quick_start.py` → `flink_manager.py` 自动创建，包含：
- S3、Glue、LakeFormation、CloudWatch、VPC 权限

### 快速开始

```bash
# 1. 编译
mvn clean package

# 2. 创建 Glue Database (一次性)
python setup_database.py --region us-east-1 --s3-bucket your-bucket

# 3. 部署 MSF 应用 (使用 MSK cluster name 自动获取网络配置)
python quick_start.py \
  --app_name flink-msk-iceberg-demo \
  --msk_cluster_name msk-log-stream \
  --kafka_topic your-topic \
  --iceberg_warehouse s3://your-bucket/iceberg-warehouse/ \
  --iceberg_database test_iceberg_db \
  --iceberg_table kafka_agg_sink

# 或者手动指定网络配置
python quick_start.py \
  --app_name flink-msk-iceberg-demo \
  --subnet_id subnet-xxx \
  --sg_id sg-xxx \
  --kafka_bootstrap your-kafka:9092 \
  --kafka_topic your-topic \
  ...

# 4. 部署其他类型作业
# Debezium CDC → Iceberg
python quick_start.py \
  --app_name flink-cdc-iceberg \
  --python_main main-debezium-iceberg.py \
  --local_dep_jar_path target/msf-pyflink-debezium-1.0.0.zip \
  --kafka_topic cdc-topic \
  ...

# Kafka → MySQL
python quick_start.py \
  --app_name flink-kafka-mysql \
  --python_main main-mysql.py \
  --local_dep_jar_path target/msf-pyflink-mysql-1.0.0.zip \
  --mysql_host your-mysql.rds.amazonaws.com \
  --mysql_database test_db \
  --mysql_table kafka_sink \
  --mysql_user admin \
  --mysql_password xxx \
  ...

# Debezium CDC → Doris
python quick_start.py \
  --app_name flink-cdc-doris \
  --python_main main-debezium-doris.py \
  --local_dep_jar_path target/msf-pyflink-doris-1.0.0.zip \
  --kafka_topic cdc-topic \
  --doris_fenodes 10.0.0.10:8030 \
  --doris_database test_db \
  --doris_table cdc_sync_doris \
  --doris_user root \
  ...

# 查看所有参数
python quick_start.py --help

# 删除应用
python quick_start.py --app_name flink-cdc-doris --delete

# 禁用 operator chaining (MSF 部署)
python quick_start.py \
  --app_name flink-cdc-doris \
  --python_main main-debezium-doris.py \
  --local_dep_jar_path target/msf-pyflink-doris-1.0.0.zip \
  --disable_operator_chaining
```

`--msk_cluster_name` 会自动获取：
- Kafka bootstrap servers
- 私有子网 ID
- 安全组 ID

如果同时提供了 `--subnet_id`、`--sg_id`、`--kafka_bootstrap`，则优先使用手动提供的值。

### 本地调试

所有 `src/main-*.py` 文件支持本地直接运行，自动检测运行环境：
- **本地运行**：使用命令行参数（带默认值），自动启用 Flink Web UI
- **MSF 运行**：从 `/etc/flink/application_properties.json` 读取配置

```bash
# 创建虚拟环境
uv venv -p 3.11
source .venv/bin/activate

# 安装依赖
uv pip install boto3 apache-flink==1.20.0 setuptools

# 编译 (根据需要选择 profile)
mvn clean package -P iceberg    # 或 mysql, debezium, doris

# 后续运行时激活环境
source .venv/bin/activate
```

> **注意**: 本项目使用 `.venv` 目录下的虚拟环境，运行任何 Python 脚本前需先执行 `source .venv/bin/activate`

#### 运行示例

```bash
# Kafka → Iceberg (查看帮助)
python src/main-kafka-iceberg.py --help

# 使用默认参数运行
python src/main-kafka-iceberg.py

# 自定义参数
python src/main-kafka-iceberg.py \
  --kafka-bootstrap your-kafka:9092 \
  --kafka-topic your-topic \
  --iceberg-warehouse s3://your-bucket/warehouse/ \
  --iceberg-database test_iceberg_db \
  --iceberg-table my_table \
  --aws-region us-east-1

# Kafka → MySQL
python src/main-mysql.py \
  --kafka-bootstrap your-kafka:9092 \
  --kafka-topic your-topic \
  --mysql-host your-mysql.rds.amazonaws.com \
  --mysql-database test_db \
  --mysql-table kafka_sink \
  --mysql-user admin \
  --mysql-password xxx

# Debezium CDC → Iceberg
python src/main-debezium-iceberg.py \
  --kafka-bootstrap your-kafka:9092 \
  --kafka-topic cdc-topic \
  --iceberg-warehouse s3://your-bucket/warehouse/ \
  --iceberg-database test_iceberg_db \
  --iceberg-table cdc_sync \
  --aws-region us-east-1

# Debezium CDC → MySQL
python src/main-debezium-mysql.py \
  --kafka-bootstrap your-kafka:9092 \
  --kafka-topic cdc-topic \
  --mysql-host your-mysql.rds.amazonaws.com \
  --mysql-database test_db \
  --mysql-table cdc_sync \
  --mysql-user admin \
  --mysql-password xxx

# Debezium CDC → Doris
python src/main-debezium-doris.py \
  --kafka-bootstrap your-kafka:9092 \
  --kafka-topic cdc-topic \
  --doris-fenodes 10.0.0.10:8030 \
  --doris-database test_db \
  --doris-table cdc_sync_doris \
  --doris-user root

# 禁用 operator chaining (所有作业通用)
python src/main-kafka-iceberg.py --disable-operator-chaining
```

#### Flink Web UI

本地运行时自动启用，访问 http://localhost:8081 查看作业状态

日志目录: `.venv/lib/python3.11/site-packages/pyflink/log/`

### 注意事项

#### 权限相关
1. Python 代码错误会导致 `CodeError.InvalidApplicationCode`
2. IAM Role 需要 Glue、S3、LakeFormation 权限
3. LakeFormation 权限问题需要给 database 添加 `IAM_ALLOWED_PRINCIPALS` 的 ALL 权限

#### 依赖相关
1. PyFlink 依赖 JAR 需要打包到 zip 中
2. UDF 需要的 Python 库可通过 `requirements.txt` 添加，参考 [AWS 示例](https://github.com/aws-samples/amazon-managed-service-for-apache-flink-examples/tree/main/python/PythonDependencies)

#### Doris 相关
1. Doris 目标表必须使用 **UNIQUE KEY** 模型才能支持 CDC 的 upsert/delete 操作
2. 需要提前在 Doris 中创建目标表：

```sql
-- 在 Doris 中创建目标表
CREATE DATABASE IF NOT EXISTS test_db;

CREATE TABLE test_db.cdc_sync_doris (
    id BIGINT,
    name VARCHAR(256),
    email VARCHAR(256),
    created_at VARCHAR(64)
)
UNIQUE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 3
PROPERTIES (
    "replication_allocation" = "tag.location.default: 1"
);
```

3. Doris FE HTTP 端口默认 8030，Query 端口默认 9030

### Debezium CDC 数据格式

```json
{
  "before": null,
  "after": {
    "id": 1,
    "username": "user1",
    "email": "user1@example.com"
  },
  "op": "c",
  "ts_ms": 1704067200000,
  "source": {
    "db": "test_db",
    "table": "users"
  }
}
```

| op | 含义 |
|----|------|
| `c` | CREATE (插入) |
| `u` | UPDATE (更新) |
| `d` | DELETE (删除) |
| `r` | READ (快照) |

### 截图

#### MSF 作业配置
![](https://pcmyp.oss-cn-beijing.aliyuncs.com/markdown/202510220029587.png)
![](https://pcmyp.oss-cn-beijing.aliyuncs.com/markdown/202510220030317.png)
![](https://pcmyp.oss-cn-beijing.aliyuncs.com/markdown/202510220031134.png)

#### 本地模式
![](https://pcmyp.oss-cn-beijing.aliyuncs.com/markdown/202510220122940.png)
