# AWS Managed Flink PyFlink 作业例子

### 版本
* MSF Flink 1.20
* Iceberg 1.9.0
* Doris 3.1.3 (Flink Doris Connector 25.1.0)
* Java 11 (Amazon Corretto)
* Maven 3.9+
* Python 3.11.x (PyFlink 1.20 不支持 3.12+)

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

#### 环境安装 (Amazon Linux 2023 / EC2)

```bash
# Java 11 (Amazon Corretto)
sudo yum install -y java-11-amazon-corretto-devel

# Maven 3.9.9
cd /tmp && curl -sL https://archive.apache.org/dist/maven/maven-3/3.9.9/binaries/apache-maven-3.9.9-bin.tar.gz | sudo tar -xz -C /opt && sudo ln -sf /opt/apache-maven-3.9.9/bin/mvn /usr/local/bin/mvn

# 验证
java -version
mvn -version
```

#### 编译打包

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
# 0. 安装 Python 依赖
uv sync
source .venv/bin/activate

# 1. 编译 (根据需要选择 profile)
mvn clean package -P iceberg    # Kafka → Iceberg
mvn clean package -P debezium   # Debezium CDC → Iceberg
mvn clean package -P mysql      # Kafka → MySQL, Debezium CDC → MySQL
mvn clean package -P doris      # Debezium CDC → Doris
mvn clean package -P kafka-s3   # Kafka → S3

# 2. 创建 Glue Database (Iceberg 作业需要，一次性)
python setup_database.py --region us-east-1 --s3-bucket your-bucket

# 3. 部署 MSF 应用

# Kafka → Iceberg
python quick_start.py \
  --app_name flink-kafka-iceberg \
  --python_main main-kafka-iceberg.py \
  --local_dep_jar_path target/msf-pyflink-iceberg-1.0.0.zip \
  --s3_key flink-jobs/flink-kafka-iceberg.zip \
  --msk_cluster_name msk-log-stream \
  --kafka_topic test \
  --iceberg_warehouse s3://your-bucket/iceberg-warehouse/ \
  --iceberg_database test_iceberg_db \
  --iceberg_table kafka_agg_sink \
  --aws_region us-east-1

# Debezium CDC → Iceberg
python quick_start.py \
  --app_name flink-cdc-iceberg \
  --python_main main-debezium-iceberg.py \
  --local_dep_jar_path target/msf-pyflink-debezium-1.0.0.zip \
  --s3_key flink-jobs/flink-cdc-iceberg.zip \
  --msk_cluster_name msk-log-stream \
  --kafka_topic cdc-topic \
  --iceberg_warehouse s3://your-bucket/iceberg-warehouse/ \
  --iceberg_database test_iceberg_db \
  --iceberg_table cdc_sync_iceberg \
  --aws_region us-east-1

# Kafka → MySQL
python quick_start.py \
  --app_name flink-kafka-mysql \
  --python_main main-mysql.py \
  --local_dep_jar_path target/msf-pyflink-mysql-1.0.0.zip \
  --s3_key flink-jobs/flink-kafka-mysql.zip \
  --msk_cluster_name msk-log-stream \
  --kafka_topic test \
  --mysql_host your-mysql.rds.amazonaws.com \
  --mysql_port 3306 \
  --mysql_database test_db \
  --mysql_table kafka_sink_data \
  --mysql_user admin \
  --mysql_password your-password

# Debezium CDC → MySQL
python quick_start.py \
  --app_name flink-cdc-mysql \
  --python_main main-debezium-mysql.py \
  --local_dep_jar_path target/msf-pyflink-mysql-1.0.0.zip \
  --s3_key flink-jobs/flink-cdc-mysql.zip \
  --msk_cluster_name msk-log-stream \
  --kafka_topic cdc-topic \
  --mysql_host your-mysql.rds.amazonaws.com \
  --mysql_port 3306 \
  --mysql_database test_db \
  --mysql_table cdc_sync \
  --mysql_user admin \
  --mysql_password your-password

# Debezium CDC → Doris
python quick_start.py \
  --app_name flink-cdc-doris \
  --python_main main-debezium-doris.py \
  --local_dep_jar_path target/msf-pyflink-doris-1.0.0.zip \
  --s3_key flink-jobs/flink-cdc-doris.zip \
  --msk_cluster_name msk-log-stream \
  --kafka_topic cdc-topic \
  --doris_fenodes 10.0.0.10:8030 \
  --doris_database test_db \
  --doris_table cdc_sync_doris \
  --doris_user root \
  --doris_password ""

# Kafka → S3
python quick_start.py \
  --app_name flink-kafka-s3 \
  --python_main main-kafka-s3.py \
  --local_dep_jar_path target/msf-pyflink-kafka-s3-1.0.0.zip \
  --s3_key flink-jobs/flink-kafka-s3.zip \
  --msk_cluster_name msk-log-stream \
  --kafka_topic test \
  --s3_output_path s3://your-bucket/flink-output/

# 4. 其他操作

# 查看所有参数
python quick_start.py --help

# 删除应用
python quick_start.py --app_name flink-cdc-doris --delete

# 禁用 operator chaining
python quick_start.py \
  --app_name flink-cdc-doris \
  --python_main main-debezium-doris.py \
  --local_dep_jar_path target/msf-pyflink-doris-1.0.0.zip \
  --s3_key flink-jobs/flink-cdc-doris.zip \
  --disable_operator_chaining
```

`--msk_cluster_name` 会自动获取：
- Kafka bootstrap servers
- 私有子网 ID
- 安全组 ID

如果同时提供了 `--subnet_id`、`--sg_id`、`--kafka_bootstrap`，则优先使用手动提供的值。

#### quick_start.py 参数说明

| 参数 | 必填 | 默认值 | 说明 |
|------|------|--------|------|
| **基础配置** | | | |
| `--app_name` | 否 | `flink-msk-iceberg-sink-demo` | MSF 应用名称 |
| `--s3_bucket` | ✅ 是 | - | S3 存储桶 (eg. your-bucket-name) |
| `--s3_key` | 否 | `flink-jobs/flink-msk-iceberg-sink-demo.zip` | S3 对象路径 |
| `--msk_cluster_name` | ✅ 是 | - | MSK 集群名称 (eg. msk-log-stream) |
| `--aws_region` | ✅ 是 | - | AWS 区域 (eg. us-east-1) |
| `--local_dep_jar_path` | 否 | `target/msf-pyflink-iceberg-1.0.0.zip` | 本地 zip 包路径 |
| `--python_main` | 否 | `main-kafka-iceberg.py` | Python 入口文件 |
| **Kafka** | | | |
| `--kafka_bootstrap` | 否 | - | 从 MSK 自动获取 |
| `--kafka_topic` | ✅ 是 | - | Kafka topic (eg. test) |
| **Iceberg** | | | |
| `--iceberg_warehouse` | ✅ Iceberg 作业 | - | Iceberg warehouse 路径 (eg. s3://your-bucket/iceberg-warehouse/) |
| `--iceberg_database` | 否 | `test_iceberg_db` | Iceberg 数据库名 |
| `--iceberg_table` | 否 | `kafka_agg_sink` | Iceberg 表名 |
| **S3 Sink** | | | |
| `--s3_output_path` | ✅ S3 作业 | - | S3 输出路径 (eg. s3://your-bucket/flink-output/) |
| **MySQL** | | | |
| `--mysql_host` | ✅ MySQL 作业 | - | MySQL 主机 (eg. your-mysql.rds.amazonaws.com) |
| `--mysql_port` | 否 | `3306` | MySQL 端口 |
| `--mysql_database` | 否 | `test_db` | MySQL 数据库 |
| `--mysql_table` | 否 | `kafka_sink_data` | MySQL 表名 |
| `--mysql_user` | ✅ MySQL 作业 | - | MySQL 用户名 (eg. admin) |
| `--mysql_password` | 否 | `""` | MySQL 密码（允许空） |
| **Doris** | | | |
| `--doris_fenodes` | ✅ Doris 作业 | - | Doris FE HTTP 地址 (eg. 10.0.0.10:8030) |
| `--doris_database` | 否 | `test_db` | Doris 数据库 |
| `--doris_table` | 否 | `cdc_sync_doris` | Doris 表名 |
| `--doris_user` | 否 | `root` | Doris 用户名 |
| `--doris_password` | 否 | `""` | Doris 密码 |
| **Flink** | | | |
| `--disable_operator_chaining` | 否 | `false` | 禁用 operator chaining |

### 本地调试

所有 `src/main-*.py` 文件支持本地直接运行，自动检测运行环境：
- **本地运行**：使用命令行参数（带默认值），自动启用 Flink Web UI
- **MSF 运行**：从 `/etc/flink/application_properties.json` 读取配置

```bash
# 安装依赖 (使用 uv，推荐)
uv sync

# 或者手动创建虚拟环境
uv venv -p 3.11
source .venv/bin/activate
uv pip install -e .

# 编译 (根据需要选择 profile)
mvn clean package -P iceberg    # 或 mysql, debezium, doris
```

> **注意**: 本项目使用 `.venv` 目录下的虚拟环境，运行任何 Python 脚本前需先执行 `source .venv/bin/activate`

#### 运行示例

```bash
# 查看帮助
python src/main-kafka-iceberg.py --help

# Kafka → Iceberg
python src/main-kafka-iceberg.py \
  --kafka-bootstrap your-kafka:9092 \
  --kafka-topic test \
  --iceberg-warehouse s3://your-bucket/iceberg-warehouse/ \
  --iceberg-database test_iceberg_db \
  --iceberg-table kafka_agg_sink \
  --aws-region us-east-1

# Debezium CDC → Iceberg
python src/main-debezium-iceberg.py \
  --kafka-bootstrap your-kafka:9092 \
  --kafka-topic cdc-topic \
  --iceberg-warehouse s3://your-bucket/iceberg-warehouse/ \
  --iceberg-database test_iceberg_db \
  --iceberg-table cdc_sync_iceberg \
  --aws-region us-east-1

# Kafka → MySQL
python src/main-mysql.py \
  --kafka-bootstrap your-kafka:9092 \
  --kafka-topic test \
  --mysql-host your-mysql.rds.amazonaws.com \
  --mysql-port 3306 \
  --mysql-database test_db \
  --mysql-table kafka_sink_data \
  --mysql-user admin \
  --mysql-password your-password

# Debezium CDC → MySQL
python src/main-debezium-mysql.py \
  --kafka-bootstrap your-kafka:9092 \
  --kafka-topic cdc-topic \
  --mysql-host your-mysql.rds.amazonaws.com \
  --mysql-port 3306 \
  --mysql-database test_db \
  --mysql-table cdc_sync \
  --mysql-user admin \
  --mysql-password your-password

# Debezium CDC → Doris
python src/main-debezium-doris.py \
  --kafka-bootstrap your-kafka:9092 \
  --kafka-topic cdc-topic \
  --doris-fenodes 10.0.0.10:8030 \
  --doris-database test_db \
  --doris-table cdc_sync_doris \
  --doris-user root \
  --doris-password ""

# Kafka → S3
python src/main-kafka-s3.py \
  --kafka-bootstrap your-kafka:9092 \
  --kafka-topic test \
  --s3-output-path s3://your-bucket/flink-output/ \
  --aws-region us-east-1

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
