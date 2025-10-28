# AWS Managed Flink PyFlink 作业例子

### 可以使用功能

1. pyflink simple test 用户简单测试,对应src中main-simple.py
2. pyflink kafka s3 sink  使用kafka, 对应src中main-kafka-s3.py, 依赖对应pom-kafka-s3.xml
3. python  kafka iceberg sink  使用iceberg，对应src中main.py, 依赖对应pom.xml

### 依赖编译方式
```
# 执行如下命令编译即可，jdk 11,  maven 3.9.x. 
mvn clean package 

# 编译好的zip

```
编译之后在target目录下会有zip和依赖jar。在MSF上提交作业只需要zip即可同时需要配置如下参数

| Group ID                              | Key       | Mandatory | Value                          | Notes                                                                     |
|---------------------------------------|-----------|-----------|--------------------------------|---------------------------------------------------------------------------|
| `kinesis.analytics.flink.run.options` | `python`  | Y         | `main.py`                      | The Python script containing the main() method to start the job.          |
| `kinesis.analytics.flink.run.options` | `jarfile` | Y         | `lib/pyflink-dependencies.jar` | Location (inside the zip) of the fat-jar containing all jar dependencies. |


### 注意事项
#### 代码权限相关
1. 如果python代码有错误，提交作业会失败，在日志中可以看到 CodeError.InvalidApplicationCode 类似错误
2. 如果MSF配置的iam role没有glue catalog的权限，或者有glue catalog权限，但是数据库，表的权限是只有LF的权限，也会有CodeError.InvalidApplicationCode 类似错误。但从INFO日志中可以找到权限异常。 如果是LF权限问题，需要在LF中对使用的database为IAMAllowedPrincipa 添加super权限，以便让MSF 有创建iceberg glue库表权限
3. 如果MSF配置的iam role没有s3 权限，也会有 CodeError.InvalidApplicationCode 类似错误，从INFO日志中可以看到相关权限异常。

### 依赖相关
1. 使用MSF时, pyflink 相关的依赖jar，比如iceberg，kafka 等，都需要maven 编译打包到zip中使用
2. 使用udf需要python的额外库，可以在添加requirenments.txt，可以参考 https://github.com/aws-samples/amazon-managed-service-for-apache-flink-examples/tree/main/python/PythonDependencies
3. 如果不使用iceberg,将 src/main文件执行 mv src/mian src/main.bak 

### 本地调试
main-local.py 提供了本地调试flink的方式，运行方式如下
```
# 安装uv和依赖
uv venv -p 3.11
source ./venv/bin/activate
uv pip install boto3
uv pip install apache-flink==1.20.0
uv pip install setuptools

# 执行
python main-local.py

# flink web ui 端口设定的是本机的 8081 直接访问即可
# flink 日志 .venv/lib/python3.11/site-packages/pyflink/log/
```

### 作业提交相关截图
![](https://pcmyp.oss-cn-beijing.aliyuncs.com/markdown/202510220029587.png)
![](https://pcmyp.oss-cn-beijing.aliyuncs.com/markdown/202510220030317.png)
![](https://pcmyp.oss-cn-beijing.aliyuncs.com/markdown/202510220031134.png)

### local 模式截图
![](https://pcmyp.oss-cn-beijing.aliyuncs.com/markdown/202510220122940.png)
