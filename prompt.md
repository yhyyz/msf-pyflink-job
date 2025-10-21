你有20年的python开发经验，阅读当前目录下的quick_start.py 程序，修改程序支持传递命令行参数，需要传递的参数如下
    APP_NAME = 'flink-msk-iceberg-sink-demo'
    S3_BUCKET = 'pcd-01'  # 请修改为您的S3存储桶
    S3_KEY = 'flink-jobs/flink-msk-iceberg-sink-demo.zip'
    SUBNET_ID = "subnet-0f79e4471cfa74ced"
    SG_ID = "sg-f83dcdb3"
    AWS_REGION="ap-southeast-1"
    LOCAL_DEP_JAR_PATH="target/managed-flink-pyfink-msk-iceberg-1.0.0.zip"
    
    默认值可以使用上边，注意python 参数传递使用人类友好的方式，比如 --app_name 表示APP_NAME