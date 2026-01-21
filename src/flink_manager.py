from __future__ import annotations

import boto3
import json
import logging
import os
import time
import zipfile

from botocore.exceptions import ClientError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


class FlinkManager:
    def __init__(self, region="us-east-1", s3_bucket=None):
        self.client = boto3.client("kinesisanalyticsv2", region_name=region)
        self.s3_client = boto3.client("s3", region_name=region)
        self.iam_client = boto3.client("iam", region_name=region)
        self.logs_client = boto3.client("logs", region_name=region)
        self.region = region
        self.s3_bucket = s3_bucket
        self.service_role_name = "msf-flink-service-role"

    def ensure_service_role(self):
        try:
            role_arn = self._get_service_role_arn()
            if role_arn:
                logger.info(f"IAM service role already exists: {role_arn}")
                if self.s3_bucket:
                    self._ensure_s3_permission(self.s3_bucket)
                return role_arn
            return self._create_service_role()
        except Exception as e:
            logger.error(f"Error managing IAM role: {e}")
            return None

    def _get_service_role_arn(self):
        try:
            response = self.iam_client.get_role(RoleName=self.service_role_name)
            return response["Role"]["Arn"]
        except self.iam_client.exceptions.NoSuchEntityException:
            return None
        except Exception as e:
            logger.error(f"Error checking role existence: {e}")
            return None

    def _ensure_s3_permission(self, bucket: str):
        """Ensure IAM role has S3 permission for the specified bucket"""
        policy_name = f"{self.service_role_name}-policy"

        try:
            response = self.iam_client.get_role_policy(
                RoleName=self.service_role_name,
                PolicyName=policy_name,
            )
            policy_doc = response["PolicyDocument"]

            s3_statement = next(
                (s for s in policy_doc["Statement"] if s.get("Sid") == "S3Access"),
                None,
            )
            if not s3_statement:
                logger.warning("S3Access statement not found in policy")
                return

            resources = s3_statement.get("Resource", [])
            if isinstance(resources, str):
                resources = [resources]

            if "arn:aws:s3:::*" in resources:
                return

            bucket_arn = f"arn:aws:s3:::{bucket}"
            bucket_objects_arn = f"arn:aws:s3:::{bucket}/*"

            if bucket_arn in resources and bucket_objects_arn in resources:
                logger.info(f"S3 permission for bucket '{bucket}' already exists")
                return

            if bucket_arn not in resources:
                resources.append(bucket_arn)
            if bucket_objects_arn not in resources:
                resources.append(bucket_objects_arn)

            s3_statement["Resource"] = resources

            self.iam_client.put_role_policy(
                RoleName=self.service_role_name,
                PolicyName=policy_name,
                PolicyDocument=json.dumps(policy_doc),
            )
            logger.info(f"Added S3 permission for bucket '{bucket}'")

        except Exception as e:
            logger.warning(f"Failed to update S3 permission: {e}")

    def _create_service_role(self):
        try:
            trust_policy = {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": {"Service": "kinesisanalytics.amazonaws.com"},
                        "Action": "sts:AssumeRole",
                    }
                ],
            }

            s3_resource = ["arn:aws:s3:::*", "arn:aws:s3:::*/*"]
            if self.s3_bucket:
                s3_resource = [
                    f"arn:aws:s3:::{self.s3_bucket}",
                    f"arn:aws:s3:::{self.s3_bucket}/*",
                ]

            permissions_policy = {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Sid": "S3Access",
                        "Effect": "Allow",
                        "Action": [
                            "s3:GetObject",
                            "s3:PutObject",
                            "s3:DeleteObject",
                            "s3:ListBucket",
                            "s3:GetBucketLocation",
                        ],
                        "Resource": s3_resource,
                    },
                    {
                        "Sid": "GlueAccess",
                        "Effect": "Allow",
                        "Action": [
                            "glue:GetDatabase",
                            "glue:GetDatabases",
                            "glue:CreateDatabase",
                            "glue:GetTable",
                            "glue:GetTables",
                            "glue:CreateTable",
                            "glue:UpdateTable",
                            "glue:DeleteTable",
                            "glue:GetPartitions",
                            "glue:BatchCreatePartition",
                            "glue:BatchDeletePartition",
                        ],
                        "Resource": "*",
                    },
                    {
                        "Sid": "LakeFormationAccess",
                        "Effect": "Allow",
                        "Action": ["lakeformation:GetDataAccess"],
                        "Resource": "*",
                    },
                    {
                        "Sid": "CloudWatchLogs",
                        "Effect": "Allow",
                        "Action": [
                            "logs:CreateLogGroup",
                            "logs:CreateLogStream",
                            "logs:PutLogEvents",
                            "logs:DescribeLogGroups",
                            "logs:DescribeLogStreams",
                        ],
                        "Resource": "arn:aws:logs:*:*:*",
                    },
                    {
                        "Sid": "KinesisAnalytics",
                        "Effect": "Allow",
                        "Action": [
                            "kinesisanalytics:DescribeApplication",
                            "kinesisanalytics:ListApplications",
                        ],
                        "Resource": "*",
                    },
                    {
                        "Sid": "VPCAccess",
                        "Effect": "Allow",
                        "Action": [
                            "ec2:DescribeVpcs",
                            "ec2:DescribeSubnets",
                            "ec2:DescribeSecurityGroups",
                            "ec2:DescribeNetworkInterfaces",
                            "ec2:CreateNetworkInterface",
                            "ec2:DeleteNetworkInterface",
                            "ec2:DescribeDhcpOptions",
                        ],
                        "Resource": "*",
                    },
                    {
                        "Sid": "ENIManagement",
                        "Effect": "Allow",
                        "Action": ["ec2:CreateNetworkInterfacePermission"],
                        "Resource": "arn:aws:ec2:*:*:network-interface/*",
                        "Condition": {
                            "StringEquals": {
                                "ec2:AuthorizedService": "kinesisanalytics.amazonaws.com"
                            }
                        },
                    },
                ],
            }

            self.iam_client.create_role(
                RoleName=self.service_role_name,
                AssumeRolePolicyDocument=json.dumps(trust_policy),
                Path="/service-role/",
                Description="Service role for MSF Flink applications",
            )

            self.iam_client.put_role_policy(
                RoleName=self.service_role_name,
                PolicyName=f"{self.service_role_name}-policy",
                PolicyDocument=json.dumps(permissions_policy),
            )

            sts = boto3.client("sts")
            account_id = sts.get_caller_identity()["Account"]
            role_arn = (
                f"arn:aws:iam::{account_id}:role/service-role/{self.service_role_name}"
            )

            # Wait for IAM role to propagate
            logger.info("Waiting for IAM role to propagate...")
            max_attempts = 10
            for attempt in range(max_attempts):
                try:
                    self.iam_client.get_role(RoleName=self.service_role_name)
                    self.iam_client.get_role_policy(
                        RoleName=self.service_role_name,
                        PolicyName=f"{self.service_role_name}-policy",
                    )
                    time.sleep(5)
                    break
                except Exception:
                    if attempt < max_attempts - 1:
                        time.sleep(2)
                    else:
                        logger.warning(
                            "IAM role propagation check timed out, proceeding anyway"
                        )

            logger.info(f"IAM service role created: {role_arn}")
            return role_arn

        except Exception as e:
            logger.error(f"Error creating IAM role: {e}")
            return None

    def create_python_application_zip(
        self, job_file_path, requirements_file_path, zip_name="pyflink-job.zip"
    ):
        """Create zip file containing the PyFlink job and requirements.txt"""
        with zipfile.ZipFile(zip_name, "w") as zipf:
            # Add the main Python file
            zipf.write(job_file_path, os.path.basename(job_file_path))
            # Add lib directory and all its contents
            job_dir = os.path.dirname(job_file_path)
            lib_dir = os.path.join(job_dir, "lib")
            for root, dirs, files in os.walk(lib_dir):
                for file in files:
                    file_path = os.path.join(root, file)
                    # Keep lib/ prefix in zip
                    arcname = os.path.relpath(file_path, job_dir)
                    zipf.write(file_path, arcname)
            # Add requirements.txt
            zipf.write(requirements_file_path, "requirements.txt")
        return zip_name

    def upload_to_s3(self, file_path, bucket, key):
        try:
            self.s3_client.upload_file(file_path, bucket, key)
            logger.info(f"Uploaded {file_path} to s3://{bucket}/{key}")
            return f"s3://{bucket}/{key}"
        except Exception as e:
            logger.error(f"Error uploading to S3: {e}")
            return None

    def create_application(
        self,
        app_name,
        s3_bucket_arn,
        s3_object_key,
        subnet_id,
        sg_id,
        python_main_file="main-kafka-iceberg.py",
        dep_jar="lib/pyflink-dependencies.jar",
        app_properties: dict[str, str] | None = None,
    ):
        """Create Kinesis Analytics application for Python/PyFlink

        Args:
            app_name: Application name
            s3_bucket_arn: S3 bucket ARN for code
            s3_object_key: S3 object key for code zip
            subnet_id: VPC subnet ID
            sg_id: Security group ID
            python_main_file: Python entry file name
            dep_jar: Dependency JAR path in zip
            app_properties: Application properties dict (passed to FlinkApplicationProperties)
        """
        service_role_arn = self.ensure_service_role()
        if not service_role_arn:
            logger.error("Failed to create or get service role")
            return None

        log_stream_arn = self._ensure_cloudwatch_log_group(app_name)

        property_groups = [
            {
                "PropertyGroupId": "kinesis.analytics.flink.run.options",
                "PropertyMap": {
                    "python": python_main_file,
                    "jarfile": dep_jar,
                },
            }
        ]

        if app_properties:
            property_groups.append(
                {
                    "PropertyGroupId": "FlinkApplicationProperties",
                    "PropertyMap": app_properties,
                }
            )

        app_config = {
            "ApplicationCodeConfiguration": {
                "CodeContent": {
                    "S3ContentLocation": {
                        "BucketARN": s3_bucket_arn,
                        "FileKey": s3_object_key,
                    }
                },
                "CodeContentType": "ZIPFILE",
            },
            "EnvironmentProperties": {"PropertyGroups": property_groups},
            "VpcConfigurations": [
                {"SubnetIds": [subnet_id], "SecurityGroupIds": [sg_id]},
            ],
            "FlinkApplicationConfiguration": {
                "CheckpointConfiguration": {"ConfigurationType": "DEFAULT"},
                "MonitoringConfiguration": {"ConfigurationType": "DEFAULT"},
                "ParallelismConfiguration": {"ConfigurationType": "DEFAULT"},
            },
            "ApplicationSnapshotConfiguration": {"SnapshotsEnabled": False},
        }

        # Retry logic for IAM assume role propagation delays
        max_retries = 3
        for attempt in range(max_retries):
            try:
                response = self.client.create_application(
                    ApplicationName=app_name,
                    ApplicationDescription="pyflink-sql",
                    RuntimeEnvironment="FLINK-1_20",
                    ServiceExecutionRole=service_role_arn,
                    ApplicationConfiguration=app_config,
                    CloudWatchLoggingOptions=[{"LogStreamARN": log_stream_arn}],
                )
                logger.info(f"Application {app_name} created successfully")
                return response
            except ClientError as e:
                error_msg = str(e)

                # Only retry for IAM assume role propagation error
                is_iam_assume_error = (
                    "sufficient privileges to assume the role" in error_msg
                )

                if is_iam_assume_error and attempt < max_retries - 1:
                    wait_time = 15 * (attempt + 1)  # 15s, 30s
                    logger.warning(
                        f"IAM role not ready, retrying in {wait_time}s... "
                        f"(attempt {attempt + 1}/{max_retries})"
                    )
                    time.sleep(wait_time)
                    continue

                logger.error(f"Error creating application: {e}")
                return None

        return None

    def _ensure_cloudwatch_log_group(self, app_name: str) -> str:
        """Create CloudWatch Log Group and Stream, return Log Stream ARN"""
        log_group_name = f"/aws/kinesis-analytics/{app_name}"
        log_stream_name = "kinesis-analytics-log-stream"

        try:
            self.logs_client.create_log_group(logGroupName=log_group_name)
            logger.info(f"Created CloudWatch Log Group: {log_group_name}")
        except self.logs_client.exceptions.ResourceAlreadyExistsException:
            pass

        try:
            self.logs_client.create_log_stream(
                logGroupName=log_group_name, logStreamName=log_stream_name
            )
            logger.info(f"Created CloudWatch Log Stream: {log_stream_name}")
        except self.logs_client.exceptions.ResourceAlreadyExistsException:
            pass

        sts = boto3.client("sts")
        account_id = sts.get_caller_identity()["Account"]
        return f"arn:aws:logs:{self.region}:{account_id}:log-group:{log_group_name}:log-stream:{log_stream_name}"

    def start_application(self, app_name):
        try:
            response = self.client.start_application(
                ApplicationName=app_name,
                RunConfiguration={
                    "FlinkRunConfiguration": {"AllowNonRestoredState": True},
                    "ApplicationRestoreConfiguration": {
                        "ApplicationRestoreType": "SKIP_RESTORE_FROM_SNAPSHOT"
                    },
                },
            )
            logger.info(f"Application {app_name} started successfully")
            return response
        except ClientError as e:
            logger.error(f"Error starting application: {e}")
            return None

    def stop_application(self, app_name):
        try:
            response = self.client.stop_application(ApplicationName=app_name)
            logger.info(f"Application {app_name} stopped successfully")
            return response
        except ClientError as e:
            logger.error(f"Error stopping application: {e}")
            return None

    def get_application_status(self, app_name):
        try:
            response = self.client.describe_application(ApplicationName=app_name)
            status = response["ApplicationDetail"]["ApplicationStatus"]
            logger.info(f"Application {app_name} status: {status}")
            return status
        except ClientError as e:
            logger.error(f"Error getting application status: {e}")
            return None

    def get_application_details(self, app_name):
        try:
            response = self.client.describe_application(ApplicationName=app_name)
            return response["ApplicationDetail"]
        except ClientError as e:
            logger.error(f"Error getting application details: {e}")
            return None

    def update_application(self, app_name, s3_bucket_arn, s3_object_key):
        try:
            app_details = self.client.describe_application(ApplicationName=app_name)
            current_version = app_details["ApplicationDetail"]["ApplicationVersionId"]

            response = self.client.update_application(
                ApplicationName=app_name,
                CurrentApplicationVersionId=current_version,
                ApplicationConfigurationUpdate={
                    "ApplicationCodeConfigurationUpdate": {
                        "CodeContentUpdate": {
                            "S3ContentLocationUpdate": {
                                "BucketARNUpdate": s3_bucket_arn,
                                "FileKeyUpdate": s3_object_key,
                            }
                        }
                    }
                },
            )
            logger.info(f"Application {app_name} updated successfully")
            return response
        except ClientError as e:
            logger.error(f"Error updating application: {e}")
            return None

    def delete_application(self, app_name):
        try:
            status = self.get_application_status(app_name)
            if status == "RUNNING":
                logger.info("Stopping application before deletion...")
                self.stop_application(app_name)
                max_wait = 180
                wait_time = 0
                while wait_time < max_wait:
                    current_status = self.get_application_status(app_name)
                    if current_status == "READY":
                        break
                    time.sleep(5)
                    wait_time += 5

            app_details = self.client.describe_application(ApplicationName=app_name)
            create_timestamp = app_details["ApplicationDetail"]["CreateTimestamp"]

            response = self.client.delete_application(
                ApplicationName=app_name, CreateTimestamp=create_timestamp
            )
            logger.info(f"Application {app_name} deleted successfully")
            return response
        except ClientError as e:
            logger.error(f"Error deleting application: {e}")
            return None
