import boto3
import json
import time
import zipfile
import os
from botocore.exceptions import ClientError

class FlinkManager:
    def __init__(self, region='us-east-1'):
        self.client = boto3.client('kinesisanalyticsv2', region_name=region)
        self.s3_client = boto3.client('s3', region_name=region)
        self.iam_client = boto3.client('iam', region_name=region)
        self.region = region
        self.service_role_name = 'kinesis-analytics-service-role'
        
    def ensure_service_role(self):
        """Ensure the required IAM service role exists, create if not"""
        try:
            # Check if role exists
            role_arn = self._get_service_role_arn()
            if role_arn:
                print(f"IAM service role already exists: {role_arn}")
                return role_arn
            
            # Create role if it doesn't exist
            return self._create_service_role()
            
        except Exception as e:
            print(f"Error managing IAM role: {e}")
            return None
    
    def _get_service_role_arn(self):
        """Get the ARN of the existing service role"""
        try:
            response = self.iam_client.get_role(RoleName=self.service_role_name)
            return response['Role']['Arn']
        except self.iam_client.exceptions.NoSuchEntityException:
            return None
        except Exception as e:
            print(f"Error checking role existence: {e}")
            return None
    
    def _create_service_role(self):
        """Create the IAM service role for Kinesis Analytics"""
        try:
            # Trust policy for Kinesis Analytics
            trust_policy = {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": {
                            "Service": "kinesisanalytics.amazonaws.com"
                        },
                        "Action": "sts:AssumeRole"
                    }
                ]
            }
            
            # Permissions policy
            permissions_policy = {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Action": [
                            "glue:*",
                        ],
                        "Resource": "*"
                    },
                    {
                        "Effect": "Allow",
                        "Action": [
                            "s3:*",
                        ],
                        "Resource": "*"
                    },
                    {
                        "Effect": "Allow",
                        "Action": [
                            "ec2:*",
                        ],
                        "Resource": "*"
                    },
                    {
                        "Effect": "Allow",
                        "Action": [
                            "logs:CreateLogGroup",
                            "logs:CreateLogStream",
                            "logs:PutLogEvents",
                            "logs:DescribeLogGroups",
                            "logs:DescribeLogStreams"
                        ],
                        "Resource": "*"
                    }
                ]
            }
            
            # Create role
            self.iam_client.create_role(
                RoleName=self.service_role_name,
                AssumeRolePolicyDocument=json.dumps(trust_policy),
                Path='/service-role/',
                Description='Service role for Kinesis Analytics applications'
            )
            
            # Attach inline policy
            self.iam_client.put_role_policy(
                RoleName=self.service_role_name,
                PolicyName='KinesisAnalyticsServicePolicy',
                PolicyDocument=json.dumps(permissions_policy)
            )
            
            # Get account ID and construct ARN
            sts = boto3.client('sts')
            account_id = sts.get_caller_identity()['Account']
            role_arn = f"arn:aws:iam::{account_id}:role/service-role/{self.service_role_name}"
            
            print(f"IAM service role created successfully: {role_arn}")
            return role_arn
            
        except Exception as e:
            print(f"Error creating IAM role: {e}")
            return None
        
    def create_python_application_zip(self, job_file_path, requirements_file_path, zip_name='pyflink-job.zip'):
        """Create zip file containing the PyFlink job and requirements.txt"""
        with zipfile.ZipFile(zip_name, 'w') as zipf:
            # Add the main Python file
            zipf.write(job_file_path, os.path.basename(job_file_path))
                # Add lib directory and all its contents
            job_dir = os.path.dirname(job_file_path)
            lib_dir = os.path.join(job_dir, 'lib')
            for root, dirs, files in os.walk(lib_dir):
                for file in files:
                    file_path = os.path.join(root, file)
                    # Keep lib/ prefix in zip
                    arcname = os.path.relpath(file_path, job_dir)
                    zipf.write(file_path, arcname)
            # Add requirements.txt
            zipf.write(requirements_file_path, 'requirements.txt')
        return zip_name
    
    def upload_to_s3(self, file_path, bucket, key):
        """Upload file to S3"""
        try:
            self.s3_client.upload_file(file_path, bucket, key)
            print(f"Uploaded {file_path} to s3://{bucket}/{key}")
            return f"s3://{bucket}/{key}"
        except Exception as e:
            print(f"Error uploading to S3: {e}")
            return None
    
    def create_application(self, app_name, s3_bucket_arn, s3_object_key, subnet_id, sg_id,python_main_file='main.py',dep_jar='lib/pyflink-dependencies.jar'):
        """Create Kinesis Analytics application for Python/PyFlink"""
        try:
            # Ensure service role exists
            service_role_arn = self.ensure_service_role()
            if not service_role_arn:
                print("Failed to create or get service role")
                return None
            
            response = self.client.create_application(
                ApplicationName=app_name,
                ApplicationDescription='pyflink-sql',
                RuntimeEnvironment='FLINK-1_20',
                ServiceExecutionRole=service_role_arn,
                ApplicationConfiguration={
                    'ApplicationCodeConfiguration': {
                        'CodeContent': {
                            'S3ContentLocation': {
                                'BucketARN': s3_bucket_arn,
                                'FileKey': s3_object_key
                            }
                        },
                        'CodeContentType': 'ZIPFILE'
                    },
                    'EnvironmentProperties': {
                        'PropertyGroups': [
                            {
                                'PropertyGroupId': 'kinesis.analytics.flink.run.options',
                                'PropertyMap': {
                                    'python': python_main_file,
                                    'jarfile': dep_jar
                                }
                            }
                        ]
                    },
                    'VpcConfigurations': [
                        {
                            'SubnetIds': [
                                subnet_id
                            ],
                            'SecurityGroupIds': [
                                sg_id
                            ]
                        },
                    ],
                    'FlinkApplicationConfiguration': {
                        'CheckpointConfiguration': {
                            'ConfigurationType': 'DEFAULT'
                        },
                        'MonitoringConfiguration': {
                            'ConfigurationType': 'DEFAULT'
                        },
                        'ParallelismConfiguration': {
                            'ConfigurationType': 'DEFAULT'
                        }
                    },
                    'ApplicationSnapshotConfiguration': {
                        'SnapshotsEnabled': False
                    }
                }
            )
            print(f"Application {app_name} created successfully")
            return response
        except ClientError as e:
            print(f"Error creating application: {e}")
            return None
    
    def start_application(self, app_name):
        """Start the application"""
        try:
            response = self.client.start_application(
                ApplicationName=app_name,
                RunConfiguration={
                    'FlinkRunConfiguration': {
                        'AllowNonRestoredState': True
                    },
                    'ApplicationRestoreConfiguration': {
                        'ApplicationRestoreType': 'SKIP_RESTORE_FROM_SNAPSHOT'
                    }
                }
            )
            print(f"Application {app_name} started successfully")
            return response
        except ClientError as e:
            print(f"Error starting application: {e}")
            return None
    
    def stop_application(self, app_name):
        """Stop the application"""
        try:
            response = self.client.stop_application(ApplicationName=app_name)
            print(f"Application {app_name} stopped successfully")
            return response
        except ClientError as e:
            print(f"Error stopping application: {e}")
            return None
    
    def get_application_status(self, app_name):
        """Get application status"""
        try:
            response = self.client.describe_application(ApplicationName=app_name)
            status = response['ApplicationDetail']['ApplicationStatus']
            print(f"Application {app_name} status: {status}")
            return status
        except ClientError as e:
            print(f"Error getting application status: {e}")
            return None
    
    def get_application_details(self, app_name):
        """Get detailed application information"""
        try:
            response = self.client.describe_application(ApplicationName=app_name)
            return response['ApplicationDetail']
        except ClientError as e:
            print(f"Error getting application details: {e}")
            return None
    
    def update_application(self, app_name, s3_bucket_arn, s3_object_key):
        """Update application code"""
        try:
            # Get current application version
            app_details = self.client.describe_application(ApplicationName=app_name)
            current_version = app_details['ApplicationDetail']['ApplicationVersionId']
            
            response = self.client.update_application(
                ApplicationName=app_name,
                CurrentApplicationVersionId=current_version,
                ApplicationConfigurationUpdate={
                    'ApplicationCodeConfigurationUpdate': {
                        'CodeContentUpdate': {
                            'S3ContentLocationUpdate': {
                                'BucketARNUpdate': s3_bucket_arn,
                                'FileKeyUpdate': s3_object_key
                            }
                        }
                    }
                }
            )
            print(f"Application {app_name} updated successfully")
            return response
        except ClientError as e:
            print(f"Error updating application: {e}")
            return None
    
    def delete_application(self, app_name):
        """Delete application"""
        try:
            # Stop application first if running
            status = self.get_application_status(app_name)
            if status == 'RUNNING':
                print("Stopping application before deletion...")
                self.stop_application(app_name)
                # Wait for application to stop
                max_wait = 180
                wait_time = 0
                while wait_time < max_wait:
                    current_status = self.get_application_status(app_name)
                    if current_status == 'READY':
                        break
                    time.sleep(5)
                    wait_time += 5
            
            # Get application details to get CreateTimestamp
            app_details = self.client.describe_application(ApplicationName=app_name)
            create_timestamp = app_details['ApplicationDetail']['CreateTimestamp']
            
            response = self.client.delete_application(
                ApplicationName=app_name,
                CreateTimestamp=create_timestamp
            )
            print(f"Application {app_name} deleted successfully")
            return response
        except ClientError as e:
            print(f"Error deleting application: {e}")
            return None
