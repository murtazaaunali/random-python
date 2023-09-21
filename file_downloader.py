import boto3
import os
import botocore

# Replace these with your AWS access key and secret key
aws_access_key_id = ''
aws_secret_access_key = ''
bucket_name = ''
local_base_dir = ''

# Initialize the S3 client with custom retry settings
s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key,
                  config=botocore.client.Config(retries={'max_attempts': 10}))

# Function to download a single file and create local directories as needed
def download_file(bucket, key, local_base_dir):
    local_path = os.path.join(local_base_dir, key)
    local_dir = os.path.dirname(local_path)
    
    # Create local directories as needed
    os.makedirs(local_dir, exist_ok=True)
    
    s3.download_file(bucket, key, local_path)

# Function to download all files in a folder and maintain the folder structure
def download_folder(bucket, prefix, local_base_dir):
    paginator = s3.get_paginator('list_objects_v2')
    page_iterator = paginator.paginate(Bucket=bucket, Prefix=prefix)
    
    for page in page_iterator:
        for obj in page.get('Contents', []):
            key = obj['Key']
            # print(key)
            download_file(bucket, key, local_base_dir)

# Create the local base directory if it doesn't exist
if not os.path.exists(local_base_dir):
    os.makedirs(local_base_dir)

# Download all files and folders from the S3 bucket while preserving structure
download_folder(bucket_name, '', local_base_dir)
