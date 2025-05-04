import boto3
import os

# Configuration S3 (LocalStack)
s3 = boto3.client('s3',
    endpoint_url='http://localhost:4566',
    aws_access_key_id='test',
    aws_secret_access_key='test',
    region_name='us-east-1'
)

# Fichiers à uploader
files = ['train.txt', 'test.txt', 'validation.txt']

for file in files:
    file_path = os.path.join('data/raw', file)
    s3.upload_file(file_path, 'raw', f'wikitext/{file}')
    print(f"Uploaded {file} to S3 bucket 'raw'")
