import boto3
from references import *
import os
from botocore.exceptions import ClientError
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# AWS Configuration
aws_region = os.getenv("AWS_REGION")
aws_access_key = os.getenv("AWS_ACCESS_KEY_ID")
aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
s3_bucket_name = os.getenv("S3_BUCKET_NAME")
s3_bucket_prefix = os.getenv("S3_BUCKET_PREFIX", "")

def setup_s3_client():
    try:
        if aws_access_key and aws_secret_key:
            s3_client = boto3.client(
                's3',
                region_name=aws_region,
                aws_access_key_id=aws_access_key,
                aws_secret_access_key=aws_secret_key
            )
            logger.info("S3 client initialized with explicit credentials")
        else:
            logger.error("AWS credentials not found in environment variables")
            return None
                
        # Verify connection
        s3_client.head_bucket(Bucket=s3_bucket_name)
        logger.info(f"Successfully connected to S3 bucket: {s3_bucket_name}")
        return s3_client
            
    except ClientError as e:
        error_code = e.response['Error']['Code']
        logger.error(f"Failed to connect to S3 bucket: {error_code}")
        raise

def list_objects(list_objects_flag):
    try:
        if not list_objects_flag:
            logger.info("S3 List Objects flag is set to False. Skipping object listing.")
            pass
        else:
            logger.info("S3 List Objects flag is set to True. Proceeding with object listing.")
        s3_client = setup_s3_client()
        if s3_client is None:
            logger.error("Failed to initialize S3 client")
            return
        response = s3_client.list_objects_v2(
            Bucket=s3_bucket_name
            #,Prefix=s3_bucket_prefix
        )
        
        objects = [obj['Key'] for obj in response.get('Contents', [])]
        logger.info(f"Found {len(objects)} objects in {s3_bucket_name}")
        for obj in objects:
            logger.info(f"- {obj}")
        
    except ClientError as e:
        logger.error(f"Error listing objects: {e}")
        raise

def upload_file_to_s3(upload_flag):
    try:
        if not upload_flag:
            logger.info("S3 Upload flag is set to False. Skipping file upload.")
            pass
        else:
            logger.info("S3 Upload flag is set to True. Proceeding with file upload.")
            # use s3_key as the path in the s3 bucket like a directory structure if needed
            file_ref = pokemon_card_file_path + pokemon_card_file_name
            s3_client = setup_s3_client()
            if s3_client is None:
                logger.error("Failed to initialize S3 client")
                return
            s3_key = s3_bucket_prefix + pokemon_card_file_name
            s3_client.upload_file(file_ref, s3_bucket_name, s3_key)
            logger.info(f"Successfully uploaded {file_ref} to s3://{s3_bucket_name}/{s3_key}")
        
    except ClientError as e:
        logger.error(f"Error uploading file: {e}")
        raise
    except FileNotFoundError:
        logger.error(f"File not found: {file_ref}")
        raise

def download_file_from_s3(download_flag):
    try:
        if not download_flag:
            logger.info("S3 Download flag is set to False. Skipping file download.")
            pass
        else:
            logger.info("S3 Download flag is set to True. Proceeding with file download.")
            s3_client = setup_s3_client()
            if s3_client is None:
                logger.error("Failed to initialize S3 client")
                return
            s3_key = s3_bucket_prefix + pokemon_card_file_name
            # Update these to use your actual output path variables
            local_file_path = pokemon_card_file_path + pokemon_card_file_name
            s3_client.download_file(s3_bucket_name, s3_key, local_file_path)
            logger.info(f"Successfully downloaded s3://{s3_bucket_name}/{s3_key} to {local_file_path}")
        
    except ClientError as e:
        logger.error(f"Error downloading file: {e}")
        raise
