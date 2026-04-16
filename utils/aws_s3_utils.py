from references import *

def setup_s3_client():
    try:
        if AWS_ACCESS_KEY and AWS_SECRET_KEY and AWS_REGION:
            s3_client = boto3.client(
                's3',
                region_name=AWS_REGION,
                aws_access_key_id=AWS_ACCESS_KEY,
                aws_secret_access_key=AWS_SECRET_KEY
            )
            logger.info("S3 client initialized with explicit credentials")
        else:
            logger.error("AWS credentials not found in environment variables")
            return None
                
        # Verify connection
        s3_client.head_bucket(Bucket=S3_BUCKET_NAME)
        logger.info(f"Successfully connected to S3 bucket: {S3_BUCKET_NAME}")
        return s3_client
            
    except ClientError as e:
        error_code = e.response['Error']['Code']
        logger.error(f"Failed to connect to S3 bucket: {error_code}", exc_info=True)
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
            logger.error("Failed to initialize S3 client", exc_info=True)
            return
        response = s3_client.list_objects_v2(
            Bucket=S3_BUCKET_NAME
            #,Prefix=S3_BUCKET_PREFIX  
            # Uncomment the above line if you want to list objects with a specific prefix
        )
        
        objects = [obj['Key'] for obj in response.get('Contents', [])]
        logger.info(f"Found {len(objects)} objects in {S3_BUCKET_NAME}")
        for obj in objects:
            logger.info(f"- {obj}")
        
    except ClientError as e:
        logger.error(f"Error listing objects: {str(e)}", exc_info=True)
        raise

def upload_file_to_s3(upload_flag):
    try:
        if not upload_flag:
            logger.info("S3 Upload flag is set to False. Skipping file upload.")
            pass
        else:
            logger.info("S3 Upload flag is set to True. Proceeding with file upload.")
            # use s3_key as the path in the s3 bucket like a directory structure if needed
            file_ref = POKEMON_CARD_FILE_PATH + POKEMON_CARD_FILE_NAME
            s3_client = setup_s3_client()
            if s3_client is None:
                logger.error("Failed to initialize S3 client", exc_info=True)
                raise
            s3_key = S3_BUCKET_PREFIX + POKEMON_CARD_FILE_NAME
            s3_client.upload_file(file_ref, S3_BUCKET_NAME, s3_key)
            logger.info(f"Successfully uploaded {file_ref} to s3://{S3_BUCKET_NAME}/{s3_key}")
        
    except ClientError as e:
        logger.error(f"Error uploading file: {str(e)}", exc_info=True)
        raise
    except FileNotFoundError:
        logger.error(f"File not found: {file_ref}", exc_info=True)
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
                logger.error("Failed to initialize S3 client", exc_info=True)
                raise
            s3_key = S3_BUCKET_PREFIX + POKEMON_CARD_FILE_NAME
            # Update these to use your actual output path variables
            local_file_path = POKEMON_CARD_FILE_PATH + POKEMON_CARD_FILE_NAME
            s3_client.download_file(S3_BUCKET_NAME, s3_key, local_file_path)
            logger.info(f"Successfully downloaded s3://{S3_BUCKET_NAME}/{s3_key} to {local_file_path}")
        
    except ClientError as e:
        logger.error(f"Error downloading file: {str(e)}", exc_info=True)
        raise
