import boto3
import os
from dotenv import load_dotenv

load_dotenv()

def upload_to_s3(local_folder):
    """
    Moves local files to the Cloud. 
    This represents the 'Extraction' phase of ETL.
    """
    bucket_name = os.getenv('S3_BUCKET_NAME')
    s3 = boto3.client(
        's3',
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
        region_name=os.getenv('AWS_REGION')
    )

    # Ensure the local folder exists
    if not os.path.exists(local_folder):
        print(f"Error: {local_folder} not found.")
        return

    for filename in os.listdir(local_folder):
        if filename.endswith(".csv"):
            local_path = os.path.join(local_folder, filename)
            s3_path = f"raw/{filename}" 
            
            print(f"🚀 Uploading {filename} to s3://{bucket_name}/{s3_path}")
            s3.upload_file(local_path, bucket_name, s3_path)
    
    print("✅ First-Ingestion to s3 Complete!")

if __name__ == "__main__":
    upload_to_s3('data/raw')