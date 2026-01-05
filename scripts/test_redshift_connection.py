import boto3
import pg8000.native
import os
from dotenv import load_dotenv

# Load variables from .env
load_dotenv()

def test_redshift_connection():
    # 1. Initialize Redshift Serverless Client
    client = boto3.client(
        'redshift-serverless',
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
        region_name=os.getenv('AWS_REGION')
    )

    try:
        print("Step 1: Fetching temporary credentials from AWS...")
        # This replaces the need for a permanent database password
        credentials = client.get_credentials(
            workgroupName='default-workgroup',
            dbName=os.getenv('REDSHIFT_DB')
        )
        
        # 2. Connect to the database
        print("Step 2: Connecting to Redshift Serverless...")
        conn = pg8000.native.Connection(
            user=credentials['dbUser'],
            password=credentials['dbPassword'],
            host=os.getenv('REDSHIFT_HOST'),
            port=5439,
            database=os.getenv('REDSHIFT_DB')
        )
        
        # 3. Run a test query
        result = conn.run("SELECT 1;")
        print(f"✅ Success! Database returned: {result}")
        conn.close()

    except Exception as e:
        print(f"❌ Connection Failed: {e}")
        print("\nTroubleshooting:")
        print("- Is 'Publicly accessible' ON in the Workgroup settings?")
        print("- Is Port 5439 open for your IP in the Security Group?")

if __name__ == "__main__":
    test_redshift_connection()