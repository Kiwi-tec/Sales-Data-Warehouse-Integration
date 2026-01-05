from airflow import DAG
from airflow.decorators import task
from datetime import datetime, timedelta
import os
import sys
import pandas as pd

# Path inside the Docker container
sys.path.append('/opt/airflow/scripts')
from ingest_to_s3 import upload_to_s3
from transform_logic import clean_sales_data

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'sales_etl_pipeline',
    default_args=default_args,
    description='ETL pipeline for monthly sales data',
    schedule_interval='@monthly', 
    catchup=False
) as dag:

    @task
    def extract_step():
        # Match path in Docker volume
        upload_to_s3('/opt/airflow/data/raw')
        return "Extraction Complete"

    @task
    def transform_step():
        input_dir = '/opt/airflow/data/raw'
        output_dir = '/opt/airflow/data/processed'
        
        all_dfs = []
        for file in os.listdir(input_dir):
            if file.endswith(".csv"):
                df = pd.read_csv(os.path.join(input_dir, file))
                cleaned_df = clean_sales_data(df)
                all_dfs.append(cleaned_df)
        
        master_df = pd.concat(all_dfs, ignore_index=True)
        out_path = os.path.join(output_dir, 'final_sales_data.csv')
        master_df.to_csv(out_path, index=False)
        return out_path

    @task
    def load_to_redshift(file_path):
        import boto3
        import psycopg2
        import csv
        import os
        from dotenv import load_dotenv
        load_dotenv('/opt/airflow/.env')

        # 1. Fetch Dynamic Credentials (IAM)
        client = boto3.client(
            'redshift-serverless',
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
            region_name=os.getenv('AWS_REGION')
        )
        creds = client.get_credentials(workgroupName='default-workgroup', dbName=os.getenv('REDSHIFT_DB'))

        # 2. Connect to Redshift
        conn = psycopg2.connect(
            user=creds['dbUser'],
            password=creds['dbPassword'],
            host=os.getenv('REDSHIFT_HOST'),
            port=5439,
            dbname=os.getenv('REDSHIFT_DB')
        )
        cur = conn.cursor()
        
        # 3. Clear and Load
        cur.execute("TRUNCATE TABLE public.sales;")
        with open(file_path, 'r') as f:
            reader = csv.reader(f)
            next(reader) 
            for row in reader:
                # Target 'date' and 'quantity' (now a DECIMAL)
                cur.execute(
                    "INSERT INTO public.sales (product, quantity, price, date, revenue) VALUES (%s, %s, %s, %s, %s)",
                    (row[0], row[1], row[2], row[3], row[4])
                )
        
        conn.commit()
        cur.close()
        conn.close()
        return "Load to Redshift Complete!"

    raw_status = extract_step()
    processed_file = transform_step()
    load_status = load_to_redshift(processed_file)

    raw_status >> processed_file >> load_status