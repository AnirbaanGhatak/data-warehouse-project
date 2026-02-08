import functions_framework
from google.cloud import bigquery
import logging
import json
import os

# Configure Structured Logging (Bank Standard for Splunk/Datadog ingestion)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

@functions_framework.cloud_event
def process_data_pipeline(cloud_event):
    """
    Production Event-Driven ELT.
    Triggered by GCS Object Finalize.
    Orchestrates Load (Bronze) and Transform (Silver).
    """
    try:
        # --- 1. PARSE EVENT ---
        data = cloud_event.data
        bucket_name = data["bucket"]
        file_name = data["name"]
        
        # Get Project ID from Environment Variable (Set by Terraform)                   #does it set env variables???
        project_id = os.environ.get('PROJECT_ID')
        
        if not project_id:
            raise ValueError("PROJECT_ID environment variable is not set.")

        logging.info(f"INIT: Processing file {file_name} from {bucket_name}")

        # Initialize BigQuery Client
        client = bigquery.Client(project=project_id)

        # --- 2. ROUTING LOGIC ---
        # Map filenames to Target Tables
        domain = None
        if "customer" in file_name.lower():
            domain = "customer"
            table_bronze = f"{project_id}.retail_bronze.raw_customers"
            table_silver = f"{project_id}.retail_silver.dim_customers"
        elif "prod" in file_name.lower():
            domain = "product"
            table_bronze = f"{project_id}.retail_bronze.raw_products"
            table_silver = f"{project_id}.retail_silver.dim_products"
        else:
            logging.warning(f"SKIP: Unknown file pattern: {file_name}")
            return

        # --- 3. BRONZE LOAD (ELT Step 1) ---
        uri = f"gs://{bucket_name}/{file_name}"
        
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,
            autodetect=True, # In Prod, we might use a predefined schema, but auto is fine for Bronze
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED
        )

        logging.info(f"ACTION: Loading Bronze Table: {table_bronze}")
        load_job = client.load_table_from_uri(uri, table_bronze, job_config=job_config)
        load_job.result() # Wait for completion
        
        # Log Statistics
        table_obj = client.get_table(table_bronze)
        logging.info(f"SUCCESS: Bronze Load. Total rows in table: {table_obj.num_rows}")

        # --- 4. SILVER TRANSFORM (ELT Step 2) ---
        logging.info(f"ACTION: Transforming Silver Table: {table_silver}")
        
        sql = generate_silver_sql(domain, table_bronze, table_silver)
        
        # Execute Query
        query_job = client.query(sql)
        query_job.result() # Wait for completion
        
        logging.info(f"SUCCESS: Silver Transformation Complete for {domain}.")

    except Exception as e:
        # Log the full error for Cloud Logging
        logging.error(f"CRITICAL FAILURE: {str(e)}")
        # Re-raise so Cloud Functions marks it as 'Failed' (Triggering retries if configured)
        raise e

def generate_silver_sql(domain, source_table, target_table):
    """
    Generates the SQL transformation logic dynamically.
    """
    if domain == "customer":
        return f"""
        CREATE OR REPLACE TABLE `{target_table}` AS
        SELECT
            cst_id,
            cst_key,
            TRIM(cst_firstname) AS first_name,
            TRIM(cst_lastname) AS last_name,
            CASE 
                WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
                WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
                ELSE 'Unknown'
            END AS marital_status,
            CASE 
                WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
                WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
                ELSE 'Unknown'
            END AS gender,
            PARSE_DATE('%Y-%m-%d', cst_create_date) AS create_date
        FROM (
            SELECT
                *,
                ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS row_num
            FROM `{source_table}`
        )
        WHERE row_num = 1
        """
    elif domain == "product":
        return f"""
        CREATE OR REPLACE TABLE `{target_table}` AS
        SELECT
            prd_id,
            REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS category_id,
            SUBSTRING(prd_key, 7, LENGTH(prd_key)) AS product_key,
            prd_nm AS product_name,
            COALESCE(prd_cost, 0) AS cost,
            CASE 
                WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
                WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
                ELSE 'Other'
            END AS product_line,
            CAST(prd_start_dt AS DATE) AS start_date
        FROM `{source_table}`
        """
    return ""