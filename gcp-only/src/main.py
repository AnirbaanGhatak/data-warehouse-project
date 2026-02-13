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
    # --- 1. PARSE EVENT ---
    data = cloud_event.data
    bucket_name = data["bucket"]
    file_path = data["name"]
    
    # Get Project ID from Environment Variable (Set by Terraform)                   #does it set env variables???
    project_id = os.environ.get('PROJECT_ID')
    
    if not project_id:
        raise ValueError("PROJECT_ID environment variable is not set.")

    logging.info(f"Processing {file_path} from {bucket_name}")

    # Initialize BigQuery Client
    client = bigquery.Client(project=project_id)

    target_table = None
    domain = None
    # --- 2. ROUTING LOGIC ---
    # Map filenames to Target Tables
    if "raw_crm" in file_path:
        domain = "crm"

        if "cust_info" in file_path:
            target_table = "bronze_crm_cust_info"
        elif "prd_info" in file_path:
            target_table = "bronze_crm_prod_info"
        elif "sales_details" in file_path:
            target_table = "bronze_crm_sales_details"
            
    elif "raw_erp" in file_path:
        domain = "erp"

        if "loc_a101" in file_path:
            target_table = "bronze_erp_loc_a101"
        elif "cust_az12" in file_path:
            target_table = "bronze_erp_cust_az12"
        elif "cat_g1v2" in file_path:
            target_table = "bronze_px_cat_g1v2"
    else:
        logging.warning(f"SKIP: Unknown file pattern: {file_path}")
        return

    # --- 3. BRONZE LOAD (ELT Step 1) ---
    table_id = f"{project_id}.retail_bronze.{target_table}"
    
    uri = f"gs://{bucket_name}/{file_path}"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=True, # In Prod, we might use a predefined schema, but auto is fine for Bronze
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        allow_jagged_rows=True,
        allow_quoted_newlines=True
    )

    logging.info(f"Loading {file_path} into {table_id}...")
    try:
        load_job = client.load_table_from_uri(uri, table_id, job_config=job_config)
        load_job.result() # Wait for completion
        logging.info("Bronze Load Complete.")
    except Exception as e:
        logging.error(f"Bronze Load Failed: {e}")
        return
        
    silver_sql = generate_silver_sql(target_table, project_id)
    
    if silver_sql:
        logging.info(f"Running Silver Transformation {target_table}")
        # Execute Query
        try:
            client.query(silver_sql).result()           
            logging.info(f"SUCCESS: Silver Transformation Complete for {domain}.")
        except Exception as e:
            logging.error(f"Silver SQL Failed: {e}")


def generate_silver_sql(bronze_table_name, project_id):
    """
    Generates the SQL transformation logic dynamically.
    FIXED: Commas, LENGTH(), and Date Types.
    """
    if bronze_table_name == "bronze_crm_cust_info":
        return f"""
        CREATE OR REPLACE TABLE `{project_id}.retail_silver.silver_crm_cust_info` AS
        SELECT
            cst_id,
            cst_key,
            first_name,
            last_name,
            marital_status,
            gender,
            create_date,
            dwh_create_date
        FROM (
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
                cst_create_date AS create_date, -- FIXED: Removed PARSE_DATE, it is already a DATE
                ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS row_num,
                CURRENT_DATETIME() AS dwh_create_date
            FROM `{project_id}.retail_bronze.bronze_crm_cust_info`
        )
        WHERE row_num = 1
        """

    elif bronze_table_name == "bronze_crm_prod_info":
        return f"""
        CREATE OR REPLACE TABLE `{project_id}.retail_silver.silver_crm_prod_info` AS
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
            CAST(prd_start_dt AS DATE) AS start_date, -- FIXED: Added Comma here
            DATE_SUB(
                LEAD(CAST(prd_start_dt AS DATE)) OVER (PARTITION BY prd_key ORDER BY prd_start_dt), 
                INTERVAL 1 DAY
            ) AS prd_end_dt,
            CURRENT_DATETIME() AS dwh_create_date
        FROM `{project_id}.retail_bronze.bronze_crm_prod_info`
        """

    elif bronze_table_name == "bronze_crm_sales_details":
        return f"""
        CREATE OR REPLACE TABLE `{project_id}.retail_silver.silver_crm_sales_details` AS
        SELECT 
            sls_ord_num,
            sls_prd_key,
            sls_cust_id,
            CASE 
                WHEN sls_order_dt = 0 OR LENGTH(CAST(sls_order_dt AS STRING)) != 8 THEN NULL -- FIXED: LEN -> LENGTH
                ELSE PARSE_DATE('%Y%m%d', CAST(sls_order_dt AS STRING))
            END AS sls_order_dt,
            CASE 
                WHEN sls_ship_dt = 0 OR LENGTH(CAST(sls_ship_dt AS STRING)) != 8 THEN NULL
                ELSE PARSE_DATE('%Y%m%d', CAST(sls_ship_dt AS STRING))
            END AS sls_ship_dt,
            CASE 
                WHEN sls_due_dt = 0 OR LENGTH(CAST(sls_due_dt AS STRING)) != 8 THEN NULL
                ELSE PARSE_DATE('%Y%m%d', CAST(sls_due_dt AS STRING))
            END AS sls_due_dt,
            CASE 
                WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price) 
                    THEN sls_quantity * ABS(sls_price)
                ELSE sls_sales
            END AS sls_sales,
            sls_quantity,
            CASE 
                WHEN sls_price IS NULL OR sls_price <= 0 
                    THEN SAFE_DIVIDE(sls_sales, sls_quantity)
                ELSE sls_price
            END AS sls_price,
            CURRENT_DATETIME() AS dwh_create_date
        FROM `{project_id}.retail_bronze.bronze_crm_sales_details`
        """
    
#---------------------------------ERP---------------------------------------------

    
    elif bronze_table_name ==  "bronze_erp_loc_a101":
        return f"""
        CREATE OR REPLACE TABLE `{project_id}.retail_silver.silver_erp_loc_a101` AS
        SELECT
            REPLACE(cid, '-', '') AS cid, 
            CASE
                WHEN TRIM(country) = 'DE' THEN 'Germany'
                WHEN TRIM(country) IN ('US', 'USA') THEN 'United States'
                WHEN TRIM(country) = '' OR country IS NULL THEN 'n/a'
                ELSE TRIM(country)
            END AS country,
            CURRENT_DATETIME() AS dwh_create_date
        FROM `{project_id}.retail_bronze.bronze_erp_loc_a101`
        """

    elif bronze_table_name == "bronze_erp_cust_az12":
        return f"""
        CREATE OR REPLACE TABLE `{project_id}.retail_silver.silver_erp_cust_az12` AS
        SELECT
            CASE
                WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LENGTH(cid)) -- FIXED: LEN -> LENGTH
                ELSE cid
            END AS cid, 
            CASE
                WHEN bdate > CURRENT_DATE() THEN NULL -- FIXED: GETDATE() -> CURRENT_DATE()
                ELSE bdate
            END AS bdate,
            CASE
                WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
                WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
                ELSE 'n/a'
            END AS gen,
            CURRENT_DATETIME() AS dwh_create_date
        FROM `{project_id}.retail_bronze.bronze_erp_cust_az12` -- FIXED: Added backticks
        """
    

    elif bronze_table_name == "bronze_px_cat_g1v2":
        return f"""
        CREATE OR REPLACE TABLE `{project_id}.retail_silver.silver_px_cat_g1v2` AS
        SELECT
            id,
            cat,
            subcat,
            maintenance,
            CURRENT_DATETIME() AS dwh_create_date
        FROM `{project_id}.retail_bronze.bronze_px_cat_g1v2`
        """
    
    return None