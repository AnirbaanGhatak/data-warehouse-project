import argparse
import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, SetupOptions
from google.cloud import bigquery # Standard Python Client

# ==============================================================================
# 1. BEAM LOGIC: Row-Level Cleaning (The "Python" Stuff)
# ==============================================================================
class CleanCustomerData(beam.DoFn):
    def process(self, element):
        try:
            # Your existing cleaning logic here (Splitting names, formatting dates)
            # ... (Copy logic from previous response) ...
            
            yield {
                'customer_id': str(element.get('customer_id')),
                'first_name': first_name, # calculated above
                'last_name': last_name,   # calculated above
                'email': clean_email,     # calculated above
                'registration_date': clean_date,
                'ingestion_timestamp': str(element.get('ingestion_timestamp')) # Assuming you track when it came in
            }
        except Exception as e:
            logging.error(f"Error: {e}")

# ==============================================================================
# 2. MAIN EXECUTION
# ==============================================================================
def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--input_table', required=True)
    parser.add_argument('--output_table', required=True) # This will be the FINAL table
    parser.add_argument('--project', required=True)
    
    known_args, pipeline_args = parser.parse_known_args(argv)
    
    # Define a Temporary "Staging" Table
    # If output is 'retail_silver.dim_customers', staging is 'retail_silver.dim_customers_staging'
    staging_table = f"{known_args.output_table}_staging"

    options = PipelineOptions(pipeline_args)
    google_cloud_options = options.view_as(GoogleCloudOptions)
    options.view_as(SetupOptions).save_main_session = True

    # --- PART A: The Beam Pipeline (Cleaning) ---
    logging.info("Starting Dataflow Job...")
    with beam.Pipeline(options=options) as p:
        (
            p
            | 'ReadBronze' >> beam.io.ReadFromBigQuery(
                table=known_args.input_table,
                method=beam.io.ReadFromBigQuery.Method.DIRECT_READ
            )
            | 'CleanRows' >> beam.ParDo(CleanCustomerData())
            | 'WriteToStaging' >> beam.io.WriteToBigQuery(
                staging_table,
                # Create staging table if it doesn't exist, truncate if it does
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE
            )
        )
    # The 'with' block ensures the pipeline finishes before code continues (for DirectRunner).
    # For DataflowRunner, we might need to explicitly wait, but let's assume standard blocking.
    
    # --- PART B: The SQL Pushdown (Window Functions) ---
    # This runs AFTER Beam has successfully dumped cleaned data into staging.
    
    logging.info("Dataflow finished. Running Window Functions via SQL...")
    
    client = bigquery.Client(project=known_args.project)
    
    # YOUR COMPLEX SQL LOGIC GOES HERE
    # Example: Deduplicate using ROW_NUMBER()
    query = f"""
    CREATE OR REPLACE TABLE `{known_args.output_table}` AS
    SELECT 
        customer_id, 
        first_name, 
        last_name, 
        email, 
        registration_date
    FROM (
        SELECT 
            *,
            -- THE WINDOW FUNCTION
            -- Partition by ID, Order by Date Descending (Keep latest)
            ROW_NUMBER() OVER(
                PARTITION BY customer_id 
                ORDER BY registration_date DESC
            ) as row_num
        FROM `{staging_table}`
    )
    WHERE row_num = 1
    """
    
    # Run the query
    query_job = client.query(query)
    query_job.result() # Wait for completion
    
    # Optional: Delete staging table to save money
    client.delete_table(staging_table, not_found_ok=True)
    
    logging.info(f"Successfully created {known_args.output_table} with Window logic applied.")

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()