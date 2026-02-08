#--------------------------BRONZE LAYER------------------------------------

#------------------------------CRM-----------------------------------------
resource "google_bigquery_dataset" "bronze_layer" {
  dataset_id    = "retail_bronze"
  friendly_name = "Bronze Layer"
  description   = "This is the Bronze layer of the Medallion Architecture, here raw data directly from CSV is only copied"

  location                   = var.gcp_region
  delete_contents_on_destroy = true
}

resource "google_bigquery_table" "bronze_crm_cust_info" {
  dataset_id = google_bigquery_dataset.bronze_layer.dataset_id
  table_id   = "bronze_crm_cust_info"
  deletion_protection = false
  schema = jsonencode(
    [
      { name : "cst_id",
        type : "INTEGER",
        description : "customer id from CRM"
      },
      { name : "cst_key",
        type : "STRING",
        description : "customer key from CRM"
      },

      { name : "cst_firstname",
        type : "STRING",
        description : "customer firstname from CRM"
      },

      { name : "cst_lastname",
        type : "STRING",
        description : "customer lastname from CRM"
      },

      { name : "cst_marital_status",
        type : "STRING",
        description : "customer marital status from CRM"
      },

      { name : "cst_gndr",
        type : "STRING",
        description : "customer gender from CRM"
      },

      { name : "cst_create_date",
        type : "DATE",
        description : "customer create_date from CRM"
      },
    ]
  )
}

resource "google_bigquery_table" "bronze_crm_prod_info" {
  dataset_id = google_bigquery_dataset.bronze_layer.dataset_id
  table_id   = "bronze_crm_prod_info"
  deletion_protection = false

  schema = jsonencode(
    [
      { name : "prd_id",
        type : "INTEGER",
        description : "product id from CRM"
      },

      { name : "prd_key",
        type : "STRING",
        description : "product key from CRM"
      },

      { name : "prd_nm",
        type : "STRING",
        description : "product number from CRM"
      },

      { name : "prd_cost",
        type : "INTEGER",
        description : "product cost from CRM"
      },

      { name : "prd_line",
        type : "STRING",
        description : "product line(type of product ig?) from CRM"
      },

      { name : "prd_start_dt",
        type : "DATETIME",
        description : "product start (the day product is created or in inventory, start of cost) from CRM"
      },

      { name : "prd_end_dt",
        type : "DATETIME",
        description : "product end (the day product is ended or in inventory, end of a particular cost) from CRM"
      },
    ]
  )
}


resource "google_bigquery_table" "bronze_crm_sales_details" {
  dataset_id = google_bigquery_dataset.bronze_layer.dataset_id
  table_id   = "bronze_crm_sales_details"
  deletion_protection = false

  schema = jsonencode(
    [
      { name : "sls_ord_num",
        type : "STRING",
        description : "sales order number from CRM"
      },

      { name : "sls_prd_key",
        type : "STRING",
        description : "sales product key from CRM"
      },

      { name : "sls_cust_id",
        type : "INTEGER",
        description : "customer whom the sales regards to from CRM"
      },

      { name : "sls_order_dt",
        type : "INTEGER",
        description : "order date from CRM"
      },

      { name : "sls_ship_dt",
        type : "INTEGER",
        description : "shipping date from CRM"
      },

      { name : "sls_due_dt",
        type : "INTEGER",
        description : "due date from CRM"
      },

      { name : "sls_sales",
        type : "INTEGER",
        description : "total cost of the product (quantity * price) from CRM"
      },
      { name : "sls_quantity",
        type : "INTEGER",
        description : "quantity of the product from CRM"
      },
      { name : "sls_price",
        type : "INTEGER",
        description : "price of the product from CRM"
      },
    ]
  )
}


#---------------------------ERP----------------------------------------------------

resource "google_bigquery_table" "bronze_erp_loc_a101" {
  dataset_id = google_bigquery_dataset.bronze_layer.dataset_id
  table_id   = "bronze_erp_loc_a101"
  deletion_protection = false

  schema = jsonencode(
    [
      { name : "cid",
        type : "STRING",
        description : "sales order number from CRM"
      },

      { name : "cntry",
        type : "STRING",
        description : "sales product key from CRM"
      },
    ]
  )
}

resource "google_bigquery_table" "bronze_erp_cust_az12" {
  dataset_id = google_bigquery_dataset.bronze_layer.dataset_id
  table_id   = "bronze_erp_cust_az12"
  deletion_protection = false

  schema = jsonencode(
    [
      { name : "cid",
        type : "STRING",
        description : "sales order number from CRM"
      },

      { name : "bdate",
        type : "DATE",
        description : "sales product key from CRM"
      },
      { name : "gen",
        type : "STRING",
        description : "sales order number from CRM"
      },
    ]
  )
}

resource "google_bigquery_table" "bronze_px_cat_g1v2" {
  dataset_id = google_bigquery_dataset.bronze_layer.dataset_id
  table_id   = "bronze_px_cat_g1v2"
  deletion_protection = false

  schema = jsonencode(
    [
      { name : "id",
        type : "STRING",
        description : "sales order number from CRM"
      },

      { name : "cat",
        type : "STRING",
        description : "sales product key from CRM"
      },

      { name : "suncat",
        type : "STRING",
        description : "sales product key from CRM"
      },

      { name : "maINTEGERenance",
        type : "STRING",
        description : "sales product key from CRM"
      },
    ]
  )
}

#---------------------------SILVER LAYER-------------------------------------------------

#-----------------------------CRM--------------------------------------------------------
resource "google_bigquery_dataset" "silver_layer" {
  dataset_id    = "retail_silver"
  friendly_name = "Silver Layer"
  description   = "This is the Silver layer of the Medallion Architecture, here first layer of transformed data from Bronze Layer is only copied"

  location                   = var.gcp_region
  delete_contents_on_destroy = true
}

resource "google_bigquery_table" "silver_crm_cust_info" {
  dataset_id = google_bigquery_dataset.silver_layer.dataset_id
  table_id   = "silver_crm_cust_info"
  deletion_protection = false

  schema = jsonencode(
    [
      { name : "cst_id"
        type : "INTEGER"
        mode : "REQUIRED"
        description : "customer id from bronze"
      },

      { name : "cst_key"
        type : "STRING"
        mode : "REQUIRED"
        description : "customer key from bronze"
      },

      { name : "cst_firstname"
        type : "STRING"
        mode : "REQUIRED"
        description : "customer firstname from bronze"
      },

      { name : "cst_lastname"
        type : "STRING"
        mode : "REQUIRED"
        description : "customer lastname from bronze"
      },

      { name : "cst_marital_status"
        type : "STRING"
        mode : "REQUIRED"
        description : "customer marital status from bronze"
      },

      { name : "cst_gndr"
        type : "STRING"
        mode : "REQUIRED"
        description : "customer gender from bronze"
      },

      { name : "cst_create_date"
        type : "DATE"
        mode : "REQUIRED"
        description : "customer create_date from bronze"
      },

      { name : "dwh_create_date",
        type : "DATETIME",
        description : "Time when the rows were added"
      },
    ]
  )
}

resource "google_bigquery_table" "silver_crm_prod_info" {
  dataset_id = google_bigquery_dataset.bronze_layer.dataset_id
  table_id   = "silver_crm_prod_info"
  deletion_protection = false

  schema = jsonencode(
    [
      { name : "prd_id",
        type : "INTEGER",
        description : "product id from bronze"
      },

      { name : "cat_id",
        type : "STRING",
        description : "splitting the product key from bronze"
      },

      { name : "prd_key",
        type : "STRING",
        description : "product number from bronze"
      },

      { name : "prd_nm",
        type : "STRING",
        description : "product number from bronze"
      },

      { name : "prd_cost",
        type : "INTEGER",
        description : "product cost from bronze"
      },

      { name : "prd_line",
        type : "STRING",
        description : "product line(type of product ig?) from bronze"
      },

      { name : "prd_start_dt",
        type : "DATE",
        description : "product start (the day product is created or in inventory, start of cost) from bronze"
      },

      { name : "prd_end_dt",
        type : "DATE",
        description : "product end (the day product is ended or in inventory, end of a particular cost) from bronze"
      },

      { name : "dwh_create_date",
        type : "DATETIME",
        description : "Time when the rows were added"
      },
    ]
  )
}

resource "google_bigquery_table" "silver_crm_sales_details" {
  dataset_id = google_bigquery_dataset.silver_layer.dataset_id
  table_id   = "silver_crm_sales_details"
  deletion_protection = false

  schema = jsonencode(
    [
      { name : "sls_ord_num",
        type : "STRING",
        description : "sales order number from CRM"
      },

      { name : "sls_prd_key",
        type : "STRING",
        description : "sales product key from CRM"
      },

      { name : "sls_cust_id",
        type : "INTEGER",
        description : "customer whom the sales regards to from CRM"
      },

      { name : "sls_order_dt",
        type : "INTEGER",
        description : "order date from CRM"
      },

      { name : "sls_ship_dt",
        type : "INTEGER",
        description : "shipping date from CRM"
      },

      { name : "sls_due_dt",
        type : "INTEGER",
        description : "due date from CRM"
      },

      { name : "sls_sales",
        type : "INTEGER",
        description : "total cost of the product (quantity * price) from CRM"
      },
      { name : "sls_quantity",
        type : "INTEGER",
        description : "quantity of the product from CRM"
      },
      { name : "sls_price",
        type : "INTEGER",
        description : "price of the product from CRM"
      },
      { name : "dwh_create_date",
        type : "DATETIME",
        description : "Time when the rows were added"
      },
    ]
  )
}


#---------------------------------ERP---------------------------------------------


resource "google_bigquery_table" "silver_erp_loc_a101" {
  dataset_id = google_bigquery_dataset.silver_layer.dataset_id
  table_id   = "silver_erp_loc_a101"
  deletion_protection = false

  schema = jsonencode(
    [
      { name : "cid",
        type : "STRING",
        description : "sales order number from CRM"
      },

      { name : "cntry",
        type : "STRING",
        description : "sales product key from CRM"
      },

      { name : "dwh_create_date",
        type : "DATETIME",
        description : "Time when the rows were added"
      },
    ]
  )
}

resource "google_bigquery_table" "silver_erp_cust_az12" {
  dataset_id = google_bigquery_dataset.silver_layer.dataset_id
  table_id   = "silver_erp_cust_az12"
  deletion_protection = false

  schema = jsonencode(
    [
      { name : "cid",
        type : "STRING",
        description : "sales order number from CRM"
      },

      { name : "bdate",
        type : "DATE",
        description : "sales product key from CRM"
      },
      { name : "gen",
        type : "STRING",
        description : "sales order number from CRM"
      },

      { name : "dwh_create_date",
        type : "DATETIME",
        description : "Time when the rows were added"
      },
    ]
  )
}

resource "google_bigquery_table" "silver_px_cat_g1v2" {
  dataset_id = google_bigquery_dataset.silver_layer.dataset_id
  table_id   = "silver_px_cat_g1v2"
  deletion_protection = false

  schema = jsonencode(
    [
      { name : "id",
        type : "STRING",
        description : "sales order number from CRM"
      },

      { name : "cat",
        type : "STRING",
        description : "sales product key from CRM"
      },

      { name : "suncat",
        type : "STRING",
        description : "sales product key from CRM"
      },

      { name : "maINTEGERenance",
        type : "STRING",
        description : "sales product key from CRM"
      },

      { name : "dwh_create_date",
        type : "DATETIME",
        description : "Time when the rows were added"
      }
    ]
  )
}




#------------------------------GOLD LAYER --------------------------------------------

resource "google_bigquery_dataset" "gold_layer" {
  dataset_id    = "retail_gold"
  friendly_name = "Gold Layer"
  description   = "This is the Gold layer of the Medallion Architecture, this equivalent of a View in SQL"

  location                   = var.gcp_region
  delete_contents_on_destroy = true
}


resource "google_bigquery_table" "gold_dim_customers" {
  dataset_id = google_bigquery_dataset.gold_layer.dataset_id
  table_id   = "gold_dim_customers"
  deletion_protection = false

  view {
    use_legacy_sql = false
    query = <<-EOF
                  SELECT 
                    ROW_NUMBER() OVER (ORDER BY cst_id) AS customer_key,
                    ci.cst_id AS customer_id, 
                    ci.cst_key AS customer_number, 
                    ci.cst_firstname AS first_name, 
                    ci.cst_lastname AS last_name, 
                    la.cntry AS country, 
                    ci.cst_marital_status AS marital_status, 
                    CASE 
                      WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr 
                      ELSE COALESCE(ca.gen, 'n/a') 
                    END AS gender,
                    ca.bdate AS birthdate, 
                    ci.cst_create_date AS create_date 
                  FROM `${google_bigquery_dataset.silver_layer.dataset_id}.silver_crm_cust_info` ci 
                  LEFT JOIN `${google_bigquery_dataset.silver_layer.dataset_id}.silver_erp_cust_az12` ca 
                    ON ci.cst_key = ca.cid 
                  LEFT JOIN `${google_bigquery_dataset.silver_layer.dataset_id}.silver_erp_loc_a101` la 
                    ON ci.cst_key = la.cid
EOF



  }

  depends_on = [  google_bigquery_table.silver_crm_cust_info, 
                  google_bigquery_table.silver_erp_cust_az12, 
                  google_bigquery_table.silver_erp_loc_a101 ]

}


resource "google_bigquery_table" "gold_dim_products" {
  dataset_id = google_bigquery_dataset.gold_layer.dataset_id
  table_id   = "gold_dim_products"
  deletion_protection = false

  view {
    use_legacy_sql = false
    
    # Using <<EOF prevents indentation errors. 
    # Make sure the closing 'EOF' is at the very start of the line.
    query = <<-EOF
                  SELECT
                    ROW_NUMBER() OVER (ORDER BY pn.prd_start_dt, pn.prd_key) AS product_key,
                    pn.prd_id       AS product_id,
                    pn.prd_key      AS product_number,
                    pn.prd_nm       AS product_name,
                    pn.cat_id       AS category_id,
                    pc.cat          AS category,
                    pc.subcat       AS subcategory,
                    pc.maintenence  AS maintenence,
                    pn.prd_cost     AS cost,
                    pn.prd_line     AS product_line,
                    pn.prd_start_dt AS start_date
                  FROM `${google_bigquery_dataset.silver_layer.dataset_id}.crm_prod_info` pn
                  LEFT JOIN `${google_bigquery_dataset.silver_layer.dataset_id}.erp_px_cat_g1v2` pc
                    ON pn.cat_id = pc.id
                  WHERE pn.prd_end_dt IS NULL    
EOF
  }

  depends_on = [  google_bigquery_table.silver_crm_cust_info,
                  google_bigquery_table.silver_px_cat_g1v2 ]
}

resource "google_bigquery_table" "gold_fact_sales" {
  dataset_id = google_bigquery_dataset.gold_layer.dataset_id
  table_id   = "gold_dim_sales"
  deletion_protection = false

  view {
    use_legacy_sql = false
    query = <<-EOF
                    SELECT
                      sd.sls_ord_num  AS order_number,
                      pr.product_key  AS product_key,
                      cu.customer_key AS customer_key,
                      sd.sls_order_dt AS order_date,
                      sd.sls_ship_dt  AS shipping_date,
                      sd.sls_due_dt   AS due_date,
                      sd.sls_sales    AS sales_amount,
                      sd.sls_quantity AS quantity,
                      sd.sls_price    AS price
                    FROM `${google_bigquery_dataset.silver_layer.dataset_id}.crm_sales_details` sd
                    LEFT JOIN `${google_bigquery_dataset.gold_layer.dataset_id}.dim_products pr
                      ON sd.sls_prd_key = pr.product_number
                    LEFT JOIN `${google_bigquery_dataset.gold_layer.dataset_id}.dim_products` cu
                      ON sd.sls_cust_id = cu.customer_id
EOF
  }
  depends_on = [  google_bigquery_table.gold_dim_customers,
                  google_bigquery_table.gold_dim_products ]
}

