--Check for Nulls or Duplicates in Primary Key
--Expectation: No Result
USE DataWarehouse;

SELECT
    *
FROM bronze.crm_cust_info

SELECT cst_id, COUNT(*) FROM bronze.crm_cust_info
GROUP BY cst_id 
HAVING COUNT(*) > 1 OR cst_id IS NULL;


SELECT * FROM bronze.crm_cust_info
WHERE cst_id IN (29449, 29473, 29433, 29483, 29466)
    OR cst_id IS NULL;


-- Removed Duplicated values --
SELECT
*
FROM (
    SELECT 
        *,
        ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) flag_last
        FROM bronze.crm_cust_info
)t WHERE flag_last = 1;
-----------------------------------------------------------------------------------------------

-- Removing unwanted spaces-----------------------

SELECT
    cst_id,
    cst_key,
    TRIM(cst_firstname) AS cst_firstname,
    TRIM(cst_lastname) AS cst_lastname,
    CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
         WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
         ELSE 'N/A'
    END AS cst_marital_status,
    CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
         WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
         ELSE 'N/A'
    END AS cst_gndr,
    cst_create_date
FROM (
    SELECT 
        *,
        ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) flag_last
        FROM bronze.crm_cust_info
        WHERE cst_id IS NOT NULL    
)t
WHERE flag_last = 1;


SELECT * from silver.crm_cust_info


------------------------------------------------------------------------------------------------------

SELECT * from bronze.crm_prod_info
SELECT * from bronze.crm_sales_details
SELECT * from bronze.erp_px_cat_g1v2


SELECT
    prd_id,
    REPLACE(SUBSTRING(prd_key, 1,5), '-', '_') AS cat_id,
    SUBSTRING(prd_key, 7,LEN(prd_key)) AS prd_key,
    TRIM(prd_nm),
    ISNULL(prd_cost, 0) as prd_cost,
    CASE
        WHEN  UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
        WHEN  UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
        WHEN  UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
        WHEN  UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
        ELSE 'N/A'
    END AS prd_line,
    prd_start_dt,
    DATEADD(day, -1, LEAD(prd_start_dt) OVER  (PARTITION BY prd_key ORDER BY prd_start_dt)) AS prd_end_dt
FROM bronze.crm_prod_info

SELECT * from silver.crm_prod_info

------------------------------------------------------------------------------------

SELECT * from bronze.crm_sales_details

SELECT
sls_ord_num,
sls_prd_key,
sls_cust_id,
    CASE 
        WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
        ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
    END AS sls_order_dt,
    CASE 
        WHEN sls_order_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
        ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
    END AS sls_ship_dt,
    CASE 
        WHEN sls_order_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
        ELSE CAST(CAST(sls_due_dt AS VARCHAR)AS DATE)
    END AS sls_due_dt,
    CASE
        WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price) 
            THEN sls_quantity * ABS(sls_price)
        ELSE sls_sales
    END AS sls_sales,
    sls_quantity,
    CASE
        WHEN sls_price IS NULL OR sls_price <= 0
            THEN sls_sales / NULLIF(sls_quantity, 0)
        ELSE sls_price
    END AS sls_price
FROM bronze.crm_sales_details

SELECT
    case
        when cid like 'NAS%' then SUBSTRING(cid, 4, LEN(cid))
        else cid
    end as cid,
    case
        when bdate > GETDATE() then NULL
        else bdate
    end as bdate,
    CASE 
        WHEN upper(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
        WHEN upper(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
        ELSE 'N/A'
    end as gen
    FROM bronze.erp_cust_az12;

----------------------------------------------------------------------------------------------------------------------------


SELECT
    ci.cst_id,
    ci.cst_key,
    ci.cst_firstname,
    ci.cst_lastname,
    ci.cst_marital_status,
    CASE WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr
        else coalesce(ca.gen, 'n/a')
    END as gender,
    ci.cst_create_date,
    ca.bdate,
    ca.gen,
    la.country
from silver.crm_cust_info as ci
LEFT JOIN  silver.erp_cust_az12 as ca
ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 as la
on ci.cst_key = la.cid

