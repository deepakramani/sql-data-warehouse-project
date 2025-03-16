/*
This sql script file contains all the individual quality checks performed on silver layer tables.
*/

-- Checking `silver.crm_cust_info` table
-- 1. check for nulls or duplicates in primary key
-- expected result - no rows
SELECT
    cst_id,
    COUNT(*)
FROM
    silver.crm_cust_info
GROUP BY
    cst_id
HAVING
    COUNT(*) > 1
    OR cst_id IS NULL;

-- 2. Check for white spaces in firstname and lastname
-- expected result: zero rows
SELECT
    *
FROM
    silver.crm_cust_info
WHERE
    cst_firstname != TRIM(cst_firstname);

SELECT
    *
FROM
    silver.crm_cust_info
WHERE
    cst_lastname != TRIM(cst_lastname);

-- 3. Data Standardisation or normalisation
-- Check whether marital status column is standardised with only Single and Married values
SELECT DISTINCT
    cst_marital_status
FROM
    silver.crm_cust_info;

-- Check whether gender column is standardised with only Male, Female and n/a values
SELECT DISTINCT
    cst_gndr
FROM
    silver.crm_cust_info;

SELECT
    *
FROM
    silver.crm_cust_info;

-- Checking `silver.crm_prd_info` table
-- 1. check for nulls or duplicates in primary key
-- expected result - no rows
SELECT
    cst_id,
    COUNT(*)
FROM
    silver.crm_prd_info
GROUP BY
    prd_id_id
HAVING
    COUNT(*) > 1
    OR prd_id IS NULL;

-- 2. Check for white spaces in prd_name
-- expected result: zero rows
SELECT
    prd_nm
FROM
    bronze.crm_prd_info
WHERE
    prd_nm != TRIM(prd_nm);

-- 3. Check whether prd_cost have negative or null values
-- expected result: zero rows
SELECT
    prd_cost
FROM
    silver.crm_prd_info
WHERE
    prd_cost < 0
    OR prd_cost IS NULL;

-- 4. Check data standardisation and consistency in prd_line column
-- expected result: Only values 'Mountain', 'Road', 'Sales', 'Touring', 'n/a' should be present.
SELECT DISTINCT
    prd_line
FROM
    silver.crm_prd_info;

-- 5. Check if prd_end_dr is less than prd_start_dt
-- expected result: zero rows
SELECT
    *
FROM
    silver.crm_prd_info
WHERE
    prd_end_dt < prd_start_dt;

-- Checking crm.sales_details table
-- 1. Check for white spaces in sls_order_num and sls_prd_key
-- expected result: zero rows
SELECT
    sls_ord_num
FROM
    silver.crm_sales_details
WHERE
    sls_ord_num != TRIM(sls_ord_num);

SELECT
    sls_prd_key
FROM
    silver.crm_sales_details
WHERE
    sls_prd_key != TRIM(sls_prd_key);

-- 2. check if sls_order_dt has 0 as values or its length 'YYYYMMDD' is not equal to 8
-- expected result: zero rows or 19 rows which originally had 0 as its value now converted to null.
-- select nullif(sls_order_dt,0) :: varchar :: date as sls_order_dt
-- from bronze.crm_sales_details
-- where sls_order_dt <=0
--     or length(sls_order_dt:: varchar) !=8
--     or sls_order_dt > 20500101 -- higher date value
--     or sls_order_dt < 19000101 -- lowest date value

SELECT
    sls_order_dt
FROM
    silver.crm_sales_details
WHERE
    sls_order_dt IS NULL
    OR sls_order_dt > '2050-01-01' -- higher date value
    OR sls_order_dt < '1900-01-01' -- lowest date value

-- 3. check if sls_order_dt is greater than sls_ship_dt or sls_due_dt
-- expected result: zero rows
SELECT
    *
FROM
    silver.crm_sales_details
WHERE
    sls_order_dt > sls_ship_dt
    OR sls_order_dt > sls_due_dt;

-- 4. Check whether sls_sales, sales_quantity and sales_price are not negative or null.
-- expected result: zero rows
SELECT
    sls_sales,
    sls_quantity,
    sls_price
FROM
    silver.crm_sales_details
WHERE
    sls_sales != sls_quantity * sls_price
    OR sls_sales IS NULL
    OR sls_quantity IS NULL
    OR sls_price IS NULL
    OR sls_sales < 0
    OR sls_quantity < 0
    OR sls_price < 0;

--- Checking silver.erp_cust_az12 table
-- 1. Check if bdate is out of range(more than 100 years old or born after today)
select distinct bdate
from silver.erp_cust_az12
where bdate < '1925-01-01' or bdate > now()::date;

-- 2. Data Standardization & Consistency on gen column to have only Male, Female and n/a values
SELECT DISTINCT 
    gen 
FROM silver.erp_cust_az12;

--- Checking silver.erp_loc_a101 table
-- 1. check if cntry doesn't have white spaces or has only distinct values in Germany, United States, France, United Kingdom, Australia, Canada and 'n/a'.
SELECT
    *
FROM
    silver.erp_loc_a101
WHERE
    cntry != TRIM(cntry);

SELECT DISTINCT
    cntry
FROM
    silver.erp_loc_a101;

-- Checking silver.erp_px_cat_g1v2
-- 1. -- Check for Unwanted Spaces
-- Expectation: No Results
SELECT 
    * 
FROM silver.erp_px_cat_g1v2
WHERE cat != TRIM(cat) 
   OR subcat != TRIM(subcat) 
   OR maintenance != TRIM(maintenance);

-- 2, Data Standardization & Consistency on maintenance column to have only yes and no values
SELECT DISTINCT
    maintenance
FROM
    silver.erp_px_cat_g1v2;