/*
Gold(business) layer DDL script

This script is used to create gold layer views using CTEs for better readability.

*/

-- Create gold.dim_customers dimensional view
DROP VIEW IF EXISTS gold.dim_customers CASCADE;
CREATE VIEW gold.dim_customers AS (
    WITH customer_data AS (
        SELECT
            ci.cst_id AS customer_id,
            ci.cst_key AS customer_key,
            ci.cst_firstname AS customer_firstname,
            ci.cst_lastname AS customer_lastname,
            ca.bdate AS customer_birthdate,
            CASE
                WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr
                ELSE COALESCE(ca.gen, 'n/a')
            END AS customer_gender,
            ci.cst_marital_status AS customer_marital_status,
            la.cntry AS customer_country,
            ci.cst_create_date AS create_date
        FROM silver.crm_cust_info ci
        LEFT JOIN silver.erp_cust_az12 ca ON ci.cst_key = ca.cid
        LEFT JOIN silver.erp_loc_a101 la ON ci.cst_key = la.cid
    )
    SELECT
        ROW_NUMBER() OVER (ORDER BY customer_id) AS customer_skey,
        *
    FROM customer_data
);


-- Create gold.dim_products dimensional view
DROP VIEW IF EXISTS gold.dim_products CASCADE;
CREATE VIEW gold.dim_products AS (
    WITH product_data AS (
        SELECT
            pi.prd_id AS product_id,
            pi.prd_key AS product_key,
            pi.prd_nm AS product_name,
            pi.cat_id AS product_cat_id,
            pc.cat AS product_category,
            pc.subcat AS product_subcategory,
            pc.maintenance AS product_maintenance_flag,
            pi.prd_cost AS product_cost,
            pi.prd_line AS product_line,
            pi.prd_start_dt AS product_start_date
        FROM silver.crm_prd_info pi
        LEFT JOIN silver.erp_px_cat_g1v2 pc ON pi.cat_id = pc.id
        WHERE pi.prd_end_dt IS NULL -- filter out historical data
    )
    SELECT
        ROW_NUMBER() OVER (ORDER BY product_id) AS product_skey,
        *
    FROM product_data
);


-- Create gold.fact_sales view
DROP VIEW IF EXISTS gold.fact_sales CASCADE;
CREATE VIEW gold.fact_sales AS (
    WITH sales_data AS (
        SELECT
            sd.sls_ord_num AS sales_order_number,
            sd.sls_order_dt AS sales_order_date,
            sd.sls_ship_dt AS sales_shipping_date,
            sd.sls_due_dt AS sales_due_date,
            sd.sls_sales AS sales_amount,
            sd.sls_quantity AS sales_quantity,
            sd.sls_price AS sales_price,
            dp.product_skey,
            dc.customer_skey
        FROM silver.crm_sales_details sd
        LEFT JOIN gold.dim_customers dc ON sd.sls_cust_id = dc.customer_id
        LEFT JOIN gold.dim_products dp ON sd.sls_prd_key = dp.product_key
    )
    SELECT
        MD5(sales_order_number || product_skey::VARCHAR || customer_skey::VARCHAR) AS sales_details_skey,
        product_skey,
        customer_skey,
        sales_order_number,
        sales_order_date,
        sales_shipping_date,
        sales_due_date,
        sales_amount,
        sales_quantity,
        sales_price
    FROM sales_data
);
