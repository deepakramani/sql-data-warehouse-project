/*
Gold(business) layer DDL script

This script is used to create gold layer views

*/


-- Create gold.dim_customers dimensional view
DROP VIEW IF EXISTS gold.dim_customers CASCADE;
CREATE VIEW gold.dim_customers as (
SELECT
    ROW_NUMBER() OVER (
        ORDER BY
            ci.cst_id
    ) AS customer_skey, -- surrogate key
    ci.cst_id AS customer_id,
    ci.cst_key AS customer_key,
    ci.cst_firstname AS customer_firstname,
    ci.cst_lastname AS customer_lastname,
    ca.bdate AS customer_birthdate,
    CASE
        WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr -- CRM is the master/preferred value
        ELSE COALESCE(ca.gen, 'n/a')
    END AS customer_gender,
    ci.cst_marital_status AS customer_marital_status,
    la.cntry AS customer_country,
    ci.cst_create_date AS create_date
FROM
    silver.crm_cust_info ci
    LEFT JOIN silver.erp_cust_az12 ca ON ci.cst_key = ca.cid
    LEFT OUTER JOIN silver.erp_loc_a101 la ON ci.cst_key = la.cid);


-- Create gold.dim_products dimensional view
DROP VIEW if EXISTS gold.dim_products CASCADE;

CREATE OR REPLACE view gold.dim_products AS (
    SELECT
        ROW_NUMBER() OVER (
            ORDER BY
                pi.prd_id
        ) AS product_skey, -- surrogate key
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
    FROM
        silver.crm_prd_info pi
        LEFT JOIN silver.erp_px_cat_g1v2 pc ON pi.cat_id = pc.id
    WHERE
        pi.prd_end_dt IS NULL
); -- filter out historical data


-- Create view gold.fact_sales
DROP VIEW IF EXISTS gold.fact_sales CASCADE;
CREATE OR REPLACE VIEW gold.fact_sales AS (
    SELECT
        MD5(sd.sls_ord_num
             || dp.product_skey::VARCHAR
             || dc.customer_skey::VARCHAR) AS sales_details_skey, -- Composite surrogate key
        dp.product_skey AS product_key,
        dc.customer_skey AS customer_key,
        sd.sls_ord_num AS sales_order_number,
        sd.sls_order_dt AS sales_order_date,
        sd.sls_ship_dt AS sales_shipping_date,
        sd.sls_due_dt AS sales_due_date,
        sd.sls_sales AS sales_amount,
        sd.sls_quantity AS sales_quantity,
        sd.sls_price AS sales_price
    FROM
        silver.crm_sales_details sd
        LEFT JOIN gold.dim_customers dc ON sd.sls_cust_id = dc.customer_id
        LEFT JOIN gold.dim_products dp ON sd.sls_prd_key = dp.product_key
);