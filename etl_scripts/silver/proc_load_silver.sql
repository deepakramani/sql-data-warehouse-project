/*
This stored procedure performs the ETL process of taking raw data from bronze layer tables, cleanses them and populates silver layer tables.
In this stored procedure, silver layer tables are first truncated and then populated.


*/
CREATE OR REPLACE PROCEDURE silver.load_silver ()
LANGUAGE plpgsql
AS $$
DECLARE
    batch_start_time TIMESTAMP;
    batch_end_time TIMESTAMP;
    start_time TIMESTAMP;
    end_time TIMESTAMP;
BEGIN
    batch_start_time := clock_timestamp();
    RAISE NOTICE '================================================';
    RAISE NOTICE 'Loading Silver Layer';
    RAISE NOTICE '================================================';

    -- CRM Tables
    RAISE NOTICE '------------------------------------------------';
    RAISE NOTICE 'Loading CRM Tables';
    RAISE NOTICE '------------------------------------------------';

    -- crm_cust_info
    start_time := clock_timestamp();
    RAISE NOTICE '>> Truncating Table: silver.crm_cust_info';
    TRUNCATE TABLE silver.crm_cust_info;
    RAISE NOTICE '>> Inserting Data Into: silver.crm_cust_info';

    WITH latest_cstid_nodup_data AS (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY
                    cst_id
                ORDER BY
                    cst_create_date desc
            ) flag_latest
        FROM
            bronze.crm_cust_info
        WHERE
            cst_id IS NOT NULL
    ),
    cleaned_bronze_crm_custinfo AS (
        SELECT
            cst_id,
            cst_key,
            TRIM(cst_firstname) AS cst_firstname,
            TRIM(cst_lastname) AS cst_lastname,
            CASE
                WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
                WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
                ELSE 'n/a'
            END AS cst_marital_status,
            CASE
                WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
                WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
                ELSE 'n/a'
            END AS cst_gndr,
            cst_create_date
        FROM
            latest_cstid_nodup_data
        WHERE
            flag_latest = 1
    )
    INSERT INTO
        silver.crm_cust_info
    SELECT
        *
    FROM
        cleaned_bronze_crm_custinfo;
    
    end_time := clock_timestamp();
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));

    -- silver.crm_prd_info
    start_time := clock_timestamp();
    RAISE NOTICE '>> Truncating Table: silver.crm_prd_info';
    TRUNCATE TABLE silver.crm_prd_info;
    RAISE NOTICE '>> Inserting Data Into: silver.crm_prd_info';
    WITH cleaned_prd_info AS (
    SELECT
        prd_id,
        REPLACE(SUBSTR(prd_key, 1, 5), '-', '_') AS cat_id,
        SUBSTR(prd_key, 7, LENGTH(prd_key)) AS prd_key,
        prd_nm,
        COALESCE(prd_cost, 0) AS prd_cost,
        CASE
            WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountains'
            WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
            WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Sales'
            WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
            ELSE 'n/a'
        END AS prd_line,
        CAST(prd_start_dt AS DATE) AS prd_start_date
    FROM bronze.crm_prd_info
    ),
    final_prd_info AS (
        SELECT
            *,
            LEAD(prd_start_date, 1) OVER (
                PARTITION BY prd_key
                ORDER BY prd_start_date
            ) - 1 AS prd_end_dt
        FROM cleaned_prd_info
    )
    INSERT INTO silver.crm_prd_info
    SELECT * FROM final_prd_info;
    
    end_time := clock_timestamp();
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));

    -- silver.crm_sales_details
    start_time := clock_timestamp();
    RAISE NOTICE '>> Truncating Table: silver.crm_sales_details';
    TRUNCATE TABLE silver.crm_sales_details;
    RAISE NOTICE '>> Inserting Data Into: silver.crm_sales_details';

    WITH cleaned_sales_dates AS (
        SELECT 
            sls_ord_num,
            sls_prd_key,
            sls_cust_id,
            CASE 
                WHEN sls_order_dt = 0 OR LENGTH(sls_order_dt::VARCHAR) != 8 THEN NULL
                ELSE sls_order_dt::VARCHAR::DATE 
            END AS sls_order_dt,
            CASE 
                WHEN sls_ship_dt = 0 OR LENGTH(sls_ship_dt::VARCHAR) != 8 THEN NULL
                ELSE sls_ship_dt::VARCHAR::DATE 
            END AS sls_ship_dt,
            CASE 
                WHEN sls_due_dt = 0 OR LENGTH(sls_due_dt::VARCHAR) != 8 THEN NULL
                ELSE sls_due_dt::VARCHAR::DATE 
            END AS sls_due_dt,
            sls_quantity,
            sls_price,
            sls_sales
        FROM bronze.crm_sales_details
    ),
    calculated_sales AS (
        SELECT 
            *,
            CASE 
                WHEN sls_price IS NULL OR sls_price < 0 
                THEN sls_sales / NULLIF(sls_quantity, 0)
                ELSE sls_price 
            END AS adjusted_sls_price,
            CASE 
                WHEN sls_sales IS NULL OR sls_sales < 0 OR sls_sales != sls_quantity * ABS(sls_price) 
                THEN sls_quantity * ABS(sls_price)
                ELSE sls_sales 
            END AS adjusted_sls_sales
        FROM cleaned_sales_dates
    )
    INSERT INTO silver.crm_sales_details (
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        sls_order_dt,
        sls_ship_dt,
        sls_due_dt,
        sls_quantity,
        sls_price,
        sls_sales
    )
    SELECT 
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        sls_order_dt,
        sls_ship_dt,
        sls_due_dt,
        sls_quantity,
        adjusted_sls_price AS sls_price,
        adjusted_sls_sales AS sls_sales
    FROM calculated_sales;


    end_time := clock_timestamp();
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));

    -- ERP Tables
    RAISE NOTICE '------------------------------------------------';
    RAISE NOTICE 'Loading ERP Tables';
    RAISE NOTICE '------------------------------------------------';

    -- erp_loc_a101
    start_time := clock_timestamp();
    RAISE NOTICE '>> Truncating Table: silver.erp_loc_a101';
    TRUNCATE TABLE silver.erp_loc_a101;
    RAISE NOTICE '>> Inserting Data Into: silver.erp_loc_a101';

    INSERT INTO
        silver.erp_loc_a101 (cid, cntry)
    SELECT
        replace (cid, '-', '') AS cid,
        CASE
            WHEN TRIM(cntry) = 'DE' THEN 'Germany'
            WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
            WHEN TRIM(cntry) = ''
            OR TRIM(cntry) IS NULL THEN 'n/a'
            ELSE TRIM(cntry)
        END AS cntry
    FROM
        bronze.erp_loc_a101;
    

    end_time := clock_timestamp();
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));

    -- erp_cust_az12
    start_time := clock_timestamp();
    RAISE NOTICE '>> Truncating Table: silver.erp_cust_az12';
    TRUNCATE TABLE silver.erp_cust_az12;
    RAISE NOTICE '>> Inserting Data Into: silver.erp_cust_az12';

    INSERT INTO silver.erp_cust_az12
                (cid,
                bdate,
                gen)
    (SELECT CASE
            WHEN cid LIKE 'NAS%' THEN Substr(cid, 4, Length(cid))
            ELSE cid
            END AS cid,
            CASE
            WHEN bdate > Now() :: DATE THEN NULL
            ELSE bdate
            END AS bdate,
            CASE
            WHEN Upper(Trim(gen)) IN ( 'M', 'MALE' ) THEN 'Male'
            WHEN Upper(Trim(gen)) IN ( 'F', 'FEMALE' ) THEN 'Female'
            ELSE 'n/a'
            END AS gen
    FROM   bronze.erp_cust_az12); 

    end_time := clock_timestamp();
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));

    -- erp_px_cat_g1v2
    start_time := clock_timestamp();
    RAISE NOTICE '>> Truncating Table: silver.erp_px_cat_g1v2';
    TRUNCATE TABLE silver.erp_px_cat_g1v2;
    RAISE NOTICE '>> Inserting Data Into: silver.erp_px_cat_g1v2';
    INSERT INTO
        silver.erp_px_cat_g1v2 (id, cat, subcat, maintenance)
    SELECT
        TRIM(id) AS id,
        TRIM(cat) AS cat,
        TRIM(subcat) AS subcat,
        TRIM(maintenance) AS maintenance
    FROM
        bronze.erp_px_cat_g1v2;

    end_time := clock_timestamp();
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));

    batch_end_time := clock_timestamp();
    RAISE NOTICE '==========================================';
    RAISE NOTICE 'Loading Silver Layer is Completed';
    RAISE NOTICE '   - Total Load Duration: % seconds', EXTRACT(EPOCH FROM (batch_end_time - batch_start_time));
    RAISE NOTICE '==========================================';

EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE '==========================================';
    RAISE NOTICE 'ERROR OCCURRED DURING LOADING Silver LAYER';
    RAISE NOTICE 'Error Message: %', SQLERRM;
    RAISE NOTICE '==========================================';
END;
$$;
