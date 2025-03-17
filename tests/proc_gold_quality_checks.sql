/*
===============================================================================
Quality Checks for Gold Layer
===============================================================================
Script Purpose:
    This stored procedure performs quality checks to validate the integrity,
    consistency, and accuracy of the Gold Layer. These checks ensure:
    - Uniqueness of surrogate keys in dimension tables.
    - Referential integrity between fact and dimension tables.
    - Validation of relationships in the data model for analytical purposes.

Usage:
    CALL gold.run_quality_checks();
===============================================================================
*/

CREATE OR REPLACE PROCEDURE gold.run_quality_checks()
LANGUAGE plpgsql
AS $$
DECLARE 
    failed_checks INT := 0;
BEGIN
    -- Checking 'gold.dim_customers' for unique surrogate keys
    RAISE NOTICE 'Checking uniqueness of customer_skey in gold.dim_customers...';
    IF EXISTS (
        SELECT 1 FROM (
            SELECT customer_skey, COUNT(*) AS duplicate_count
            FROM gold.dim_customers
            GROUP BY customer_skey
            HAVING COUNT(*) > 1
        ) AS subquery
    ) THEN
        RAISE NOTICE '❌ Duplicate customer_skeys found!';
        failed_checks := failed_checks + 1;
    END IF;
    
    -- Checking customer_gender standardization
    RAISE NOTICE 'Checking data standardization in gold.dim_customers...';
    IF EXISTS (
        SELECT DISTINCT customer_gender 
        FROM gold.dim_customers 
        WHERE customer_gender NOT IN ('n/a', 'Female', 'Male')
    ) THEN
        RAISE NOTICE '❌ Unexpected values found in customer_gender column!';
        failed_checks := failed_checks + 1;
    END IF;
    
    -- Checking 'gold.dim_products' for unique surrogate keys
    RAISE NOTICE 'Checking uniqueness of product_skey in gold.dim_products...';
    IF EXISTS (
        SELECT 1 FROM (
            SELECT product_skey, COUNT(*) AS duplicate_count
            FROM gold.dim_products
            GROUP BY product_skey
            HAVING COUNT(*) > 1
        ) AS subquery
    ) THEN
        RAISE NOTICE '❌ Duplicate product_skeys found!';
        failed_checks := failed_checks + 1;
    END IF;
    
    -- Checking referential integrity in 'gold.fact_sales'
    RAISE NOTICE 'Checking referential integrity between fact_sales and dimensions...';
    IF EXISTS (
        SELECT 1 FROM gold.fact_sales fs
        LEFT JOIN gold.dim_customers dc ON dc.customer_skey = fs.customer_skey
        LEFT JOIN gold.dim_products dp ON dp.product_skey = fs.product_skey
        WHERE dc.customer_skey IS NULL OR dp.product_skey IS NULL
    ) THEN
        RAISE NOTICE '❌ Orphaned records found in gold.fact_sales!';
        failed_checks := failed_checks + 1;
    END IF;
    
    -- Final result
    IF failed_checks = 0 THEN
        RAISE NOTICE '✅ All quality checks passed successfully!';
    ELSE
        RAISE EXCEPTION 'Data quality validation failed with % issues!', failed_checks;
    END IF;
END;
$$;
