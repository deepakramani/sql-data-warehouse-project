/*
===============================================================================
Quality Checks
===============================================================================
Script Purpose:
    This script performs quality checks to validate the integrity, consistency, 
    and accuracy of the Gold Layer. These checks ensure:
    - Uniqueness of surrogate keys in dimension tables.
    - Referential integrity between fact and dimension tables.
    - Validation of relationships in the data model for analytical purposes.

Usage Notes:
    - Investigate and resolve any discrepancies found during the checks.
===============================================================================
*/

-- ====================================================================
-- Checking 'gold.dim_customers'
-- ====================================================================
-- Check for Uniqueness of Customer Surrogate key in gold.dim_customers
-- Expectation: No results 
SELECT 
    customer_skey,
    COUNT(*) AS duplicate_count
FROM gold.dim_customers
GROUP BY customer_skey
HAVING COUNT(*) > 1;

-- Check for data standardisation and consistency in customer_gender
-- Expection: only 'n/a', 'Female' and 'Male' values should be present
SELECT DISTINCT
    customer_gender
FROM
    gold.dim_customers;

-- ====================================================================
-- Checking 'gold.product_key'
-- ====================================================================
-- Check for Uniqueness of Product Key in gold.dim_products
-- Expectation: No results 
SELECT 
    product_skey,
    COUNT(*) AS duplicate_count
FROM gold.dim_products
GROUP BY product_skey
HAVING COUNT(*) > 1;

-- ====================================================================
-- Checking 'gold.fact_sales'
-- ====================================================================
-- Check the data model connectivity between fact and dimensions
-- Expected result: no rows returned
SELECT
    *
FROM
    gold.fact_sales fs
    LEFT JOIN gold.dim_customers dc ON dc.customer_skey = fs.customer_key
    LEFT JOIN gold.dim_products dp ON dp.product_skey = fs.product_key
WHERE
    dc.customer_skey IS NULL
    OR dp.product_skey IS NULL;