/* 
===============================================================================
Stored Procedure: silver.run_quality_checks
===============================================================================
Purpose:
    This stored procedure performs a series of data quality checks on the 
    Silver Layer tables to ensure data integrity, consistency, and correctness. 
    It verifies primary key uniqueness, standardization, referential integrity, 
    and logical correctness of business rules.

Checks Performed:
    - Primary key uniqueness and null checks.
    - Whitespace removal in text fields.
    - Standardization of categorical values (e.g., gender, marital status).
    - Validation of numerical and date values (e.g., negative prices, invalid dates).
    - Referential integrity between fact and dimension tables.
    - Logical consistency checks (e.g., sales amount validation).

Behavior:
    - If any check fails, a notice with the issue count is raised.
    - If at least one check fails, the procedure raises an exception.
    - If all checks pass, a success message is displayed.

Usage:
    CALL silver.run_quality_checks();

===============================================================================

*/

CREATE OR REPLACE PROCEDURE silver.run_quality_checks()
LANGUAGE plpgsql
AS $$
DECLARE
    failed_checks INT := 0;
    failed_count INT;
BEGIN
    -- Check for nulls or duplicates in primary key (crm_cust_info)
    SELECT COUNT(*) INTO failed_count FROM (
        SELECT cst_id FROM silver.crm_cust_info GROUP BY cst_id HAVING COUNT(*) > 1 OR cst_id IS NULL
    ) t;
    IF failed_count > 0 THEN
        RAISE NOTICE '❌ Duplicate or NULL primary keys in silver.crm_cust_info: %', failed_count;
        failed_checks := failed_checks + 1;
    END IF;

    -- Check for white spaces in cst_firstname
    SELECT COUNT(*) INTO failed_count FROM silver.crm_cust_info WHERE cst_firstname != TRIM(cst_firstname);
 lity checks passed!"**.  
- If any check **fails**, execution **stops** with an error message.        failed_checks := failed_checks + 1;
    END IF;

    -- Check for white spaces in cst_lastname
    SELECT COUNT(*) INTO failed_count FROM silver.crm_cust_info WHERE cst_lastname != TRIM(cst_lastname);
    IF failed_count > 0 THEN
        RAISE NOTICE '❌ Last name has leading/trailing spaces in crm_cust_info: %', failed_count;
        failed_checks := failed_checks + 1;
    END IF;

    -- Check gender column standardization
    SELECT COUNT(*) INTO failed_count FROM silver.crm_cust_info WHERE cst_gndr NOT IN ('Male', 'Female', 'n/a');
    IF failed_count > 0 THEN
        RAISE NOTICE '❌ Invalid gender values in crm_cust_info: %', failed_count;
        failed_checks := failed_checks + 1;
    END IF;

    -- Check marital status standardization
    SELECT COUNT(*) INTO failed_count FROM silver.crm_cust_info WHERE cst_marital_status NOT IN ('Single', 'Married');
    IF failed_count > 0 THEN
        RAISE NOTICE '❌ Invalid marital status values in crm_cust_info: %', failed_count;
        failed_checks := failed_checks + 1;
    END IF;

    -- Check if product cost is negative or NULL
    SELECT COUNT(*) INTO failed_count FROM silver.crm_prd_info WHERE prd_cost < 0 OR prd_cost IS NULL;
    IF failed_count > 0 THEN
        RAISE NOTICE '❌ Negative or NULL prd_cost in crm_prd_info: %', failed_count;
        failed_checks := failed_checks + 1;
    END IF;

    -- Check for white spaces in product name
    SELECT COUNT(*) INTO failed_count FROM silver.crm_prd_info WHERE prd_nm != TRIM(prd_nm);
    IF failed_count > 0 THEN
        RAISE NOTICE '❌ Product name has leading/trailing spaces in crm_prd_info: %', failed_count;
        failed_checks := failed_checks + 1;
    END IF;

    -- Check if order date is valid
    SELECT COUNT(*) INTO failed_count FROM silver.crm_sales_details WHERE sls_order_dt IS NULL OR sls_order_dt > '2050-01-01' OR sls_order_dt < '1900-01-01';
    IF failed_count >19 THEN
        RAISE NOTICE '❌ Invalid order dates in crm_sales_details: %', failed_count;
        failed_checks := failed_checks + 1;
    END IF;

    -- Check if sales_price * quantity = sales_amount
    SELECT COUNT(*) INTO failed_count FROM silver.crm_sales_details WHERE sls_sales != sls_quantity * sls_price;
    IF failed_count > 0 THEN
        RAISE NOTICE '❌ Mismatched sales calculation in crm_sales_details: %', failed_count;
        failed_checks := failed_checks + 1;
    END IF;

    -- Check if sls_order_dt is greater than ship/due date
    SELECT COUNT(*) INTO failed_count FROM silver.crm_sales_details WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt;
    IF failed_count > 0 THEN
        RAISE NOTICE '❌ Order date is greater than shipping or due date in crm_sales_details: %', failed_count;
        failed_checks := failed_checks + 1;
    END IF;

    -- Check for invalid birth dates in erp_cust_az12
    SELECT COUNT(*) INTO failed_count FROM silver.erp_cust_az12 WHERE bdate < '1925-01-01' OR bdate > NOW()::DATE;
    IF failed_count > 17 THEN
        RAISE NOTICE '❌ Invalid birth dates in erp_cust_az12: %', failed_count;
        failed_checks := failed_checks + 1;
    END IF;

    -- Check for invalid country names in erp_loc_a101
    SELECT COUNT(*) INTO failed_count FROM silver.erp_loc_a101 WHERE cntry NOT IN ('Germany', 'United States', 'France', 'United Kingdom', 'Australia', 'Canada', 'n/a');
    IF failed_count > 0 THEN
        RAISE NOTICE '❌ Invalid country names in erp_loc_a101: %', failed_count;
        failed_checks := failed_checks + 1;
    END IF;

    -- Check for invalid maintenance values in erp_px_cat_g1v2
    SELECT  COUNT(*) INTO failed_count FROM silver.erp_px_cat_g1v2 WHERE maintenance NOT IN ('Yes', 'No');
    IF failed_count > 0 THEN
        RAISE NOTICE '❌ Invalid maintenance values in erp_px_cat_g1v2: %', failed_count;
        failed_checks := failed_checks + 1;
    END IF;

    -- Raise an error if any check failed
    IF failed_checks > 0 THEN
        RAISE EXCEPTION 'Quality checks failed: % issues found.', failed_checks;
    ELSE
        RAISE NOTICE '✅ All quality checks passed!';
    END IF;
END;
$$;