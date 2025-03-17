SHELL:=/bin/bash

.PHONY: help install_docker setup_bronze_tables populate_bronze_tables setup_silver_tables populate_silver_tables setup_gold_layer test_silver_layer test_gold_layer

help:
	@echo "Usage: make [target]"
	@echo
	@echo "Recommended order to run commands:"
	@echo "  1. make install_docker         # Install Docker and prerequisites"
	@echo "  2. make up                     # Start the ETL DWH environment"
	@echo "  3. make setup_bronze_tables    # Create Bronze tables"
	@echo "  4. make populate_bronze_tables # Load raw data into Bronze tables"
	@echo "  5. make setup_silver_tables    # Create Silver tables"
	@echo "  6. make populate_silver_tables # Clean and transform data into Silver layer"
	@echo "  7. make setup_gold_layer       # Create Gold layer views"
	@echo "  8. make test_silver_layer      # Run quality checks on Silver layer"
	@echo "  9. make test_gold_layer        # Run quality checks on Gold layer"
	@echo " 10. make down                   # Stop and clean up the environment"
	@echo
	@echo "Run 'make <target>' to execute a specific step."

install_docker:
	source ./scripts/install_docker.sh 

pg:
	pgcli -h localhost -p 5432 -U ${POSTGRES_USER} -d ${POSTGRES_DB}

up:
	docker-compose up -d

down:
	docker-compose down -v

setup_bronze_tables:
	@echo "Creating bronze layer tables. Please wait..."
	docker exec -i dwh-crm-erp_container psql -U ${POSTGRES_USER} -d ${POSTGRES_DB} < ./etl_scripts/bronze/ddl_bronze.sql
	@sleep 5
	@echo -n "Bronze layer tables created"

populate_bronze_tables:
	@echo "Loading the bronze stored procedure"
	docker exec -i dwh-crm-erp_container psql -U ${POSTGRES_USER} -d ${POSTGRES_DB} < ./etl_scripts/bronze/proc_load_bronze.sql	
	@echo -n "Populating tables..."
	docker exec -i dwh-crm-erp_container psql -U ${POSTGRES_USER} -d ${POSTGRES_DB} -c "CALL bronze.load_bronze();"
	@sleep 10
	@echo -n "Bronze layer tables populated."

setup_silver_tables:
	@echo -n "Creating silver layer tables. Please wait..."
	docker exec -i dwh-crm-erp_container psql -U ${POSTGRES_USER} -d ${POSTGRES_DB} < ./etl_scripts/silver/ddl_silver.sql
	@sleep 5
	@echo -n "Silver layer tables created"

populate_silver_tables:
	@echo -n "Loading the silver stored procedure"
	docker exec -i dwh-crm-erp_container psql -U ${POSTGRES_USER} -d ${POSTGRES_DB} < ./etl_scripts/silver/proc_load_silver.sql	
	@echo -n "Populating tables..."
	docker exec -i dwh-crm-erp_container psql -U ${POSTGRES_USER} -d ${POSTGRES_DB} -c "CALL silver.load_silver();"
	@sleep 10
	@echo -n "Silver layer tables loading complete"

setup_gold_layer:
	@echo -n "Creating gold layer views..."
	docker exec -i dwh-crm-erp_container psql -U ${POSTGRES_USER} -d ${POSTGRES_DB} < ./etl_scripts/gold/ddl_gold.sql	
	@sleep 5

test_silver_layer:
	@echo -n "Loading the silver data quality check stored procedure"
	docker exec -i dwh-crm-erp_container psql -U ${POSTGRES_USER} -d ${POSTGRES_DB} < ./tests/proc_silver_quality_checks.sql
	@echo -n "Populating tables..."
	docker exec -i dwh-crm-erp_container psql -U ${POSTGRES_USER} -d ${POSTGRES_DB} -c "CALL silver.run_quality_checks();"
	@sleep 2

test_gold_layer:
	@echo -n "Loading the gold data quality check stored procedure"
	docker exec -i dwh-crm-erp_container psql -U ${POSTGRES_USER} -d ${POSTGRES_DB} < ./tests/proc_gold_quality_checks.sql
	@echo -n "Populating tables..."
	docker exec -i dwh-crm-erp_container psql -U ${POSTGRES_USER} -d ${POSTGRES_DB} -c "CALL gold.run_quality_checks();"
	@sleep 2