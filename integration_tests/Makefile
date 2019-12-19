test-redshift:
	dbt deps
	dbt seed --target redshift
	dbt run --target redshift --full-refresh
	dbt run --target redshift
	dbt test --target redshift

test-snowflake:
	dbt deps
	dbt seed --target snowflake
	dbt run --target snowflake --full-refresh
	dbt run --target snowflake
	dbt test --target snowflake

test-all: test-redshift test-snowflake
	echo "Completed successfully"
