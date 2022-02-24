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

test-bigquery:
	dbt deps
	dbt seed --target bigquery
	dbt run --target bigquery --full-refresh
	dbt run --target bigquery
	dbt test --target bigquery

test-postgres:
	dbt deps
	dbt seed --target postgres
	dbt run --target postgres --full-refresh
	dbt run --target postgres
	dbt test --target postgres

test-all: test-redshift test-snowflake test-bigquery test-postgres
	echo "Completed successfully"
