test-redshift:
	dbt deps
	dbt seed --target redshift
	dbt run --target redshift --full-refresh
	dbt run --target redshift
	dbt test --target redshift