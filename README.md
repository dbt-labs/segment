> This package is not officially related to Segment and is maintained by [Fleetio](https://fleetio.com). CI jobs run on PRs will only test for postgres compatibility. We're working on setting up a service account to support tests for Snowflake. Please submit an issue for any bugs/feature requests related to Redshift or BigQuery and we'll figure out how to help!

# dbt-segment
This [dbt package](https://docs.getdbt.com/docs/package-management):
* Performs "user stitching" to tie all events associated with a cookie to the same user_id
* Transforms pageviews into sessions ("sessionization")


## Installation instructions
New to dbt packages? Read more about them [here](https://docs.getdbt.com/docs/building-a-dbt-project/package-management/).
1. Include this package in your `packages.yml` — check [here](https://hub.getdbt.com/dbt-labs/segment/latest/) for the latest version number.
2. Run `dbt deps`
3. Include the following in your `dbt_project.yml` directly within your `vars:` block (making sure to handle indenting appropriately). **Update the value to point to your segment page views table**.

```YAML
# dbt_project.yml
config-version: 2
...

vars:
  dbt_segment:
    segment_page_views_table: "{{ source('segment', 'pages') }}"

```
This package assumes that your data is in a structure similar to the test
file included in [example_segment_pages](integration_tests/seeds/example_segment_pages.csv).
You may have to do some pre-processing in an upstream model to get it into this shape.
Similarly, if you need to union multiple sources, de-duplicate records, or filter
out bad records, do this in an upstream model.

4. Optionally configure extra parameters by adding them to your own `dbt_project.yml` file – see [dbt_project.yml](dbt_project.yml)
for more details:

```YAML
# dbt_project.yml
config-version: 2

...

vars:
  dbt_segment:
    segment_page_views_table: "{{ source('segment', 'pages') }}"
    segment_sessionization_trailing_window: 3
    segment_inactivity_cutoff: 30 * 60
    segment_pass_through_columns: []
    segment_bigquery_partition_granularity: 'day' # BigQuery only: partition granularity for `partition_by` config

```
5. Execute `dbt seed` -- this project includes a CSV that must be seeded for it
the package to run successfully.
6. Execute `dbt run` – the Segment models will get built as part of your run!

## Database support
This package has been tested on Redshift, Snowflake, BigQuery, and Postgres.

### Contributing
Additional contributions to this repo are very welcome! Check out [this post](https://discourse.getdbt.com/t/contributing-to-a-dbt-package/657) on the best workflow for contributing to a package. All PRs should only include functionality that is contained within all Segment deployments; no implementation-specific details should be included. CI jobs run on PRs will only test for postgres compatibility. 
