# dbt-segment
This [dbt package](https://docs.getdbt.com/docs/package-management):
* Performs "user stitching" to tie all events associated with a cookie to the same user_id
* Transforms pageviews into sessions ("sessionization")


## Installation instructions

1. Include this package in your `packages.yml` -- check [here](https://hub.getdbt.com/fishtown-analytics/segment/latest/)
for installation instructions.
2. Run `dbt deps`
3. Include the following in your `dbt_project.yml` directly within your
`models:` block (making sure to handle indenting appropriately). **Update the value to point to the schema that contains segment page views table**.

```YAML
# dbt_project.yml
...

models:
  segment:
    vars:
      segment_schema: "segment"

```
If you are using Snowflake or BigQuery, you may also have to define a different database (== project on BQ):
```yml
models:
  segment:
    vars:
      segment_schema: "segment"
      segment_database: "RAW"
```


This package assumes that your data is in a structure similar to the test
file included in [example_segment_pages](integration_tests/data/example_segment_pages.csv).
You may have to do some pre-processing in an upstream model to get it into this shape.
Similarly, if you need to union multiple sources, de-duplicate records, or filter
out bad records, do this in an upstream model.

Then, pass the package the pre-processed model instead, like so (do **not** set the `segment_schema` parameter)
```yml
models:
  segment:
    vars:
      segment_page_views_relation: {{ ref("unioned_segment_page_views") }}
```


4. Optionally configure extra parameters by adding them to your own `dbt_project.yml` file – see [dbt_project.yml](dbt_project.yml)
for more details:
```yaml
# dbt_project.yml
...

models:
  segment:
    vars:
      ...
      segment_sessionization_trailing_window: 3
      segment_inactivity_cutoff: 30 * 60
      segment_pass_through_columns: []

```
5. Execute `dbt seed` -- this project includes a CSV that must be seeded for it
the package to run successfully.
6. Execute `dbt run` – the Segment models will get built as part of your run!

## Database support
This package has been tests on Redshift, Snowflake, and BigQuery.

## Contributing

Additional contributions to this repo are very welcome! Check out [this post](https://discourse.getdbt.com/t/contributing-to-a-dbt-package/657) on the best workflow for contributing to a package. All PRs should only include functionality that is contained within all Segment deployments; no implementation-specific details should be included.
