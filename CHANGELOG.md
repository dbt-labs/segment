# babylist/dbt_segment v0.2.0

## Improvements

- Remove expensive deduplication of source page views, which is already handled upstream in our dbt models
- Add uniquifying element to page view sessionization to account for subsetting of source page view model

# babylist/dbt_segment v0.1.0

This release restarts versioning at v0.8.1 of the upstream repo, which is what we're using as of April 2024.

# segment v0.8.1

## Fixes

- Fix duplication of sources in incremental materializations ([#80](https://github.com/dbt-labs/segment/issues/80), [#81](https://github.com/dbt-labs/segment/pull/81))

Contributors:

- [rjh336](https://github.com/rjh336) (#81)

# segment v0.8.0

## New Features

- Postgres Support ([#69](https://github.com/dbt-labs/segment/issues/69), [#70](https://github.com/dbt-labs/segment/pull/70))

## Improvements

- Significantly improved BigQuery performance ([#72](https://github.com/dbt-labs/segment/issues/72), [#73](https://github.com/dbt-labs/segment/pull/73))
- Deduplication of source page views ([#76](https://github.com/dbt-labs/segment/pull/76))

Contributors:

- [shippy](https://github.com/shippy) (#70)
- [rjh336](https://github.com/rjh336) (#73)
- [MarkMacArdle](https://github.com/MarkMacArdle) (#76)

# segment v0.7.0

This release supports any version (minor and patch) of v1, which means far less need for compatibility releases in the future.

## Under the hood

- Change `require-dbt-version` to `[">=1.0.0", "<2.0.0"]`
- Bump dbt-utils dependency
- Replace `source-paths` and `data-paths` with `model-paths` and `seed-paths` respectively
- Rename `data` and `analysis` directories to `seeds` and `analyses` respectively
- Replace `dbt_modules` with `dbt_packages` in `clean-targets`

# segment v0.6.1

🚨 This is a compatibility release in preparation for `dbt-core` v1.0.0 (🎉). Projects using this version with `dbt-core` v1.0.x can expect to see a deprecation warning. This will be resolved in the next minor release.
