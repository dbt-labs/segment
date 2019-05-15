### dbt models for Segment

Requires [dbt](https://www.getdbt.com/) >= 0.12.2

### models ###

#### segment_web_page_views

This is a base model for Segment's web page views table. It does some straightforward renaming and parsing of Segment raw data in this table.

#### segment_web_user_stitching

This model performs "user stitching" on top of web event data. User stitching is the process of tying all events associated with a cookie to the same user_id, and solves a common problem in event analytics that users are only identified part way through their activity stream. This model returns a single user_id for every anonymous_id, and is later joined in to build a `blended_user_id` field, that acts as the primary user identifier for all sessions.

#### segment_web_page_views__sessionized

The purpose of this model is to assign a `session_id` to page views. The business logic of how this is done is that any period of inactivity of 30 minutes or more resets the session, and any subsequent page views are assigned a new `session_id`.

#### segment_web_sessions__initial

This model performs the aggregation of page views into sessions. The `session_id` having already been calculated in `segment_web_page_views__sessionized`, this model simply calls a bunch of window functions to grab the first or last value of a given field and store it at the session level.

#### segment_web_sessions__stitched

This model joins initial session data with user stitching to get the field `blended_user_id`, the id for a user across all devices that they can be identified on. This logic is broken out from other models because, while incremental, it will frequently need to be rebuilt from scratch: this is because the user stitching process can change the `blended_user_id` values for historical sessions.

It is recommended to typically run this model in its default configuration (incrementally) but on some regular basis to do a `dbt run --full-refresh --models segment_web_sessions__stitched+` so that this model and downstream models get rebuilt.

#### segment_web_sessions

The purpose of this model is to expose a single web session, derived from Segment web events. Sessions are the most common way that analysis of web visitor behavior is conducted, and although Segment doesn't natively output session data, this model uses standard logic to create sessions out of page view events.

A session is meant to represent a single instance of web activity where a user is actively browsing a website. In this case, we are demarcating sessions by 30 minute windows of inactivity: if there is 30 minutes of inactivity between two page views, the second page view begins a new session. Additionally, page views across different devices will always be tied to different sessions.

The logic implemented in this particular model is responsible for incrementally calculating a user's session number; the core sessionization logic is done in upstream models.


### installation ###

To install the latest version of this package, see the instructions at [dbt hub](https://hub.getdbt.com/fishtown-analytics/segment/latest/).

Alternate installation instructions can be found in the [dbt documentation](https://docs.getdbt.com/docs/package-management)

Then run `dbt deps`

### configuration ###

The variables needed to configure this package are as follows:

| variable | information | default value | required |
|----------|-------------|---------------|:--------:|
|`segment_page_views_table`|Location of the raw data from Segment. Segment recommends querying the `page_views` view rather than the Segment tables directly: We use views in our de-duplication process to ensure you are querying unique events and the latest objects from third-party data. All our views are set up to show information from the last 60 days. Whenever possible, we recommend that you query from these views.|None|Yes|
|`segment_sessionization_trailing_window`|Number of trailing hours to re-sessionize for. Events can come in late and we want to still be able to incorporate them into the definition of a session without needing a full refresh.|`3`|No|
|`segment_inactivity_cutoff`|Sessionization inactivity cutoff: of there is a gap in page view times that exceeds this number of seconds, the subsequent page view will start a new session.|`30 * 60`|No|
|`segment_pass_through_columns`|If there are extra columns you wish to pass through this package, define them here. Columns will be included in the `segment_web_sessions` model as `first_<column>` and `last_<column>`. Extremely useful when using this package on top of unioned Segment sources, as you can then pass through a column indicating which source the data is from.|`[]`|No|

#### a note about `segment_page_views_table`

If you're doing additional "pre-processing" on your Segment data first, like unioning or doing additional filtering/de-duplication, you may wish to point `segment_page_views_table` to something other than the Segment raw source table. If so, you'll likely want this config value pointed at something in your environment schema (i.e. the dev or testing version of the model, in addition to the production version as your models are modified and promoted). In this case, you can set this value dynamically, i.e.:

```
segment_page_views_table: "{{ ref('myschema', 'segment_unioned_pages') }}"
```

#### example configuration

An example `dbt_project.yml` configuration is provided below:

```yml
# dbt_project.yml

...

models:
    segment:
        vars:
          segment_page_views_table: "`projectname`.`segment_dataset`.`pages_view`"
          segment_sessionization_trailing_window: 3
          segment_inactivity_cutoff: 30 * 60
          segment_pass_through_columns: []

```

### database support

These models were written for Redshift and Bigquery.

### contribution ###

Additional contributions to this repo are very welcome! Please submit PRs to master. All PRs should only include functionality that is contained within all Segment deployments; no implementation-specific details should be included.
