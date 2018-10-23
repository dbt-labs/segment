### segment_web_user_stitching

{% docs segment_web_user_stitching %}

This model performs "user stitching" on top of web event data. User stitching is the process of tying all events associated with a cookie to the same user_id, and solves a common problem in event analytics that users are only identified part way through their activity stream. This model returns a single user_id for every anonymous_id, and is later joined in to build a `blended_user_id` field, that acts as the primary user identifier for all sessions.

{% enddocs %}


### segment_web_page_views__sessionized

{% docs segment_web_page_views__sessionized %}

The purpose of this model is to assign a `session_id` to page views. The business logic of how this is done is that any period of inactivity of 30 minutes or more resets the session, and any subsequent page views are assigned a new `session_id`.

The implementation of this logic is rather involved, and requires multiple CTEs. Comments have been added to the source to describe the purpose of the CTEs that are more esoteric.

{% enddocs %}


### segment_web_sessions__initial

{% docs segment_web_sessions__initial %}

This model performs the aggregation of page views into sessions. The `session_id` having already been calculated in `segment_web_page_views__sessionized`, this model simply calls a bunch of window functions to grab the first or last value of a given field and store it at the session level. 

{% enddocs %}


### segment_web_sessions__stitched

{% docs segment_web_sessions__stitched %}

This model joins initial session data with user stitching to get the field `blended_user_id`, the id for a user across all devices that they can be identified on. This logic is broken out from other models because, while incremental, it will frequently need to be rebuilt from scratch: this is because the user stitching process can change the `blended_user_id` values for historical sessions.

It is recommended to typically run this model in its default configuration (incrementally) but on some regular basis to do a `dbt run --full-refresh --models segment_web_sessions__stitched+` so that this model and downstream models get rebuilt.

{% enddocs %}


### segment_web_sessions

{% docs segment_web_sessions %}

The purpose of this model is to expose a single web session, derived from Segment web events. Sessions are the most common way that analysis of web visitor behavior is conducted, and although Segment doesn't natively output session data, this model uses standard logic to create sessions out of page view events. 

A session is meant to represent a single instance of web activity where a user is actively browsing a website. In this case, we are demarcating sessions by 30 minute windows of inactivity: if there is 30 minutes of inactivity between two page views, the second page view begins a new session. Additionally, page views across different devices will always be tied to different sessions.

The logic implemented in this particular model is responsible for incrementally calculating a user's session number; the core sessionization logic is done in upstream models.

{% enddocs %}


