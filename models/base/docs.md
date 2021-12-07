### segment_web_page_views

{% docs segment_web_page_views %}

This is a base model for Segment's web page views table. It does some straightforward renaming and parsing of Segment raw data in this table.
If there are multiple entries in the source table for one page view id deduplication is done to keep the row with the earliest `received_at` timestamp.

{% enddocs %}
