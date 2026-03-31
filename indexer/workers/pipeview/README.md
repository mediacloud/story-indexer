This worker processes messages containing "breadcrumbs", collects
counts in a Postgres database, and provides an API to be able to track
where stories end up, especially if they're being dropped or ending up
in an unexpected domain!!

This processing is COMPLETELY optional/extra and does not interfere
with the processing of stories!

Currently only enabled for "regular" (rss) pipelines.
