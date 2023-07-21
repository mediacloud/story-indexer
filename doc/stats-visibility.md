We are using statsd+prometheus to log statistics to grafana for ongoing visibility.

These stats are stored within a directory hierarchy:

`mc/REALM/story-indexer/PROCESSNAME/STATNAME`

where REALM indicates the deployment environment (ie: prod, staging, test, developer-name)

and PROCESSNAME is the process_name argument that's passed to an App at runtime: (fetcher, parser, indexer)

the STATNAME is specified in code at the point that the stat is invoked, and should name the thing which is being counted.
Ideally, STATNAME is a plural name, as well.
