# story-indexer choices

NOTE!  This started as Phil's attempt to braindump design choices.
It should not be taken as authoritative.

## Requirements

The original requirements for the project are documented at
https://docs.google.com/document/d/1cMYBqpeRXiXX8B3t_2Wq5FV1aZKEX8FSjSjoHJRR0-c/edit?usp=sharing

### Open

* Use and build open source tools, not proprietary ones
* Can run anywhere (dedicated hardware or the cloud)
* Use standard file formats (WARC file)
* BUT, use available services (like sentry) to avoid excessive infrastructure

### Observable

* Visible statistics for monitoring
* Alerts

### Scalability

* Use queues not databases or files to represent system state
* Avoid SQL databases in processing path

### Reduce spin-up and ongoing costs

### Reduce data maintenance needs

* keep data in one place, not two or more

### Split off different tasks to separate projects that could be owned by different organizations

Rahul: this was an original requirement, which hasn't worked out for social-technical reasons but definitely drove engineering architecture decisions.

### Package choices

#### Queuing

We decided on bare RabbitMQ because "streaming" frameworks like Kafka
seemed to be a solution aimed at larger scale applications than
story-indexer.  Streaming systems seem to prefer that data always
arrive chronologically, but side-loading and back-filling violate that
assumption.

#### Searching

[PB: something here on choice of ES over Solr?]

### Abstraction

"Hide mistakes behind abstractions, so they can be replaced if needed"

* App class

  + Base class for command line programs, provides command line argument parsing, logging, statistics reporting abstraction.
  + Eliminates "boilerplate"
  + Mixins provide additional sets of common options/functionality.

* Story class

  + Pipeline processes are unaware of the storage representation of stories.

* Worker/Queuer classes

  + Pipeline process are unaware of how and where stories move.

### Simplicity

When possible, choose a simple solition.
If it's likely the choice will be revisited, see "Abstraction" above.

### Data Safety

Three layers:

1. ES Cluster

   Running a three node Elasticsearch cluster with a replica of every shard.

2. Rotating indices

   Using ES Index Lifetime Management to close the currently active index
   every three months, so static/past processed data can be dumped and archived off-site.

3. Story Archive Files

   After stories are indexed (found to be new/unique), they're packed into Web Archive
   (WARC) files, and archived off site.  With the above index archives, at most
   the last three months of WARC files should be needed to restore all data.

### Modularity

Pipeline "Worker" processes are unaware of their position in a
pipeline: They process input messages from a `workername-in` queue,
and write to a `workername-out` exchange, and can be assembled in
multiple configurations.

`pipeline.py` "plumbs" the pipeline based on the `--type` argument.

The basic structure for the three current flavors of pipeline is:

  queuer[1] -> fetcher -> parser -> importer -> archiver[2]

As shown in (system-diagram.pdf).

NOTES:
1. With batch based fetching there is no queuer
2. For archive processing pipelines, the archiver is not run (since archives already exist!)

`pipeline.py` can create "fanouts" where a Worker's output is fed to
more than one Worker's input queue.  This could be used for any number
of reasons: indexing in multiple search engines, temporary or
permanent statistics gathering, piping stories to specific research
projects for processing according to their needs.

Alternate pipelines could be used, for example:

  hist-queuer -> hist-fetcher -> archiver

could be used to re-pack historical S3 objects into WARC files in new
S3 bucket, for later transport/download and parsing.

arch-fetcher could be used to process WARC files if a research
question required additional processing.

### Write installation scripts!

Installation scripts are preferable to documentation!

Remember!  There may be emergency circumstances (with little or no
advance notice) that require the system to be set up at an alternate
location.  Hand installation/configuration should be avoided!!!

### Repeatability

See [../docker/README.md](../docker/README.md) for how to install
the system, and to deploy the code.

`deploy.sh` exists to automate the process of testing and deploying
code WITHOUT hand tweaking.  Hand tweaking makes it hard/impossible
for another person to replicate your results!

### Document your decisions

Whenever possible, put in comments that describe why a choice was
made, or a particular number was used!  The code will likely outlast
your involvement with the project, or you may not remember the details
two years, or even six months from now!

If you DO put a file in the doc directory, try to remember to update
it when something changes!!!

### Monitoring

Mediacloud was already using the dokku grafana-graphite-statsd docker
image for the rss-fetcher, which while dated and hard to configure, is
turn-key.  The image uses statsd to aggregate data sent from multiple
containers to a well-known DNS name.  Stats are reported via App class
methods and are intentionally abstract if replacement in warrented.

story-indexer uses the schema established by rss-fetcher (and the use
of statsd as the collector):

  `stats.TYPE.mc.REALM.PROJECT.PROGRAM.NAME[.LABEL=VALUE....]`

Where
* TYPE is `counters`, `gauges` or `timers` (forced by statsd)
* REALM contains the deployment type (and optional pipeline) type, ie: prod, staging, hist-prod, devname
  in grafana the variable `$realm` can be used in dashboards to allow multiple stacks to be monitored.
* PROJECT: `story-indexer`
* PROGRAM is the name passed to the App (sub)class
* NAME is the statistic name (can be dotted)
* `LABEL=VALUE` is used when a group of sub-stats sum meaningfully and want to be displayed together

#### Data types

The following stats datatypes are available:

* counters
  + sum events across multiple container instances for each reporting period
  + reported/stored incrementally (sums over a period can be queried)
  + almost always monotonically increasing (like an odometer)
  + statsd reports raw `count` for period, or `rate` scaled to events/second
  + ideally should be a plural noun for the event, ie; "stories".  Sub-conditions should be reported with a `status` label.
* gauges
  + used for things that go up and down (like a thermometer): containers, queue levels
  + only a single (last) report for current reporting period saved
* timers
  + aggregate statistics for multiple reports per period
  + for time data, canonically reported in floating point milliseconds
  + can be used for any multi-value data
  + statsd reports a LARGE number of aggregates (upper, lower, mean) on all data or on particular percentiles

#### Stock stats

All Apps report a timer for "main_loop".  This enables monitoring
of processing time, and frequent restarts.

All queue-based apps report a `sent-msgs` counter with label name
`dest` and labels `appname-{delay,out,quar}`.

Everything that handles Story objects has a `stories` counter,
that can be incremented by calling `StoryMixin.incr_stories(status, story_url)`

`incr_stories` should be called *EXACTLY ONCE* per story.

* multiple calls will cause overcounts
* provides tracing of a story through the log files!!!

Error conditions are as/more important as success, and can help
localize problems when something goes wrong!!!

If there is need to report some additional orthogonal condition,
you may need to add another counter (or replace "success" with
several different conditions).

`StoryApp` also provides methods to enforce important conditions which
may occur at different points in pipeline processing (depending on
Story origin):

* `check_story_length`
* `check_story_url`

Both call `incr_stories` and return `False` when a story should
be discarded.

#### Reporters

Some programs run in every stack that report server/stack stats:

* `elastic-stats` reports data from `http:server:9200/_stats/`
* `rabbitmq-stats` reports:
  + queue stats: acked, ready, delivered, memory usage, published, redelivered, unacked
  + node stats: fds, memory usage, open sockets
* `docker-stats` reports: services running, desired, completed

All of the above report data only once a minute, to avoid excessive overhead.

#### timer aggregate stats

The following are reported for each timer:

* count
* count_{50,75,90,95,98,99,99_9,99_99,99_999,ps}
* lower
* mean
* mean_{50,75,90,95,98,99,99_9,99_99,99_999}
* median
* std
* sum
* sum_{50,75,90,95,98,99,99_9,99_99,99_999}
* sum_squares
* sum_squares_{50,75,90,95,98,99,99_9,99_99,99_999}
* upper
* upper_{50,75,90,95,98,99,99_9,99_99,99_999}

#### too many stats

It's easy to report more stats than will ever be examined (although
this project has not fallen into that hole, yet).

Each series (One week of data at 10-second granularity) takes 709K of
storage.  Since two series are reported for each counter, they take 1410K,
and since 54(!!) series are reported for each timer, they take 38M.

#### Where the bodies are buried

Stats data ends up on tarbell in
`/space/dokku/services/grafana-graphite-statsd/ObscureStatsServiceName/data/whisper`
(the dokku service has name ObscureStatsServiceName so that the name
"stats" can be associated with a proxy that speaks https).

Each series ends up in a `.wsp` file, and can be removed, HOWEVER, if
the series has been reported since the grafana container started, it
will be re-reported by statsd.  So to eliminate a stat you first need
to restart statsd on tarbell:

  `docker exec dokku.graphite.ObscureStatsServiceName supervisorctl restart statsd`

Then you can remove the old files without having them reappear, this
will however cause a discontinuity in some graphs (that will be gone
in a week).
