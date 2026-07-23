"""
mc-deploy script for story-indexer
started 7/11/2026
from deploy.sh started 9/2023
from rss-fetcher/dokku-scripts/push.sh 9/2022!

The creation of the mc-deploy module and the translation from shell
scripts to Python was motivated by the increased number of different
deploy scripts (using both Dokku and docker compose), their
deviance/deviations, and the maintenance nightmare that imposed.

This is, as much as possible, a translation of the functionality of
the script into the mc-deploy framework rather than taking the
opportunity to completely restructure things.  As the first use of
DockerDeploy (and probably the most complex), some fiddling with the
framework was necessary.  All of this goes to say this isn't the
cleanest, simplest thing, but rather an expedient to avoid the worser
fate of many similar-ish scripts.

In the original shell script all configuration was collected as
environment variables which were selectively put in a JSON file which
was read by the jinja2 cli to produce docker-compose.yml for different
flavors of pipeline and deployment (dev/staging/prod).

The (unchanged) philosophy of the docker-compose.yml.j2 template is to
do as much processing OUTSIDE the template as possible, trying to do
only simple substitution, if's and in the case of dev/staging, a loop
to create ES containers.

All settings are collected using mc-deploy settings_xxx calls into a
dict (self.settings) of STRING VALUES, echoing the original "sourcing"
config files into the shell environment.

  NOTE!!! **MANY** things are in "settings" and passed into the jinja
  template, to avoid having the same/related value appear more than
  once in the template, or just because they were shell variables in
  deploy.sh, NOT because they are changed by ANY configuration file.
  Pain has only been taken to make sure things that matter (need to
  work to deploy production) work as expected.

From those settings, values are EXPLICTLY transferred into lower case
named variables in the jinja_vars dict.

  NOTE! The ordeal below *MIGHT* be made simpler by having a global list:

  `SETTINGS = [Var("NAME", "default", Check.XXX, ....), ...]`

  Than can be iterated over, rather than having code in multiple places
  for each variable.  Possible Var parameters:

  check=Check.NONE (do not pass to Jinja2)
  check=Check.TMPL which generates a value (if nothing set)
        by doing `default_value.format(options)`
  cond="VARNAME" to make the output conditional
        on the Truthiness of the referenced variable.

  But the proof is in the pudding, and there are no doubt
  additional cases that would need to be handled, and debugged!

If you want to pass a new setting from configuration files to
or command line options to docker-compose.yml.j2:

FIRST, come up with a variable name.  It should be SPECIFIC to the
part of the indexer pipeline, its use and the technology is applies
to, for example PIPEVIEW_POSTGRES_IMAGE, ARCHIVER_B2_BUCKET

Specificity helps clarify the use (and avoid confusion), AND allows
room for the particulars to change at a later date and to be able to
select/configure the old and new ways in development, testing, and if
need be, to fall back in production.  Having a single generally named
variable whose contents needs to change when new code is running in
such a way that falling back to old code ALSO requires falling back to
old configuration makes operations work harder.

SECOND; If there is a rational default value that applies to both dev
and staging/production you can set it in `deploy_default_settings()`.

If the configuration needs to be kept private, the value needs to go
into a config file that is not publicly legible.

  If you're adding a service with a network port:

    please have a configuration variable (eg PIPEVIEW_API_PORT, or
    ELASTICSEARCH_PORT_BASE if the number will be used to calculate
    other ports) for use inside the container stack.

    If you want to make the service visible outside docker, have a
    setting like PIPEVIEW_API_PORT_EXPORTED generated using a base
    value that _reminds_ you of the native port used by the service
    (ie; add 40K, multiply by 10) added to the "port_bias" for the
    stack instance.

  Similarly, with any setting constructed from other settings (ie; to
  make a URL) remember to do heavy lifting, and creating common values
  HERE and NOT in the .j2 file) and configuration needs to be able to
  override a component of the constructed value, the construction
  can be done at the bottom of 'settings_get_new()`

THIRD; you need to transfer the value to jinja_vars using the
`set` function in `deploy_default_settings`.  While it may seem
just annoying, this step checks the values:

  1. The settings variable has been set.
  2. The value conforms to expectations
     (most are strings that must not be empty,
     but some can be empty, especially to disable
     a feature, and some are passed as integers
     or bools)

jinja is being used in "strict" mode, so ALL referenced variables
should always be set unless the references happen to be inside in "if".

FOURTH; You can now use lower cased variable name in
the template file.

FIFTH; Before submitting a PR to merge changes to this, please do
"dry-runs" to see that things run cleanly and as expected for dev,
staging and production environments:

    venv/bin/python deploy.py -dn deploy
    venv/bin/python deploy.py deploy -dn --test staging deploy
    venv/bin/python deploy.py deploy -dn --test prod deploy

  If you test your code outside Docker, doing a test run in a dev
  stack helps ensure you avoid unpopularity when the NEXT developer,
  who does use a dev stack tries to deploy their code after your PR is
  merged, or even greater unpopularity when someone tries to move
  code into staging and something is broken.

  AND if, you've made changes that depend on the stack "flavor",
  do dry runs with OTHER flavors using --flavor top level option.

It could use a re-work/cleanup, but it's difficult to justify the time
it would take to clearly understand the issues, and ordering
constraints, and testing flavors vs deployment types!!
"""

# TODO:
# make sure all indexer args used!!

import os
import re
import sys
from enum import Enum

# PyPI:
import jinja2

# mc-deploy package (in mediacloud/system-dev-ops repo):
from mc_deploy.base import CmdArgs, CmdParser, Flavor, ParserArgs
from mc_deploy.docker import DockerDeploy

SUPER_VERBOSE = False  # for debug


class Check(Enum):
    """
    value checks for settings passed as jinja template vars
    """

    INT = "int"
    BOOL = "bool"
    STR = "str"
    ALLOW_EMPTY = "allow-empty"


class StoryIndexerDeploy(DockerDeploy):
    INST_BASE = "indexer"  # app base name

    # map command line names to inst_base prefix & port bias
    # ES uses ports 9200 & 9300, so bias values increase by 200
    INST_FLAVORS = {
        "queue-fetcher": Flavor("", 0),
        # "batch-fetcher": Flavor("", 0), # scrapy removed from requirements
        "historical": Flavor("hist-", 200),
        "archive": Flavor("arch-", 400),
        "csv": Flavor("csv-", 600),
    }
    PROJECT_REPO = "story-indexer"

    def before_inst_name(self) -> None:
        assert not getattr(self, "inst_name", None)

    def deploy_default_settings(self, args: CmdArgs) -> None:  # noqa: C901
        """
        called before deploy_cmd_helper to set defaults
        before settings files loaded
        """

        assert not self._conf_loaded
        self.settings_add("PIPELINE_TYPE", args.flavor)

        # copied more or less slavishly from deploy.sh

        # defaults for template variables that might change based on BRANCH/DEPLOY_TYPE
        # (in alphabetical order):

        multi_node_deployment = False
        fetcher_options = []
        importer_args = []

        self.settings_add("ARCHIVER_REPLICAS", "1")  # seems to scale 1:1 with importers
        # configuration for Elastic Search Containers
        self.settings_add("ELASTICSEARCH_CLUSTER", "mc_elasticsearch")
        self.settings_add("ELASTICSEARCH_CONFIG_DIR", "./conf/elasticsearch/templates")
        self.settings_add(
            "ELASTICSEARCH_IMAGE",
            "docker.elastic.co/elasticsearch/elasticsearch:8.17.4",
        )
        elasticsearch_port_base = 9200  # native port
        self.settings_add("ELASTICSEARCH_PORT_BASE", str(elasticsearch_port_base))
        self.settings_add(
            "ELASTICSEARCH_PORT_BASE_EXPORTED",
            str(elasticsearch_port_base + self.port_bias),
        )

        self.settings_add("ELASTICSEARCH_SNAPSHOT_CRONJOB_ENABLE", "false")
        self.settings_add("ELASTICSEARCH_SNAPSHOT_REPO_TYPE", "fs")

        self.settings_add("FETCHER_CRONJOB_ENABLE", "true")  # batch fetcher
        self.settings_add("FETCHER_NUM_BATCHES", "20")  # batch fetcher
        fetcher_options.append("--yesterday")  # batch fetcher

        self.settings_add("IMPORTER_REPLICAS", "1")

        self.settings_add("HIST_FETCHER_REPLICAS", "4")

        self.settings_add("PARSER_REPLICAS", "4")

        pipeview_native = 8000  # internal port
        self.settings_add("PIPEVIEW_API_PORT", str(pipeview_native))
        self.settings_add(
            "PIPEVIEW_API_PORT_EXPORTED", str(40000 + pipeview_native + self.port_bias)
        )
        self.settings_add("PIPEVIEW_DAYS", "90")
        self.settings_add("PIPEVIEW_PGPORT", "5432")  # native internal port
        self.settings_add("PIPEVIEW_POSTGRES_CONTAINER", "pipeview-db")
        self.settings_add("PIPEVIEW_POSTGRES_DB", "pipeview")
        self.settings_add("PIPEVIEW_POSTGRES_IMAGE", "postgres:18-alpine")
        self.settings_add(
            "PIPEVIEW_POSTGRES_PORT_EXPORTED", str(54320 + self.port_bias)
        )
        self.settings_add("PIPEVIEW_POSTGRES_USER", "postgres")

        self.settings_add("QUEUER_CRONJOB_ENABLE", "true")
        self.settings_add("QUEUER_CRONJOB_MINUTES", "3-59/5")
        self.settings_add("QUEUER_CRONJOB_REPLICAS", "1")
        self.settings_add("QUEUER_INITIAL_REPLICAS", "0")

        self.settings_add(
            "RABBITMQ_CONTAINERS", "1"
        )  # integer to allow cluster in staging??
        rabbitmq_port = 5672  # native port
        self.settings_add("RABBITMQ_PORT", str(rabbitmq_port))

        # must be a valid hostname (no underscores!)
        self.settings_add("SYSLOG_SINK_CONTAINER", "syslog-sink")

        # Pushing to a local registry for now
        # (since we're not deploying on multi-node
        # clusters we don't even NEED to push it?!)
        # MUST have trailing slash unless empty
        worker_image_registry = "localhost:5000/"
        if self.is_dev():
            # testing: disable registry push for development
            # We always run on just one node.
            # Images accumulate in registry volume.
            worker_image_registry = ""
        self.settings_add("WORKER_IMAGE_REGISTRY", worker_image_registry)

        # PLB: maybe indexer-common, now that it's used for config & stats reporting?
        self.settings_add("WORKER_IMAGE_NAME", "indexer-worker")

        # if adding anything here also add to indexer.pipeline.MyPipeline.pipe_layer method,
        # and to PIPELINE_TYPES (above the usage function)!

        arch_suffix = ""
        if self.inst_flavor == "historical":
            hist_year = args.hist_year
            if not hist_year:
                self.fatal("need --hist-year")
            self.settings_add("HIST_YEAR", hist_year)

            hist_file_prefix = args.hist_file_prefix
            if hist_file_prefix:
                if hist_file_prefix[0] != "/":
                    self.fatal("--file-file-prefix must start with '/'")
                self.settings_add("HIST_FILE_PREFIX", hist_file_prefix)

            # unless archives disabled, prefix will end with:
            arch_suffix = f"hist{hist_year}"
            # uncomment to disable archives:
            # importer_args.append("--no-output")
            if self.is_prod():
                # In Feb 2024, on bernstein (Xeon Gold 6134@3.2GHz, 32 cores, 6400 bogomips):
                # 12 fetchers, 18 parsers, 2 importers: ~100 stories/second w/ load avg 24
                # 14 fetchers, 21 parsers, 2 importers: ~125 stories/second w/ load avg 27
                # 12 fetchers, 21 parsers, 4 importers: ~100 stories/second w/ load avg 22
                # (mean fetch: ~109ms, parse: ~150ms, import: ~11ms)
                # Aug 2024, fetching ~120 stories/sec (12 fetchers, mean 100ms), incr. to 7 importers.
                # Nov 2024: calculated that 100 stories/sec eats 30Mbit/s of bandwidth
                #    dropped to 6 fetchers, to run at 50 stories/sec
                # July 2025, on ramos (Xeon Gold 6246R@3.40GHz, 64 cores, 6800 bogomips)
                # while also running production:
                # 6 fetchers, 21 parsers, 7 importers: ~50 stories/second w/ load avg 8?
                # 14 fetchers, 25 parsers, 7 importers: ~130 stories/second w/ load avg <= 25
                #
                self.settings_add("HIST_FETCHER_REPLICAS", "14")
                self.settings_add("PARSER_REPLICAS", "25")
                self.settings_add("IMPORTER_REPLICAS", "7")
                self.settings_add("ARCHIVER_REPLICAS", "2")
            else:
                # Aug 2024 fetching ~40 stories/sec w/ HIST_FETCHER_REPLICAS=4
                # increased to 5 parsers(4 parsers wouldn't keep up even if
                # HIST_FETCHER_REPLICAS lowered to 3)
                self.settings_add("PARSER_REPLICAS", "5")
            self.settings_add(
                "QUEUER_FILES",
                f"s3://mediacloud-database-files/{hist_year}{hist_file_prefix}",
            )
            self.settings_add("QUEUER_TYPE", "hist-queuer")  # name of run- script
        elif self.inst_flavor == "archive":
            self.settings_add("ARCHIVER_REPLICAS", "0")  # no archivers
            importer_args.append("--no-output")  # no archives!!!
            if self.is_prod():
                # In Feb 2024, on ramos (Xeon Gold 6246R@3.4GHz 6800 bogomips)
                # 8 importers could write output of one arch-queuer (>500 stories/sec):
                self.settings_add("IMPORTER_REPLICAS", "8")
                assert args.input_files
                self.settings_add("QUEUER_FILES", args.input_files)
            self.settings_add("PARSER_REPLICAS", "0")  # no parsing required!
            # maybe require command line option to select file(s)?
            self.settings_add("QUEUER_FILES", "/app/data/archives")
            self.settings_add("QUEUER_TYPE", "arch-queuer")  # name of run- script
        elif self.inst_flavor == "queue-fetcher":
            # Using --days 2 will fetch both days (the older day first).  After N days,
            # this can be switched to "--days N" so that if a day (or more) has been
            # missed due to downtime, any holes will be filled on restart.  The code
            # that remembers what has been queued is indexer.tracker.
            self.settings_add("QUEUER_FILES", "--days 2")  # check last two days
            self.settings_add("QUEUER_TYPE", "rss-queuer")  # name of run- script
            if args.input_files:
                arch_suffix = "rss"
        elif self.inst_flavor == "csv":
            arch_suffix = "csv"
            self.settings_add("QUEUER_TYPE", "csv-queuer")  # name of run- script
            self.settings_add(
                "QUEUER_FILES", "s3://mediacloud-database-e-files/csv_files/"
            )
        else:
            self.fatal(f"Unknown pipeline flavor {self.inst_flavor}")

        assert "QUEUER_TYPE" in self.settings

        if args.no_import:
            importer_args.append("--no-import")

        self.story_limit = 0
        if self.is_prod():
            arch_prefix = f"mc{arch_suffix}"

            self.settings_add("ELASTICSEARCH_CONTAINERS", "0")
            # ES index settings are static, prod settings should not change
            self.settings_add("ELASTICSEARCH_SHARD_COUNT", "12")
            self.settings_add("ELASTICSEARCH_SHARD_REPLICAS", "1")
            self.settings_add("ELASTICSEARCH_ILM_MAX_SHARD_SIZE", "50gb")
            self.settings_add("ELASTICSEARCH_HOSTS", "http://es.newsscribe.angwin:9209")
            self.settings_add("ELASTICSEARCH_SNAPSHOT_REPO_TYPE", "s3")

            self.settings_add("SENTRY_ENVIRONMENT", "production")  # XXX

            self.settings_add(
                "VOLUME_DEVICE_PREFIX",
                f"/srv/data/docker/{self.inst_flavor_prefix}{self.INST_BASE}/",
            )  # XXX CHECK RESULTS!!!

        elif self.is_staging():
            arch_prefix = f"staging{arch_suffix}"
            self.settings_add("ELASTICSEARCH_CONTAINERS", "3")
            self.settings_add("ELASTICSEARCH_SHARD_COUNT", "5")
            self.settings_add("ELASTICSEARCH_SHARD_REPLICAS", "1")
            self.settings_add("ELASTICSEARCH_ILM_MAX_SHARD_SIZE", "5gb")
            self.settings_add(
                "ELASTICSEARCH_SNAPSHOT_REPO_SETTINGS_LOCATION", "mc_story_indexer"
            )

            self.story_limit = 50000

            # don't run daily, fetch 10x more than dev:
            self.settings_add("FETCHER_CRONJOB_ENABLE", "false")
            fetcher_options.append(f"--sample-size={self.story_limit}")
            self.settings_add("FETCHER_NUM_BATCHES", "10")  # betch fetcher

            self.settings_add("QUEUER_CRONJOB_ENABLE", "false")
            self.settings_add("QUEUER_CRONJOB_REPLICAS", "0")
            self.settings_add("QUEUER_INITIAL_REPLICAS", "1")
            self.settings_add(
                "VOLUME_DEVICE_PREFIX",
                f"/srv/data/docker/staging-{self.inst_flavor_prefix}{self.INST_BASE}/",
            )
        else:
            arch_prefix = f"{self.login_user}{arch_suffix}"
            self.settings_add("ELASTICSEARCH_CONTAINERS", "1")
            self.settings_add("ELASTICSEARCH_SHARD_COUNT", "2")
            self.settings_add("ELASTICSEARCH_SHARD_REPLICAS", "1")
            self.settings_add("ELASTICSEARCH_ILM_MAX_SHARD_SIZE", "100mb")
            self.settings_add(
                "ELASTICSEARCH_SNAPSHOT_REPO_SETTINGS_LOCATION", "mc_story_indexer"
            )

            self.story_limit = 5000

            # batch fetcher:
            # fetch limited articles under development, don't run daily:
            self.settings_add("FETCHER_CRONJOB_ENABLE", "false")
            fetcher_options.append(f"--sample-size={self.story_limit}")
            self.settings_add("FETCHER_NUM_BATCHES", "10")

            self.settings_add("QUEUER_CRONJOB_ENABLE", "false")
            self.settings_add("QUEUER_CRONJOB_REPLICAS", "0")
            self.settings_add("QUEUER_INITIAL_REPLICAS", "1")
            self.settings_add("VOLUME_DEVICE_PREFIX", "")

        self.settings_add("FETCHER_OPTIONS", " ".join(fetcher_options))
        self.settings_add("IMPORTER_ARGS", " ".join(importer_args))
        self.settings_add("ARCHIVER_PREFIX", arch_prefix)

        if self.inst_flavor in ("queue-fetcher", "batch-fetcher"):
            breadcrumb_exchange = "breadcrumb"
        else:
            breadcrumb_exchange = ""
        self.settings_add("BREADCRUMB_EXCHANGE", breadcrumb_exchange)

        # NOTE! in-network containers see native (unmapped) ports,
        # so set environment variable values BEFORE applying PORT_BIAS!!
        if self.settings["RABBITMQ_CONTAINERS"] == "0":
            # if production switches to using an external (non-docker)
            # RabbitMQ cluster, set RABBITMQ_VHOST (to "/${PIPELINE_TYPE}" for
            # anything but batch-fetcher) to give each pipe-type it's own
            # queue namespace (would want to clear PIPE_TYPE_BIAS for
            # rabbitmq_port!), and have index.pipeline "configure" make sure
            # vhost exists.
            self.fatal("RABBITMQ_CONTAINERS is zero: need rabbitmq_(v)host!!!")
        else:
            rabbitmq_host = "rabbitmq"  # container name
            rabbitmq_vhost = ""

        self.settings_add(
            "RABBITMQ_URL",
            f"amqp://{rabbitmq_host}:{rabbitmq_port}{rabbitmq_vhost}/?connection_attempts=10&retry_delay=5",
        )

        es_containers = int(self.settings["ELASTICSEARCH_CONTAINERS"] or "0")
        if es_containers > 0:
            es_hosts = []
            es_nodes = []
            for i in range(1, es_containers + 1):
                node = f"elasticsearch{i}"
                es_hosts.append(f"http://{node}:{elasticsearch_port_base}")
                es_nodes.append(node)
            self.settings_add("ELASTICSEARCH_HOSTS", ",".join(es_hosts))
            self.settings_add("ELASTICSEARCH_NODES", ",".join(es_nodes))

            assert not multi_node_deployment

            # default to placement on manager for (single node) developement
            self.settings_add(
                "ELASTICSEARCH_PLACEMENT_CONSTRAINT", "node.role == manager"
            )

        assert not multi_node_deployment
        self.settings_add("WORKER_PLACEMENT_CONSTRAINT", "node.role == manager")

        # these appear at top of generated docker-compose.yml
        self.settings_add("DEPLOYMENT_BRANCH", self.branch)
        self.settings_add("DEPLOYMENT_DATE_TIME", self.date_time)
        self.settings_add("DEPLOYMENT_GIT_HASH", self.git_revision_hash())
        self.settings_add("DEPLOYMENT_HOST", self.hostname)
        self.settings_add("DEPLOYMENT_USER", self.login_user)
        # this memorializes the command line settings used to start
        # the stack in the docker-compose.yaml.TAG file:
        self.settings_add("DEPLOYMENT_OPTIONS", " ".join(sys.argv))

        self.settings_add("RABBITMQ_PORT_EXPORTED", str(rabbitmq_port + self.port_bias))

    def docker_compose_file_create(self) -> None:
        self.jinja_vars: dict[str, str | int] = {}

        # identification unique to this deployment
        # used as an exchange name to signal pipeline config complete
        # MUST start with mc-configuration- and be less than 256 bytes:
        self.settings_add(
            "DEPLOYMENT_ID", f"mc-configuration-{self.date_time}-{self.inst_name}"
        )

        # registry (if any, must end with slash) + image name
        image_reg = self.settings["WORKER_IMAGE_REGISTRY"]
        if image_reg and not image_reg.endswith("/"):
            self.fatal(f"WORKER_IMAGE_REGISTRY must end with /: {image_reg}")
        image_name = self.settings["WORKER_IMAGE_NAME"]

        # use git tag for image tag.
        # in development this means old tagged images will pile up until removed
        # (used to allow "dirty" builds where tag was always "dirty").
        image_tag = re.sub(r"[^a-zA-Z0-9_.-]", "_", self.tag)

        self.settings_add("WORKER_IMAGE_FULL", f"{image_reg}{image_name}:{image_tag}")

        # allow multiple deploys on same swarm/cluster:
        self.settings_add("NETWORK_NAME", self.inst_name)
        self.settings_add("STACK_NAME", self.inst_name)

        # things that vary by stack type, from most to least interesting
        self.debug("stack_name", self.inst_name)
        self.debug("statsd_realm", self.settings["STATSD_REALM"])
        self.debug("archiver_prefix", self.settings["ARCHIVER_PREFIX"])
        self.debug(
            "pipeview_api_port_exported", self.settings["PIPEVIEW_API_PORT_EXPORTED"]
        )

        def add(var: str, check: Check = Check.ALLOW_EMPTY) -> None:
            if var not in self.settings:
                self.fatal(f"{var} not set")
            val = self.settings.get(var, "NOVALUE")  # dry-run
            jname = var.lower()
            if check == Check.BOOL:
                if val in ("true", "false"):
                    self.jinja_vars[jname] = val == "true"
                else:
                    self.fatal(f"{var} bad bool: '{val}'")
            elif check == Check.INT:
                if val is not None and val.isdigit():
                    self.jinja_vars[jname] = int(val)
                else:
                    self.fatal(f"{var} bad int: '{val}'")
            elif check == Check.STR:
                if val:
                    self.jinja_vars[jname] = val
                else:
                    self.fatal(f"{var} is empty")
            elif check == Check.ALLOW_EMPTY:
                self.jinja_vars[jname] = val or ""
            else:
                self.fatal(f"{var} bad check {check}")

        # NOTE! COULD pass deploy_type, but would rather
        # pass multiple variables that effect specific outcomes
        # (keep decision making in this file, and not template;
        #  don't ifdef C code based on platform name, but on features)

        # keep in alphabetical order to avoid duplicates

        # When adding a new variable, you almost certainly need to add an
        # environment: "FOO: {{foo}}" line in docker-compose.yaml.j2!

        # "private" means almost certainly came from a private/production configuration file

        add("ARCHIVER_B2_BUCKET")  # private
        add("ARCHIVER_B2_REGION", Check.ALLOW_EMPTY)  # private: empty to disable
        add("ARCHIVER_B2_SECRET_ACCESS_KEY")  # private
        add("ARCHIVER_B2_ACCESS_KEY_ID")  # private
        add("ARCHIVER_PREFIX")
        add("ARCHIVER_REPLICAS", Check.INT)
        add("ARCHIVER_S3_BUCKET")  # private
        add("ARCHIVER_S3_REGION", Check.ALLOW_EMPTY)  # private: empty to disable
        add("ARCHIVER_S3_SECRET_ACCESS_KEY")  # private
        add("ARCHIVER_S3_ACCESS_KEY_ID")  # private
        add("BREADCRUMB_EXCHANGE", Check.ALLOW_EMPTY)  # empty to disable
        add("DEPLOYMENT_BRANCH")  # for context
        add("DEPLOYMENT_DATE_TIME")  # for context
        add("DEPLOYMENT_GIT_HASH")  # for context
        add("DEPLOYMENT_HOST")  # for context
        add("DEPLOYMENT_ID")  # for RabbitMQ sentinal
        add("DEPLOYMENT_OPTIONS", Check.ALLOW_EMPTY)  # for context
        add("DEPLOYMENT_USER")  # for context
        add("ELASTICSEARCH_CLUSTER")
        add("ELASTICSEARCH_CONFIG_DIR")
        add("ELASTICSEARCH_CONTAINERS", Check.INT)
        add("ELASTICSEARCH_HOSTS")
        add("ELASTICSEARCH_SNAPSHOT_CRONJOB_ENABLE")  # NOT bool!
        add("ELASTICSEARCH_SNAPSHOT_REPO")
        add("ELASTICSEARCH_SNAPSHOT_REPO_TYPE")
        add("ELASTICSEARCH_SNAPSHOT_REPO_SETTINGS_LOCATION", Check.ALLOW_EMPTY)
        add("ELASTICSEARCH_SNAPSHOT_REPO_SETTINGS_BUCKET", Check.ALLOW_EMPTY)  # private
        add("ELASTICSEARCH_SNAPSHOT_REPO_SETTINGS_ENDPOINT", Check.ALLOW_EMPTY)
        add("ELASTICSEARCH_SHARD_COUNT", Check.INT)
        add("ELASTICSEARCH_SHARD_REPLICAS", Check.INT)
        add("ELASTICSEARCH_ILM_MAX_SHARD_SIZE")
        if int(self.settings["ELASTICSEARCH_CONTAINERS"] or "0") > 0:
            # make these conditional rather than allow-empty
            add("ELASTICSEARCH_IMAGE")
            add("ELASTICSEARCH_PLACEMENT_CONSTRAINT")
            add("ELASTICSEARCH_PORT_BASE", Check.INT)
            add("ELASTICSEARCH_PORT_BASE_EXPORTED", Check.INT)
            add("ELASTICSEARCH_NODES")

        add("FETCHER_CRONJOB_ENABLE")  # batch-fetcher: NOT bool!
        add("FETCHER_NUM_BATCHES", Check.INT)  # batch-fetcher
        add("FETCHER_OPTIONS")  # batch-fetcher (see QUEUER_ARGS)
        add("HIST_FETCHER_REPLICAS", Check.INT)
        add("IMPORTER_ARGS", Check.ALLOW_EMPTY)
        add("IMPORTER_REPLICAS", Check.INT)
        add("NETWORK_NAME")
        add("PIPELINE_TYPE")
        add("PIPEVIEW_API_PORT")
        add("PIPEVIEW_API_PORT_EXPORTED")
        add("PIPEVIEW_DATABASE_URL")
        add("PIPEVIEW_DAYS", Check.INT)
        add("PIPEVIEW_PGPORT")
        add("PIPEVIEW_POSTGRES_CONTAINER")
        add("PIPEVIEW_POSTGRES_DB")
        add("PIPEVIEW_POSTGRES_IMAGE")
        add("PIPEVIEW_POSTGRES_PASSWORD")
        add("PIPEVIEW_POSTGRES_PORT_EXPORTED")
        add("PIPEVIEW_POSTGRES_USER")
        add("QUEUER_ARGS")
        add("QUEUER_CRONJOB_ENABLE")  # NOT bool!
        add("QUEUER_CRONJOB_MINUTES")  # can be *, int or range with optional /minutes
        add("QUEUER_CRONJOB_REPLICAS", Check.INT)
        add("QUEUER_INITIAL_REPLICAS", Check.INT)
        add("QUEUER_S3_ACCESS_KEY_ID")  # private
        add("QUEUER_S3_REGION", Check.ALLOW_EMPTY)  # private
        add("QUEUER_S3_SECRET_ACCESS_KEY")  # private
        add("QUEUER_TYPE", Check.ALLOW_EMPTY)  # empty for batch-fetcher
        add("PARSER_REPLICAS", Check.INT)
        add("RABBITMQ_CONTAINERS", Check.INT)
        add("RABBITMQ_PORT", Check.INT)
        add("RABBITMQ_PORT_EXPORTED", Check.INT)
        add("RABBITMQ_URL")
        add("RSS_FETCHER_PASS", Check.ALLOW_EMPTY)  # private
        add("RSS_FETCHER_URL", Check.ALLOW_EMPTY)  # private
        add("RSS_FETCHER_USER", Check.ALLOW_EMPTY)  # private
        add("SENTRY_DSN", Check.ALLOW_EMPTY)  # private: empty to disable
        add("SENTRY_ENVIRONMENT")  # private
        add("STACK_NAME")
        add("STATSD_REALM")
        add("STATSD_URL")
        add("SYSLOG_SINK_CONTAINER")
        add("VOLUME_DEVICE_PREFIX", Check.ALLOW_EMPTY)
        add("WORKER_IMAGE_FULL")
        add("WORKER_IMAGE_NAME")
        add("WORKER_PLACEMENT_CONSTRAINT")

        if SUPER_VERBOSE:
            print("======== jinja_vars")
            for key, val in self.jinja_vars.items():
                print(key, val)

        jenv = jinja2.Environment(
            loader=jinja2.FileSystemLoader(self.deploy_dir),
            undefined=jinja2.StrictUndefined,
        )

        jtemplate = jenv.get_template(f"{self.COMPOSE_FILE}.j2")
        expanded = jtemplate.render(self.jinja_vars)
        output_file = os.path.join(self.deploy_dir, self.COMPOSE_FILE)
        if os.path.exists(output_file):
            os.unlink(output_file)  # in case owned by root
        with open(output_file, "w") as f:
            self.fix_file_owner(f)
            f.write(expanded)

        self.debug("wrote", output_file)

    def settings_get_new(self, args: ParserArgs) -> None:
        """
        load project settings; called from deploy_cmd_helper
        """
        super().settings_get_new(args)
        assert not self._conf_loaded
        self.deploy_default_settings(args)  # before loading files

        self.settings_add("STATSD_URL", self.statsd_url)
        self.settings_add("STATSD_REALM", self.inst_id)
        if self.is_prod_staging():
            r = self.PROJECT_REPO
            if self.is_prod():
                file = "prod.sh"
            else:
                file = "staging.sh"  # stand-alone (not overrides to prod)
            self.settings_load_private_files(f"{r}-config", [file])
        else:
            self.settings_load_file(os.path.join(self.deploy_dir, "dev.sh"))
            user_file = os.path.join(self.deploy_dir, f"{self.inst_id}.sh")
            if os.path.exists(user_file):
                self.settings_load_file(user_file)

        # after loading settings:
        assert self._conf_loaded
        self.before_inst_name()
        queuer_type = self.settings["QUEUER_TYPE"]
        if queuer_type == "arch-queuer":
            # borrow (r/w) key from archiver config for reading archive files
            self.settings_add(
                "QUEUER_S3_ACCESS_KEY_ID",
                self.settings["ARCHIVER_S3_ACCESS_KEY_ID"] or "",
            )
            self.settings_add(
                "QUEUER_S3_REGION", self.settings["ARCHIVER_S3_REGION"] or ""
            )
            self.settings_add(
                "QUEUER_S3_SECRET_ACCESS_KEY",
                self.settings["ARCHIVER_S3_SECRET_ACCESS_KEY"] or "",
            )
        elif queuer_type == "rss-queuer":
            # If explicit input files supplied for "normal" (RSS/puller) pipeline,
            # change stack/stats prefix from empty to "rss-" to distinguish from
            # regular/prod stack.  NOTE: rss-puller only run if input_files is
            # empty, so will always be processing rss files.
            if args.input_files:
                self.before_inst_name()
                self.inst_flavor_prefix = "rss-"
            elif self.settings["RSS_FETCHER_URL"]:
                # switch to rss-puller if rss-fetcher API URL supplied (and no -I)
                self.settings_add("QUEUER_TYPE", queuer_type := "rss-puller")
                self.settings_add("QUEUER_FILES", "")

                # polling too quickly will generate many small
                # archives (and queue-fetcher input might have most of
                # the urls from a few sources).

                # starting once an hour, can add /30 for twice an hour, etc
                # 17 is MIT Random Hall "most random number"
                self.settings_add("QUEUER_CRONTAB_MINUTES", "17")

        # construct QUEUER_ARGS for {arch,hist,rss}-queuers
        if queuer_type:
            queuer_opts = ""
            # after reading PRIVATE_CONF and checking for RSS_FETCHER_xxx params
            # command line -O overrides sampling options
            if args.queuer_opts:
                queuer_opts = args.queuer_opts
            elif self.story_limit:
                # pick random sample:
                queuer_opts = f"--force --sample-size {self.story_limit}"

            if args.input_files:
                # Command line -I overrides default file(s)
                # [MAY include command line options]
                queuer_files = args.input_files
                self.settings_add("QUEUER_FILES", queuer_files)
            else:
                queuer_files = self.settings["QUEUER_FILES"]
            queuer_args = f"{queuer_opts} {queuer_files}"
        else:
            # not applicable for batch fetcher
            queuer_args = "N/A"

        self.debug("queuer_type", queuer_type)
        self.debug("queuer_args", queuer_args)
        self.settings_add("QUEUER_ARGS", queuer_args)

        # will throw KeyError if vars not present
        self.settings_add(
            "PIPEVIEW_DATABASE_URL",
            "postgresql+psycopg://"
            "{PIPEVIEW_POSTGRES_USER}:{PIPEVIEW_POSTGRES_PASSWORD}"
            "@{PIPEVIEW_POSTGRES_CONTAINER}:{PIPEVIEW_PGPORT}"
            "/{PIPEVIEW_POSTGRES_DB}".format_map(self.settings),
        )

    def tag_prod(self) -> str:
        # pyproject version not used/updated.
        prefix = self.inst_flavor_prefix
        return f"{self.date_time}-{prefix}prod"

    ################ commands

    def deploy_cmd_init(self, cp: CmdParser) -> None:
        super().deploy_cmd_init(cp)
        # indexer specific options:
        cp.add_argument(
            "-H", "--hist-file-prefix", help="must start with /", default=""
        )
        cp.add_argument("-I", "--input-files", help="override default input files")
        cp.add_argument("-O", "--queuer-opts", help="override queuer sampling options")
        cp.add_argument(
            "-Y", "--hist-year", help="select year for 'historical' pipeline"
        )
        cp.add_argument("-z", "--no-import", help="do not run ES importer in pipeline")

    def deploy_cmd_helper(self, args: CmdArgs) -> None:
        super().deploy_cmd_helper(args)  # load config

        if SUPER_VERBOSE:
            print("======== settings")
            for key, val in self.settings.items():
                print(key, val)


d = StoryIndexerDeploy()
sys.exit(d.run())
