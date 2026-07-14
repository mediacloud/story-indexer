"""
START of mc-deploy script for story-indexer to replace deploy.sh
"""

# TODO:
# make sure all indexer args used!!

import os
import re
import sys
from enum import Enum

from mc_deploy.base import CmdArgs, CmdParser
from mc_deploy.docker import DockerDeploy


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
    # ES usesports 9200 & 9300, so bias values increase by 200
    INST_FLAVORS = {
        "queue-fetcher": ("", 0),
        # "batch-fetcher": ("", 0), # scrapy removed from requirements
        "historical": ("hist-", 200),
        "archive": ("arch-", 400),
        "csv": ("csv-", 600),
    }
    PROJECT_REPO = "story-indexer"

    def deploy_default_settings(self, args: CmdArgs) -> None:  # noqa: C901
        """
        called before deploy_cmd_helper to set defaults
        before settings files loaded

        SIGH: deploy_cmd_helper sets port_bias!
        PORT values never set in actual config!!
        """

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

        self.settings_add("PIPEVIEW_API_PORT", "8000")  # internal port
        self.settings_add(
            "PIPEVIEW_API_PORT_BASE_EXPORTED", str(48000 + self.port_bias)
        )
        self.settings_add("PIPEVIEW_DAYS", "90")
        self.settings_add("PIPEVIEW_PGPORT", "5432")  # native internal port
        self.settings_add("PIPEVIEW_POSTGRES_CONTAINER", "pipeview-db")
        self.settings_add("PIPEVIEW_POSTGRES_DB", "pipeview")
        self.settings_add("PIPEVIEW_POSTGRES_IMAGE", "postgres:18-alpine")
        self.settings_add(
            "PIPEVIEW_POSTGRES_PORT_BASE_EXPORTED", str(54320 + self.port_bias)
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
        # Pushing to a local registry for now, while in dev loop.
        # set registry differently based on BRANCH?!
        # MUST have trailing slash unless empty
        worker_image_registry = "localhost:5000/"
        # PLB: maybe indexer-common, now that it's used for config & stats reporting?
        worker_image_name = "indexer-worker"

        if self.is_dev():
            # testing: disable registry push for development
            # We always run on just one node.
            # Images accumulate in registry volume.
            worker_image_registry = ""

        self.settings_add("IMPORTER_ARGS", "")
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
        elif self.inst_flavor == "csv":
            arch_suffix = "csv"
            self.settings_add("QUEUER_TYPE", "csv-queuer")  # name of run- script
            self.settings_add(
                "QUEUER_FILES", "s3://mediacloud-database-e-files/csv_files/"
            )
        else:
            self.fatal(f"Unknown pipeline flavor {self.inst_flavor}")

        if args.no_import:
            importer_args.append("--no-import")

        # If explicit input files supplied for "normal" (RSS/puller) pipeline,
        # change stack/stats prefix from empty to "rss-" to distinguish from
        # regular/prod stack.  NOTE: rss-puller only run if OPT_INPUTS is
        # empty, so will always be processing rss files.
        if args.input_files and self.inst_flavor in ("queue-fetcher", "batch-fetcher"):
            # XXX prefix inst_name with rss-????
            arch_suffix = "rss"

        if self.is_prod():
            arch_prefix = f"mc{arch_suffix}"

            self.settings_add("ELASTICSEARCH_CONTAINERS", "0")
            # ES index settings are static, prod settings should not change
            self.settings_add("ELASTICSEARCH_SHARD_COUNT", "12")
            self.settings_add("ELASTICSEARCH_SHARD_REPLICAS", "1")
            self.settings_add("ELASTICSEARCH_ILM_MAX_SHARD_SIZE", "50gb")
            self.settings_add("ELASTICSEARCH_HOSTS", "http://es.newsscribe.angwin:9209")
            self.settings_add("ELASTICSEARCH_SNAPSHOT_REPO_TYPE", "s3")

            # for RabbitMQ and worker_data:
            self.settings_add(
                "VOLUME_DEVICE_PREFIX", f"/srv/data/docker/{self.inst_name}"
            )  # XXX
            self.settings_add("SENTRY_ENVIRONMENT", "production")  # XXX
        elif self.is_staging():
            arch_prefix = f"staging{arch_suffix}"
            self.settings_add("ELASTICSEARCH_CONTAINERS", "3")
            self.settings_add("ELASTICSEARCH_SHARD_COUNT", "5")
            self.settings_add("ELASTICSEARCH_SHARD_REPLICAS", "1")
            self.settings_add("ELASTICSEARCH_ILM_MAX_SHARD_SIZE", "5gb")
            self.settings_add(
                "ELASTICSEARCH_SNAPSHOT_REPO_SETTINGS_LOCATION", "mc_story_indexer"
            )

            story_limit = 50000

            # don't run daily, fetch 10x more than dev:
            self.settings_add("FETCHER_CRONJOB_ENABLE", "false")
            fetcher_options.append(f"--sample-size={story_limit}")
            self.settings_add("FETCHER_NUM_BATCHES", "10")  # betch fetcher

            self.settings_add("QUEUER_CRONJOB_ENABLE", "false")
            self.settings_add("QUEUER_CRONJOB_REPLICAS", "0")
            self.settings_add("QUEUER_INITIAL_REPLICAS", "1")
            self.settings_add(
                "VOLUME_DEVICE_PREFIX", f"/srv/data/docker/{self.inst_name}"
            )  # XXX
        else:
            arch_prefix = f"{self.login_user}{arch_suffix}"
            self.settings_add("ELASTICSEARCH_CONTAINERS", "1")
            self.settings_add("ELASTICSEARCH_SHARD_COUNT", "2")
            self.settings_add("ELASTICSEARCH_SHARD_REPLICAS", "1")
            self.settings_add("ELASTICSEARCH_ILM_MAX_SHARD_SIZE", "100mb")
            self.settings_add(
                "ELASTICSEARCH_SNAPSHOT_REPO_SETTINGS_LOCATION", "mc_story_indexer"
            )

            story_limit = 5000

            # batch fetcher:
            # fetch limited articles under development, don't run daily:
            self.settings_add("FETCHER_CRONJOB_ENABLE", "false")
            fetcher_options.append(f"--sample-size={story_limit}")
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
            self.settings_add("WORKER_PLACEMENT_CONSTRAINT", "node.role == manager")

        # identification unique to this deployment
        # used as an exchange name to signal pipeline config complete
        # MUST start with mc-configuration- and be less than 256 bytes:
        self.settings_add(
            "DEPLOYMENT_ID", f"mc-configuration-{self.date_time}-{self.inst_name}"
        )

        # use git tag for image tag.
        # in development this means old tagged images will pile up until removed
        # (used to allow "dirty" builds where tag was always "dirty")
        worker_image_tag = re.sub(r"[^a-zA-Z0-9_.-]", "_", self.tag)

        self.settings_add("WORKER_IMAGE_NAME", worker_image_name)
        self.settings_add(
            "WORKER_IMAGE_FULL",
            f"{worker_image_registry}{worker_image_name}:{worker_image_tag}",
        )

        # allow multiple deploys on same swarm/cluster:
        self.settings_add("NETWORK_NAME", self.inst_name)
        self.settings_add("STACK_NAME", self.inst_name)

        self.settings_add("RABBITMQ_PORT_EXPORTED", str(rabbitmq_port + self.port_bias))

        self.input_files = args.input_files

    def docker_compose_file_create(self) -> None:
        self.jinja_vars: dict[str, str | int] = {}

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
        if False:
            for key, val in self.jinja_vars.items():
                print(key, val)

    def settings_get_new(self) -> None:
        """
        load project settings
        """
        super().settings_get_new()
        self.settings_add("STATSD_URL", self.statsd_url)
        self.settings_add("STATSD_REALM", self.inst_id)
        if self.is_prod_staging():
            if self.is_prod():
                file = "prod.sh"
            else:
                file = "staging.sh"  # stand-alone (not overrides to prod)
            self.settings_load_private_files(f"{self.PROJECT_REPO}-config", [file])
        else:
            self.settings_load_file(os.path.join(self.deploy_dir, "dev.sh"))

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

        cp.add_argument("-Q", "--queuer-args", help="override queuer sampling args")
        cp.add_argument(
            "-Y", "--hist-year", help="select year for 'historical' pipeline"
        )
        cp.add_argument("-z", "--no-import", help="do not run ES importer in pipeline")

    def deploy_cmd_helper(self, args: CmdArgs) -> None:
        super().deploy_cmd_helper(args)
        self.deploy_default_settings(args)  # wants port_bias (set by _helper)

        if False:
            for key, val in self.settings.items():
                print(key, val)


d = StoryIndexerDeploy()
sys.exit(d.run())
