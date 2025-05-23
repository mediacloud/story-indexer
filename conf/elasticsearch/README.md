# Elasticsearch Configuration Files

By default ILM checks every 10 minutes if there’s any action to execute. In our example we had to use very small intervals (seconds, minutes), for testing.
Execute the following command to execute ILM

Test ILM policy

```yaml
{
    "name": "mediacloud-lifecycle-policy",
    "policy": {
      "phases": {
        "hot": {
          "actions": {
            "rollover": {
              "max_age": "10s"
            }
          }
        }
      }
    },
    "_meta": {
      "description": "built-in ILM policy using the hot phase with a rollover of 10s,
      "managed": true
    }
}
```

`
PUT _cluster/settings { "transient": { "indices.lifecycle.poll_interval": "15s"  } }
`
These configuration files are exported from the Elasticsearch host nodes as reference for the existing configuration.

To create a new Elasticsearch node configuration, refer to the script at `docker/elastic-deploy.sh`

### Scripts

```sh
./es-reindex-continuous.sh -e staging -s mc_search-000006 -d mc_search -f 2025-04-31T23:59:59.000Z -c
```

Run continuous re-indexing

This initiates a re-indexing task from start time (mc_reindex_from_date) and this is incremental every (reindex-interval (default:2hours))
e.g If start time is 2025-04-31T23:59:59.000Z, on the first run we re-index from 2025-04-31T23:59:59.000Z to now(e.g 2025-05-12T15:49:00.000Z)-2hours ~= 2025-05-12T13:49:00.000Z
A cron job that runs ever */re-index interval hours will execute the next task from
2025-05-12T13:49:00.000Z to 2025-05-12T15:49:00.000Z
