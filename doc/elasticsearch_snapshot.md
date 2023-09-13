## Local Filesystem Snapshot

### Specify Snapshot Repo

Update the ES configuration to specify the snapshot repository

`path.repo: "/usr/share/elasticsearch/backup"`

### Register Snapshot

Use the Elasticsearch API to register our snapshot repository

```sh
curl -X PUT "http://localhost:9200/\_snapshot/mc-es-backup" -H "Content-Type: application/json" -d '{
"type": "fs",
"settings": {
"location": "/usr/share/elasticsearch/backup",
}
}'

### Snapshot SLM policy

Using Snapshot lifecycle management (SLM) to regularly backup our ES cluster.
The SLM policy automatically takes snapshosts on a preset schedule, example below takes snapshots daily at 2.30AM

```sh
curl -X PUT "http://localhost:9200/_slm/policy/mc_daily_snapshot_policy
'{
  "schedule": "0 30 2 * * ?",  # Cron expression for scheduling (e.g., daily at 2:30 AM)
  "name": "mc-es-snapshot-{now/d}",
  "repository": "mc-es-backup",  # Name of your repository
  "config": {
    "indices": ["mediacloud_search_text"],  # You can specify indices to snapshot, use "*" to match all
    "ignore_unavailable": true,
    "include_global_state": false
  },
  "retention": {
    "expire_after": "60d",  # How long to retain snapshots (e.g., 60 days)
    "min_count": 1,
    "max_count": 50  # Maximum number of snapshots to keep
  }
}'
"

### Manually run SLM policy

To run the SLM policy immediately to create a snapshot, outside the SLM schedule

curl X -POST "http://localhost:9200/_slm/policy/mc_daily_snapshot_policy/_execute"

### Manually create snapshots

Create the snapshot without SLM policy, from ES create snapshot API

curl X -POST "http://localhost:9200/_snapshot/mc-es-backup/snapshot_{now/d}?wait_for_completion=true"


<!-- ### Schedule snapshots

We can use Elasticsearch's API to regularly take snapshots. We can automate this using cron jobs (leveraging on `swarm-cronjob`)

curl -X PUT "http://localhost:9200/_snapshot/mc_es_backup/<my_snapshot>?wait_for_completion=true -->
