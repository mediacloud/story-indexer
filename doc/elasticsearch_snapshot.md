## Local Filesystem Snapshot

### Specify Snapshot Repo

Update the ES configuration to specify the snapshot repository

`path.repo: "/usr/share/elasticsearch/backup"`

### Register Ssapshot

Use the Elasticsearch APi to register our snapshot repository

curl -X PUT "http://localhost:9200/\_snapshot/mc_es_backup" -H "Content-Type: application/json" -d '{
"type": "fs",
"settings": {
"location": "/usr/share/elasticsearch/backup",
}
}'

### Schedule snapshots

We can use Elasticsearch's API to regularly take snapshots. We can automate this using cron jobs (leveraging on `swarm-cronjob`)

curl -X PUT "http://localhost:9200/_snapshot/mc_es_backup/<my_snapshot>?wait_for_completion=true
