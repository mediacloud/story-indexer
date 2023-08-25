## Local Filesystem Snapshot

### Register SNapshot

curl -X PUT "http://localhost:9200/\_snapshot/mc_es_backup" -H "Content-Type: application/json" -d '{
"type": "fs",
"settings": {
"location": "/usr/share/elasticsearch/backup",
}
}'

### Schedule snapshots
