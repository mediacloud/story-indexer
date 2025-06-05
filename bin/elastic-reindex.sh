#!/bin/sh

show_help() {
  echo "Usage: $0 [OPTIONS]"
  echo ""
  echo "Reindex specific options:"
  echo "  -r, --source-remote REMOTE       URL of the remote Elasticsearch cluster to re-index from"
  echo "  -s, --source-index SOURCE        Source index name"
  echo "  -d, --dest-index DEST            Destination index name (default: mc_search)"
  echo "  -f, --from DATETIME              Start date for re-indexing (format: YYYY-MM-DDTHH:mm:ss.sssZ)"
  echo "  -t, --to DATETIME                End date for re-indexing (format: YYYY-MM-DDTHH:mm:ss.sssZ)"
  echo "  -b, --batch-size SIZE            Number of documents to re-index per batch"
  echo "  -i, --reindex-interval INTERVAL  Used to configure how often the re-indexing job runs via crontab (Default: 1h)"
  echo "  -w, --delay DELAY                Delay buffer between \"now\" and calculated --to time (Default: 2h)"
  echo ""
  echo "Execution options:"
  echo "  --run-once                       Execute re-indexing operation once and exit (no cron scheduling)"
  echo ""
  echo "Cron management options:"
  echo "  --set-cron                        Schedule re-indexing task using a cron job"
  echo "  --remove-cron                     Remove the scheduled re-indexing cron job"
  echo "  --update-cron                     Update the existing crontab entry with new configuration parameters"
  exit 0
}

if [ $# -eq 0 ] || [ "$1" = "-h" ] || [ "$1" = "--help" ]; then
  show_help
fi