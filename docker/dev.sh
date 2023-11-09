# sourced by deploy.sh to set private, deployment-specific values
# "export" is NOT NEEDED!
#
# If you're adding variable VARIABLE here, you MUST:
# 1. add it to staging & prod configs too!!!
# 2. add an "add VARIABLE # private" line to deploy.sh
# 3. expand {{variable}} in docker-compose.yml.j2
#
# PLEASE KEEP IN SORTED ORDER!!!
#
ARCHIVER_S3_ACCESS_KEY_ID=AKxxxxxx
ARCHIVER_S3_BUCKET=NO_ARCHIVE	# magical: leave temp files in place
ARCHIVER_S3_REGION=	# MUST be empty to disable S3 / check for "NO_ARCHIVE" bucket
ARCHIVER_S3_SECRET_ACCESS_KEY=..........
#
# SENTRY only enabled when SENTRY_DSN is non-empty
SENTRY_DSN=
SENTRY_ENVIRONMENT=disabled_by_empty_dsn
