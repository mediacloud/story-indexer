"""
common values for batch and queue fetchers
"""

B2_BASE = "https://mediacloud-public.s3.us-east-005.backblazeb2.com"
S3_BASE = "https://mediacloud-public.s3.amazonaws.com"

# starts 2022-02-18
# object name format is mc-YYYY-MM-DD.rss.gz for both:
_RSS_FETCHER_B2_URL_BASE = f"{B2_BASE}/daily-rss/rss-fetcher"
_RSS_FETCHER_S3_URL_BASE = f"{S3_BASE}/backup-daily-rss"  # ends 2024-08-15

RSS_FETCHER_URL_BASE = _RSS_FETCHER_B2_URL_BASE

# for completeness: legacy system files:
# date range 2022-02-22 to 2023-12-07
_LEGACY_B2_URL_BASE = f"{B2_BASE}/daily-rss/old"  # files gzip'ed
_LEGACY_S3_URL_BASE = f"{S3_BASE}/daily-rss"  # files NOT gzip'ed
