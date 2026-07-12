"""
START of deploy script
"""

import sys

from mc_deploy.docker import DockerDeploy


class StoryIndexerDeploy(DockerDeploy):
    INST_BASE = "indexer"  # app base name

    # map command line names to inst_base prefix & port bias
    # ES usesports 9200 & 9300, so bias values increase by 200
    INST_FLAVORS = {
        "queue-fetcher": ("", 0),
        "batch-fetcher": ("", 0),
        "historical": ("hist-", 200),
        "archive": ("arch-", 400),
        "csv": ("csv-", 600),
    }
    PROJECT_REPO = "story-indexer"

    def tag_prod(self) -> str:
        # pyproject version not updated.
        prefix = self.inst_flavor_prefix
        return f"{self.date_time}-{prefix}prod"


d = StoryIndexerDeploy()
sys.exit(d.run())
