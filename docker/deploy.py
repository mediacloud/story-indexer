"""
START of deploy script
"""

import sys

from mc_deploy.docker import DockerDeploy


class StoryIndexerDeploy(DockerDeploy):
    INST_BASE = "indexer"  # app base name
    PROJECT_REPO = "story-indexer"


d = StoryIndexerDeploy()
sys.exit(d.run())
