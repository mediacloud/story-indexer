from enum import Enum
from functools import total_ordering

# We can add new states to this easily, just making sure to respect the ordering.
# I think the current state should represent essentially the farthest frontier of a job


@total_ordering
class WorkState(Enum):
    ERROR = -1
    INIT = 0
    RSS_READY = 1
    BATCHES_READY = 2
    BATCHES_FETCHING = 3
    BATCHES_FINISHED = 4
    DONE = 100

    def __lt__(self, other):
        if type(other) == type(self):
            return self.value < other.value

        return NotImplemented

# This state machine has no funcitonal use other than for visibility/debugging..


@total_ordering
class BatchState(Enum):
    READY = 0
    FETCHING = 1
    FINISHED = 2
    ERROR = -1

    def __lt__(self, other):
        if type(other) == type(self):
            return self.value < other.value

        return NotImplemented
