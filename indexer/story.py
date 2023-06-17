import pickle
import re
from dataclasses import dataclass, field, fields
from typing import Any, Callable, Optional

# A single story interface object, with typed data fields for each pipeline step,
# context management on each of those step datum, and a serialization scheme.
# Subclassable with hooks for different storage backends


# Core dataclass for each pipeline step, with context management
@dataclass(kw_only=True)
class StoryData:
    dirty: bool = False
    exit_cb: Callable = field(repr=False)
    frozen: bool = field(default=False, repr=False)
    internals: tuple = field(default=("dirty", "frozen", "exit_cb"), repr=False)

    # Freeze the thing only after initialization- otherwise we can't __init__ >.<
    def __post_init__(self) -> None:
        self.frozen = True

    # Implementing typing on return:self is really finicky, just doing Any for now
    def __enter__(self) -> Any:
        self.frozen = False
        return self

    def __setattr__(self, key: str, value: Any) -> None:
        if self.frozen and key not in self.internals:
            raise RuntimeError(
                "Attempting write on frozen StoryData, outside of 'with'"
            )

        if key not in [f.name for f in fields(self)]:
            raise RuntimeError(f"Field {key} not defined for {self.__class__.__name__}")

        self.__dict__[key] = value

        # lest we go in circles
        if key not in self.internals:
            self.dirty = True

    def __exit__(self, type: Any, value: Any, traceback: Any) -> None:
        self.frozen = True
        self.exit_cb(self)


# Example
@dataclass(kw_only=True)
class RSSEntry(StoryData):
    link: Optional[str] = None
    title: Optional[str] = None
    domain: Optional[str] = None
    pub_date: Optional[str] = None


# Core Story Object
class BaseStory:
    dirty: bool = False
    # NB: this '_snake_case: CamelCase' convention is required
    _rss_entry: RSSEntry

    # Just one getter stub for each property. This pattern ensures that we don't have to redefine
    # the getters on each subclass.
    def rss_entry(self) -> RSSEntry:
        if not hasattr(self, "_rss_entry"):
            uninitialized: RSSEntry = RSSEntry(exit_cb=self.context_exit_cb)
            self.load_metadata(uninitialized)

        return self._rss_entry

    # One cb to rule them all
    def context_exit_cb(self, story_data: StoryData) -> None:
        if story_data.dirty:
            self.dirty = story_data.dirty
            name = story_data.__class__.__name__
            private_name = camel_to_private_snake(name)
            setattr(self, private_name, story_data)
            self.save_metadata(story_data)

    def save_metadata(self, story_data: StoryData) -> None:
        # Do subclass-specific storage routines here- none needed in the base story however.
        pass

    def load_metadata(self, story_data: StoryData) -> None:
        # Do subclass-specific lazy loading routines here.
        # In the base case, we only ever return the same object we started with
        name = story_data.__class__.__name__
        private_name = camel_to_private_snake(name)
        setattr(self, private_name, story_data)

    # For now just dump down the whole darn thing, why not.
    def dump(self) -> bytes:
        return pickle.dumps(self)

    @classmethod
    def load(cls, serialized: bytes) -> Any:
        return pickle.loads(serialized)


# enforces a specific naming pattern within this object, for concision and extensibility in the exit cb
# https://stackoverflow.com/questions/1175208/elegant-python-function-to-convert-camelcase-to-snake-case
def camel_to_private_snake(name: str) -> str:
    name = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", name)
    return "_" + re.sub("([a-z0-9])([A-Z])", r"\1_\2", name).lower()


# A subclass which manages saving data to the disk, and uses the rss
class DiskStory(BaseStory):
    def save_metadata(self, story_data: StoryData) -> None:
        pass

    def load_data(self, story_data: StoryData) -> None:
        pass

    def dump(self) -> bytes:
        return b""

    @classmethod
    def load(cls, serialized: bytes) -> Any:
        pass
