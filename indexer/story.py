import json
import pickle
import re
from dataclasses import asdict, dataclass, field, fields
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Optional, Union, overload

from indexer.path import DATAROOT

# A single story interface object, with typed data fields for each pipeline step,
# context management on each of those step datum, and a serialization scheme.
# Subclassable with hooks for different storage backends


# Core dataclass for each pipeline step, with context management
@dataclass(kw_only=True)
class StoryData:
    dirty: bool = False
    exit_cb: Callable = field(repr=False)
    frozen: bool = field(default=False, repr=False)
    internals: tuple = field(
        default=("dirty", "frozen", "exit_cb", "internals"), repr=False
    )

    # Freeze the thing only after initialization- otherwise we can't __init__ >.<
    # Convenient that dataclasses have this functionality, though.
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

    def as_dict(self) -> dict:
        output: dict = {}
        for key in fields(self):
            if key.name not in self.internals:
                output[key.name] = getattr(self, key.name)

        return output

    # As a convenience for loading in values from a storage interface.
    def load_dict(self, load_dict: dict) -> None:
        field_names: list[str] = [f.name for f in fields(self)]
        for key, value in load_dict.items():
            if key in field_names:
                setattr(self, key, value)


@dataclass(kw_only=True)
class RSSEntry(StoryData):
    link: Optional[str] = None
    title: Optional[str] = None
    domain: Optional[str] = None
    pub_date: Optional[str] = None
    fetch_date: Optional[str] = None


RSSENTRY = "_rss_entry"


# Core Story Object
class BaseStory:
    dirty: bool = False
    # NB: this '_snake_case: CamelCase' convention is required
    _rss_entry: RSSEntry

    # Just one getter stub for each property. This pattern ensures that we don't have to redefine
    # the getters on each subclass.
    def rss_entry(self) -> RSSEntry:
        if not hasattr(self, RSSENTRY):
            uninitialized: RSSEntry = RSSEntry(exit_cb=self.context_exit_cb)
            self.load_metadata(uninitialized)

        return self._rss_entry

    # One cb to rule them all
    def context_exit_cb(self, story_data: StoryData) -> None:
        if story_data.dirty:
            self.dirty = story_data.dirty
            name = story_data.__class__.__name__
            private_name = camel_to_snake(name)
            setattr(self, private_name, story_data)
            self.save_metadata(story_data)

    def save_metadata(self, story_data: StoryData) -> None:
        # Do subclass-specific storage routines here- none needed in the base story however.
        pass

    def load_metadata(self, story_data: StoryData) -> None:
        # Do subclass-specific lazy loading routines here.
        # In the base case, we only ever set the object we started with.
        name = story_data.__class__.__name__
        private_name = camel_to_snake(name)
        setattr(self, private_name, story_data)

    # For now just dump down the whole darn thing, why not.
    def dump(self) -> bytes:
        return pickle.dumps(self)

    @classmethod
    def load(cls, serialized: bytes) -> Any:
        return pickle.loads(serialized)


# enforces a specific naming pattern within this object, for concision and extensibility in the exit cb
# https://stackoverflow.com/questions/1175208/elegant-python-function-to-convert-camelcase-to-snake-case
def camel_to_snake(name: str, private: bool = True) -> str:
    name = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", name)
    if private:
        prefix = "_"
    else:
        prefix = ""
    return prefix + re.sub("([a-z0-9])([A-Z])", r"\1_\2", name).lower()


# A subclass which manages saving data to the disk
# will follow a pattern like this:
"""
data/
- {date}/
-- {link_hash}/
--- rss_entry.json
--- original_html.html
--- http_metadata.json
--- content_metadata.json
-- ...
"""
# That way the serialized bytestring can just be b'{date}/{link_hash}'
# Two kinks- one is that we need some way to mark that the html is stored as html- that's easy enough though.
# Two is that the link_hash from the first draft didn't work for long url, filesystem has a length limit.
# ...

# Some of the ways that project level paramaters like the data directory are managed here feel wrong, but
# there's room to come back to that later


class DiskStory(BaseStory):
    filetypes: dict[str, str] = {
        "_rss_entry": "json",
    }

    directory: Optional[str]
    path: Path
    loading: bool = False

    def __init__(self, directory: Optional[str] = None):
        self.directory = directory
        if self.directory is not None:
            self.path = Path(f"{DATAROOT()}{self.directory}")

    # The original way to handle this- there might be some way to further compress so we're always in the limit.
    # Maybe surt is the way? Do we care if it's reversable?
    def link_hash(self, link: str) -> str:
        return link.replace("/", "\\")

    # Using the dict interface to story_data so as to avoid typing issues.
    def init_storage(self, story_data: StoryData) -> None:
        data_dict: dict = story_data.as_dict()
        fetch_date = data_dict["fetch_date"]

        if fetch_date is None:
            raise RuntimeError("Cannot init directory if RSSEntry.fetch_date is None")
        year, month, day = fetch_date.split("-")

        link = data_dict["link"]
        if link is None:
            raise RuntimeError("Cannot init directory if RSSEntry.link is None")

        self.directory = f"{year}/{month}/{day}/{self.link_hash(link)}/"
        self.path = Path(f"{DATAROOT()}{self.directory}")
        self.path.mkdir(parents=True, exist_ok=True)

    def save_metadata(self, story_data: StoryData) -> None:
        # Short circuit any callbacks on load.
        if self.loading:
            return

        name = camel_to_snake(story_data.__class__.__name__)

        if self.directory is None:
            # This is bad sweng, I know- but I think this is an acceptable shortcut in this context
            # like, subclass-specific execution paths should live in the subclass itself, right?
            # but idk how to avoid this without a silly amount of extra engineering.
            # So 'init_storage' will accept StoryData but expect RSSEntry fields. Sorry.
            if name == RSSENTRY:
                self.init_storage(story_data)

            else:
                raise RuntimeError(
                    "Cannot save if directory information is uninitialized"
                )

        if self.filetypes[name] == "json":
            filepath = self.path.joinpath(name + ".json")
            with filepath.open("w") as output_file:
                json.dump(story_data.as_dict(), output_file)

    def load_metadata(self, story_data: StoryData) -> None:
        name = camel_to_snake(story_data.__class__.__name__)

        # if directory location is undefined, just set the provided empty story_data
        if self.directory is None:
            setattr(self, name, story_data)
            return

        filepath = self.path.joinpath(f"{name}.json")

        if not filepath.exists():
            setattr(self, name, story_data)
            return

        self.loading = True
        if self.filetypes[name] == "json":
            with filepath.open("r") as input_file:
                content = json.load(input_file)
                with story_data:
                    story_data.load_dict(content)

        self.loading = False
        setattr(self, name, story_data)

    def dump(self) -> bytes:
        if self.directory is not None:
            return str.encode(self.directory)
        raise RuntimeError("Cannot dump story with uninitialized directory")

    @classmethod
    def load(cls, serialized: bytes) -> Any:
        return DiskStory(serialized.decode("utf8"))
