import json
import pickle
import re
from dataclasses import asdict, dataclass, field, fields
from datetime import datetime
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

    # could just be a dict comprehension, but mypy is a tyrant! (with love)
    def as_dict(self) -> dict:
        output: dict = {}
        for key in fields(self):
            if key.name not in self.internals:
                output[key] = getattr(self, key.name)
        return output


# Example
@dataclass(kw_only=True)
class RSSEntry(StoryData):
    link: Optional[str] = None
    title: Optional[str] = None
    domain: Optional[str] = None
    pub_date: Optional[str] = None
    fetch_date: Optional[str] = None


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
        "rss_entry": "json",
    }

    directory: Optional[str]
    data_root: str = "data/"

    def __init__(self, directory: Optional[str] = None):
        self.directory = directory

    # The original way to handle this- there might be some way to further compress so we're always in the limit.
    # Maybe surt is the way? Do we care if it's reversable?
    def link_hash(self, link: str) -> str:
        return link.replace("/", "\\")

    # Just approaching this with string comprehensions for now.
    # def init_directory(self, rss_entry: StoryData) -> None:
    #    fetch_date = rss_entry.fetch_date
    #    if fetch_date is None:
    #        raise RuntimeError("Cannot init directory if RSSEntry.fetch_date is None")
    #    year, month, day = fetch_date.split("-")

    #    link = rss_entry.link
    #    if link is None:
    #        raise RuntimeError("Cannot init directory if RSSEntry.link is None")

    # Need to do some pathlib schtuff to make sure the whole path actually exists
    #    self.directory = f"{year}/{month}/{day}/{self.link_hash(link)}/"

    # There will always be an init path, and it is called after the first context exit.
    def save_metadata(self, story_data: StoryData) -> None:
        name = camel_to_snake(story_data.__class__.__name__, private=False)

        if self.directory is None:
            if name != "rss_entry":
                raise RuntimeError(
                    "Cannot save if directory is None, ex. if date/link info is missing."
                )
            # else:
            # self.init_directory(story_data)

        if self.filetypes[name] == "json":
            path = f"{self.data_root}{self.directory}{name}.json"
            with open(path, "w") as output_file:
                json.dump(story_data.as_dict(), output_file)

    # Need some way to handle the first initialization...
    def load_metadata(self, story_data: StoryData) -> None:
        if self.directory is None:
            raise RuntimeError("Cannot load from uninitialized story")

        # name = camel_to_snake(story_data.__class__.__name__, private= False)
        # if self.filetypes[name] == "json":
        # path = f"{self.data_root}{self.directory}{name}.json"
        # load the file
        # marshall it into the story_data
        # set the story data on self.
        # Keep on keeping on

    def dump(self) -> bytes:
        if self.directory is not None:
            return str.encode(self.directory)
        raise RuntimeError("Cannot dump story with uninitialized directory")

    @classmethod
    def load(cls, serialized: bytes) -> Any:
        return DiskStory(serialized.decode("utf8"))
