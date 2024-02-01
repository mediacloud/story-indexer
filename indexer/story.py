import json
import os
import pickle
import re
from dataclasses import dataclass, field, fields
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional
from uuid import NAMESPACE_URL, uuid3

import cchardet as chardet

from indexer.path import DATAPATH_BY_DATE, STORIES

"""
A single story interface object, with typed data fields for each pipeline step,
context management on each of those step datum, and a serialization scheme.
Subclassable with hooks for different storage backends
"""


def uuid_by_link(link: str) -> str:
    return uuid3(NAMESPACE_URL, link).hex


# enforces a specific naming pattern within this object, for concision and extensibility in the exit cb
# https://stackoverflow.com/questions/1175208/elegant-python-function-to-convert-camelcase-to-snake-case
def class_to_member_name(original_class: Callable, private: bool = True) -> str:
    name = original_class.__name__
    name = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", name)
    if private:
        prefix = "_"
    else:
        prefix = ""
    return prefix + re.sub("([a-z0-9])([A-Z])", r"\1_\2", name).lower()


@dataclass(kw_only=True)
class StoryData:
    """
    Core dataclass for each pipeline step, with context management
    """

    dirty: bool = False
    exit_cb: Callable = field(repr=False)
    frozen: bool = field(default=False, repr=False)
    CONTENT_TYPE: str = field(default="json", repr=False)
    MEMBER_NAME: str = field(default="", repr=False)
    internals: tuple = field(
        default=(
            "internals",
            "dirty",
            "frozen",
            "exit_cb",
            "CONTENT_TYPE",
            "MEMBER_NAME",
        ),
        repr=False,
    )

    # Freeze the thing only after initialization- otherwise we can't __init__ >.<
    # Convenient that dataclasses have this functionality, though.
    def __post_init__(self) -> None:
        self.MEMBER_NAME = class_to_member_name(self.__class__)
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
    fetch_date: Optional[str] = None  # date from input file name

    # none of the following are imported/indexed as of 2/2024.
    # they're being saved for internal tracing/debugging only.
    source_url: Optional[str] = None  # source tag url property
    source_feed_id: Optional[int] = None  # source tag mcFeedId property
    source_source_id: Optional[int] = None  # source tag mcFeedId property
    file_name: Optional[str] = None  # name or URL of source file


RSS_ENTRY = class_to_member_name(RSSEntry)


@dataclass(kw_only=True)
class RawHTML(StoryData):
    CONTENT_TYPE: str = "html"
    html: Optional[bytes] = None
    encoding: Optional[str] = None

    # A backup plan, in case setting encoding at fetch time fails for some reason.
    def guess_encoding(self) -> None:
        if self.html is None:
            return None
        else:
            self.encoding = chardet.detect(self.html)["encoding"]

    @property
    def unicode(self) -> Optional[str]:
        if self.html is None:
            return None
        else:
            if self.encoding is None:
                self.guess_encoding()
            assert isinstance(self.encoding, str)
            return self.html.decode(self.encoding, errors="replace")


RAW_HTML = class_to_member_name(RawHTML)


@dataclass(kw_only=True)
class HTTPMetadata(StoryData):
    response_code: Optional[int] = None
    fetch_timestamp: Optional[float] = None
    final_url: Optional[str] = None
    encoding: Optional[str] = None

    # ... there's more here, figure out later


HTTP_METADATA = class_to_member_name(HTTPMetadata)


@dataclass(kw_only=True)
class ContentMetadata(StoryData):
    original_url: Optional[str] = None
    url: Optional[str] = None
    normalized_url: Optional[str] = None
    canonical_domain: Optional[str] = None
    publication_date: Optional[str] = None
    language: Optional[str] = None
    full_language: Optional[str] = None
    text_extraction_method: Optional[str] = None
    article_title: Optional[str] = None
    normalized_article_title: Optional[str] = None
    text_content: Optional[str] = None
    is_homepage: Optional[bool] = None
    is_shortened: Optional[bool] = None

    # parsed_date used by importer to populate ES indexed_date, so
    # that stories reimported from WARC files get same indexed_date.
    # ISO format datetime (so researchers can query for new articles)
    # by indexed date.  Formatted string has microseconds, and ES
    # stores timestamps in millisecond resolution.  When reading old
    # WARC files, populated from WARC metadata record WARC-Date header.
    parsed_date: Optional[str] = None


CONTENT_METADATA = class_to_member_name(ContentMetadata)

###################################


class BaseStory:
    """
    Core story object, supporting serialization and context management for it's child StoryData entries.
    """

    dirty: bool = False
    # NB: this '_snake_case: CamelCase' convention is required
    _rss_entry: RSSEntry
    _raw_html: RawHTML
    _http_metadata: HTTPMetadata
    _content_metadata: ContentMetadata

    # Just one getter stub for each property. This pattern ensures that we don't have to redefine
    # the getters on each subclass.
    def rss_entry(self) -> RSSEntry:
        if not hasattr(self, RSS_ENTRY):
            uninitialized: RSSEntry = RSSEntry(exit_cb=self.context_exit_cb)
            self.load_metadata(uninitialized)

        return self._rss_entry

    def uuid(self) -> Optional[str]:
        # Used in serialization
        if self._rss_entry is None:
            return None
        else:
            link = self._rss_entry.link
            assert isinstance(link, str)
            return uuid_by_link(link)

    def raw_html(self) -> RawHTML:
        if not hasattr(self, RAW_HTML):
            uninitialized: RawHTML = RawHTML(exit_cb=self.context_exit_cb)
            self.load_metadata(uninitialized)

        return self._raw_html

    def http_metadata(self) -> HTTPMetadata:
        if not hasattr(self, HTTP_METADATA):
            uninitialized: HTTPMetadata = HTTPMetadata(exit_cb=self.context_exit_cb)
            self.load_metadata(uninitialized)

        return self._http_metadata

    def content_metadata(self) -> ContentMetadata:
        if not hasattr(self, CONTENT_METADATA):
            uninitialized: ContentMetadata = ContentMetadata(
                exit_cb=self.context_exit_cb
            )
            self.load_metadata(uninitialized)

        return self._content_metadata

    def context_exit_cb(self, story_data: StoryData) -> None:
        """
        All story_data exit through here- manage saving the result of any edits to the object, and
        pass on to subclass-specific storage routine
        """
        if story_data.dirty:
            self.dirty = story_data.dirty
            setattr(self, story_data.MEMBER_NAME, story_data)
            self.save_metadata(story_data)

    def save_metadata(self, story_data: StoryData) -> None:
        """
        Do subclass-specific storage routines here- none needed in the base story however.
        """
        pass

    def load_metadata(self, story_data: StoryData) -> None:
        """
        Do subclass-specific lazy loading routines here.
        In the base case, we only ever set the object we started with.
        """
        setattr(self, story_data.MEMBER_NAME, story_data)

    def dump(self) -> bytes:
        """
        Returns a queue-appropriate serialization of the the object- in this case just pickle bytes
        """
        return pickle.dumps(self)

    @classmethod
    def load(cls, serialized: bytes) -> Any:
        """
        Loads from a queue-appropriate serialization of the object.
        """
        return pickle.loads(serialized)


# A subclass which manages saving data to the disk
# will follow a pattern like this:
"""
data/
- {date}/
-- {stories}/
--- {link_hash}/
---- _rss_entry.json
---- _raw_html.html
---- _http_metadata.json
---- _content_metadata.json
-- ...
"""
# That way the serialized bytestring can just be the path from data to link_hash
# ...

# Some of the ways that project level paramaters like the data directory are managed here feel wrong, but
# there's room to come back to that later


class DiskStory(BaseStory):
    """
    The same interface as DiskStory, but this object manages saving fields to the filesystem, and serializes to
    a filesystem path instead of a whole object.
    """

    directory: Optional[str]
    path: Path
    loading: bool = False

    def __init__(self, directory: Optional[str] = None):
        self.directory = directory
        if self.directory is not None:
            self.path = Path(self.directory)

    # Using the dict interface to story_data so as to avoid typing issues.
    def init_storage(self, story_data: StoryData) -> None:
        data_dict: dict = story_data.as_dict()
        fetch_date = data_dict["fetch_date"]

        if fetch_date is None:
            # test case
            raise RuntimeError("Cannot init directory if RSSEntry.fetch_date is None")
        year, month, day = fetch_date.split("-")

        link = data_dict["link"]
        if link is None:
            raise RuntimeError("Cannot init directory if RSSEntry.link is None")

        data_path = DATAPATH_BY_DATE(fetch_date)
        self.directory = f"{data_path}{STORIES}/{self.uuid()}/"
        self.path = Path(self.directory)
        self.path.mkdir(parents=True, exist_ok=True)

    def save_metadata(self, story_data: StoryData) -> None:
        # Short circuit any callbacks on load.
        if self.loading:
            return

        name = story_data.MEMBER_NAME

        if self.directory is None:
            # This is bad sweng, I know- but I think this is an acceptable shortcut in this context
            # like, subclass-specific execution paths should live in the subclass itself, right?
            # but idk how to avoid this without a silly amount of extra engineering.
            # So 'init_storage' will accept StoryData but expect RSSEntry fields.
            if name == RSS_ENTRY:
                self.init_storage(story_data)

            else:
                raise RuntimeError(
                    "Cannot save if directory information is uninitialized"
                )

        if story_data.CONTENT_TYPE == "json":
            filepath = self.path.joinpath(name + ".json")
            with filepath.open("w") as output_file:
                json.dump(story_data.as_dict(), output_file)

        # special case for html
        if story_data.CONTENT_TYPE == "html":
            filepath = self.path.joinpath(name + ".html")
            filepath.write_bytes(story_data.as_dict()["html"])

    def load_metadata(self, story_data: StoryData) -> None:
        name = story_data.MEMBER_NAME

        # if directory location is undefined, just set the provided empty story_data
        if self.directory is None:
            setattr(self, name, story_data)
            return

        filepath = self.path.joinpath(f"{name}.{story_data.CONTENT_TYPE}")

        if not filepath.exists():
            setattr(self, name, story_data)
            return

        self.loading = True

        if story_data.CONTENT_TYPE == "json":
            with filepath.open("r") as input_file:
                content = json.load(input_file)
                with story_data:
                    story_data.load_dict(content)

        if story_data.CONTENT_TYPE == "html":
            content = filepath.read_bytes()
            with story_data:
                story_data.html = content

        self.loading = False
        setattr(self, name, story_data)

    def dump(self) -> bytes:
        """
        Returns a queue-appropriate serialization of the the object- this story with just a directory name, pickled.
        """
        if self.directory is not None:
            dumped_story: DiskStory = DiskStory(self.directory)
            return pickle.dumps(dumped_story)
        raise RuntimeError("Cannot dump story with uninitialized directory")


class StoryFactory:
    """
    A Story Factory- eg:
        Story = StoryFactory()
        new_story = Story()
        loaded_story = Story.load()
    """

    def __init__(self) -> None:
        self.iface = os.getenv("STORY_FACTORY", "BaseStory")
        self.classes = {
            "BaseStory": BaseStory,
            "DiskStory": DiskStory,
        }

    def __call__(self, *args: List, **kwargs: Dict[Any, Any]) -> BaseStory:
        instance = self.classes[self.iface](*args, **kwargs)
        assert isinstance(instance, BaseStory)
        return instance

    def load(self, serialized: bytes) -> Any:
        return pickle.loads(serialized)
