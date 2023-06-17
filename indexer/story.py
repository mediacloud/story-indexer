import pickle
import re
from dataclasses import dataclass, field, fields
from typing import Any, Callable, Optional

# A single story interface object, with typed data fields for each pipeline step,
# context management on each of those step datum, and a serialization scheme.
# Subclassable with hooks for different storage backends


# dataclass over typeddict so that we can instantiate it with a method
# from the parent- keeps typing rules, added flexibility.
@dataclass(kw_only=True)
class StoryData:
    dirty: bool = False
    exit_cb: Callable = field(repr=False)

    # implementing typing on return:self is really finicky, just doing Any for now
    def __enter__(self) -> Any:
        return self

    def __setattr__(self, key: str, value: Any) -> None:
        if key not in [f.name for f in fields(self)]:
            raise RuntimeError(f"Field {key} not defined for {self}")
        self.__dict__[key] = value
        if key != "dirty":  # lest we go in circles
            self.dirty = True

    def __exit__(self, type: Any, value: Any, traceback: Any) -> None:
        self.exit_cb(self)


@dataclass(kw_only=True)
class RSSEntry(StoryData):
    link: Optional[str] = None
    title: Optional[str] = None
    domain: Optional[str] = None
    pub_date: Optional[str] = None


class BaseStory:
    dirty: bool = False
    _rss_entry: RSSEntry

    def __init__(self) -> None:
        self._rss_entry: RSSEntry = RSSEntry(exit_cb=self.context_exit_cb)

    # Just one getter stub for each property
    def rss_entry(self) -> RSSEntry:
        return self._rss_entry

    # One cb to rule them all
    def context_exit_cb(self, story_data: StoryData) -> None:
        if story_data.dirty:
            self.dirty = story_data.dirty
            name = story_data.__class__.__name__
            private_name = camel_to_private_snake(name)
            setattr(self, private_name, story_data)
            self.dump_metadata(story_data)

    def dump_metadata(self, story_data: StoryData) -> None:
        # Do subclass-specific storage routines here- none needed in the base story however.
        pass

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


if __name__ == "__main__":
    sample_rss = {
        "link": "https://hudsontoday.com/stories/641939920-rep-payne-jr-opposes-republican-budget-bill-to-benefit-the-wealthy-and-punish-the-middle-class",
        "title": "Rep. Payne, Jr. Opposes Republican Budget Bill to Benefit the Wealthy and Punish the Middle Class",
        "domain": "hudsontoday.com",
        "pub_date": "Sun, 30 Apr 2023 23:08:47 -0000",
    }

    story: BaseStory = BaseStory()
    with story.rss_entry() as rss_entry:
        print(rss_entry)
        rss_entry.link = sample_rss["link"]
        rss_entry.title = sample_rss["title"]
        rss_entry.domain = sample_rss["domain"]
        rss_entry.pub_date = sample_rss["pub_date"]
        print(rss_entry.link)
        print(rss_entry)
        print(rss_entry.__class__.__name__)
        try:
            rss_entry.bad_attr = 0
        except Exception:
            print("Successfully prevented out of band attr setting")

    print(story.rss_entry().link)
    b = story.dump()

    new_story: BaseStory = BaseStory.load(b)
    print(new_story.rss_entry().link)
