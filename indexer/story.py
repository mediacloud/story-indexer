import pickle
from dataclasses import dataclass
from typing import Any, Callable

# A single story interface object, with typed data fields for each pipeline step,
# context management on each of those step datum, and a serialization scheme.
# Subclassable with hooks for different storage backends


# dataclass over typeddict so that we can instantiate it with a method
# from the parent- keeps typing rules, added flexibility.
@dataclass
class StoryDatum:
    dirty: bool = False

    # I'm not sure how get the cm behavior in
    # https://github.com/mediacloud/story-indexer/issues/6#issuecomment-1591589186
    # without a circular composition. It makes me a little gassy, but it works!

    def __init__(self, exit_cb: Callable) -> None:
        self.exit_cb = exit_cb

    # implimenting typing on return:self is really finicky, just doing Any for now
    def __enter__(self) -> Any:
        return self

    def __setitem__(self, key: str, value: Any) -> None:
        super().__setattr__(key, value)
        self.dirty = True

    def __exit__(self, type: Any, value: Any, traceback: Any) -> None:
        self.exit_cb(self)


class RSSEntry(StoryDatum):
    link: str
    title: str
    domain: str
    pub_date: str


class BaseStory:
    dirty: bool = False
    rss_entry_data: RSSEntry

    def __init__(self) -> None:
        self.rss_entry_data: RSSEntry = RSSEntry(self.rss_entry_cb)

    # Will need the following two stubs for each property.
    # Maybe a better way to manage this when something is
    def rss_entry(self) -> RSSEntry:
        return self.rss_entry_data

    def rss_entry_cb(self, rss_entry: RSSEntry) -> None:
        self.dirty = rss_entry.dirty
        self.rss_entry_data = rss_entry
        self.sync()

    def sync(self) -> None:
        # Do subclass-specific storage routines here- none needed in the base story however.
        pass

    # For now just dump down the whole darn thing, why not.
    def dump(self) -> bytes:
        return pickle.dumps(self)

    @classmethod
    def load(cls, serialized: bytes) -> Any:
        return pickle.loads(serialized)


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
        rss_entry["link"] = sample_rss["link"]
        rss_entry["title"] = sample_rss["title"]
        rss_entry["domain"] = sample_rss["domain"]
        rss_entry["pub_date"] = sample_rss["pub_date"]

        print(rss_entry)

    print(story.rss_entry_data.link)

    b = story.dump()

    new_story: BaseStory = BaseStory.load(b)
    print(new_story.rss_entry_data.link)
