import pytest

from indexer.story import BaseStory


class TestBaseStory:
    sample_rss = {
        "link": "https://hudsontoday.com/stories/641939920-rep-payne-jr-opposes-republican-budget-bill-to-benefit-the-wealthy-and-punish-the-middle-class",
        "title": "Rep. Payne, Jr. Opposes Republican Budget Bill to Benefit the Wealthy and Punish the Middle Class",
        "domain": "hudsontoday.com",
        "pub_date": "Sun, 30 Apr 2023 23:08:47 -0000",
    }

    def test_write_data(self) -> None:
        story: BaseStory = BaseStory()
        with story.rss_entry() as rss_entry:
            rss_entry.link = self.sample_rss["link"]
            rss_entry.title = self.sample_rss["title"]
            rss_entry.domain = self.sample_rss["domain"]
            rss_entry.pub_date = self.sample_rss["pub_date"]

        rss_entry = story.rss_entry()
        assert rss_entry.link == self.sample_rss["link"]
        assert rss_entry.title == self.sample_rss["title"]
        assert rss_entry.domain == self.sample_rss["domain"]
        assert rss_entry.pub_date == self.sample_rss["pub_date"]

    def test_dump_story(self) -> None:
        story: BaseStory = BaseStory()
        with story.rss_entry() as rss_entry:
            rss_entry.link = self.sample_rss["link"]
            rss_entry.title = self.sample_rss["title"]
            rss_entry.domain = self.sample_rss["domain"]
            rss_entry.pub_date = self.sample_rss["pub_date"]

        dumped: bytes = story.dump()
        new_story: BaseStory = BaseStory.load(dumped)

        rss_entry = new_story.rss_entry()
        assert rss_entry.link == self.sample_rss["link"]
        assert rss_entry.title == self.sample_rss["title"]
        assert rss_entry.domain == self.sample_rss["domain"]
        assert rss_entry.pub_date == self.sample_rss["pub_date"]

    def test_no_frozen_writes(self) -> None:
        with pytest.raises(RuntimeError):
            story: BaseStory = BaseStory()
            rss_entry = story.rss_entry()
            rss_entry.link = self.sample_rss["link"]

    def test_no_new_attrs(self) -> None:
        with pytest.raises(RuntimeError):
            story: BaseStory = BaseStory()
            rss_entry = story.rss_entry()
            rss_entry.new_attr = True
