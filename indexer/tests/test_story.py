import os
import shutil
from importlib import reload
from typing import Any, Generator

import pytest

from indexer.story import BaseStory, DiskStory, StoryFactory

TEST_DATA_DIR = "test_data/"


class TestBaseStory:
    sample_rss = {
        "link": "https://hudsontoday.com/stories/641939920-rep-payne-jr-opposes-republican-budget-bill-to-benefit-the-wealthy-and-punish-the-middle-class",
        "title": "Rep. Payne, Jr. Opposes Republican Budget Bill to Benefit the Wealthy and Punish the Middle Class",
        "domain": "hudsontoday.com",
        "pub_date": "Sun, 30 Apr 2023 23:08:47 -0000",
        "fetch_date": "2023-05-01",
    }

    test_html = b"<html> <body> abracadabra </body> </html>"

    def test_write_data(self) -> None:
        story: BaseStory = BaseStory()
        with story.rss_entry() as rss_entry:
            rss_entry.link = self.sample_rss["link"]
            rss_entry.title = self.sample_rss["title"]
            rss_entry.domain = self.sample_rss["domain"]
            rss_entry.pub_date = self.sample_rss["pub_date"]
            rss_entry.fetch_date = self.sample_rss["fetch_date"]

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
            rss_entry.fetch_date = self.sample_rss["fetch_date"]

        dumped: bytes = story.dump()
        new_story: BaseStory = BaseStory.load(dumped)

        rss_entry = new_story.rss_entry()
        assert rss_entry.link == self.sample_rss["link"]
        assert rss_entry.title == self.sample_rss["title"]
        assert rss_entry.domain == self.sample_rss["domain"]
        assert rss_entry.pub_date == self.sample_rss["pub_date"]

    def test_multiple_fields(self) -> None:
        story: BaseStory = BaseStory()
        with story.rss_entry() as rss_entry:
            rss_entry.link = self.sample_rss["link"]
            rss_entry.title = self.sample_rss["title"]
            rss_entry.domain = self.sample_rss["domain"]
            rss_entry.pub_date = self.sample_rss["pub_date"]
            rss_entry.fetch_date = self.sample_rss["fetch_date"]

        dumped: bytes = story.dump()

        new_story: BaseStory = BaseStory.load(dumped)
        with new_story.raw_html() as raw_html:
            raw_html.html = self.test_html

        dumped_again: bytes = new_story.dump()

        third_story: BaseStory = BaseStory.load(dumped_again)
        rss_entry = third_story.rss_entry()
        assert rss_entry.link == self.sample_rss["link"]
        raw_html = third_story.raw_html()
        assert raw_html.html == self.test_html

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

    def test_unicode(self) -> None:
        story: BaseStory = BaseStory()
        with story.rss_entry() as rss_entry:
            rss_entry.link = self.sample_rss["link"]
            rss_entry.title = self.sample_rss["title"]
            rss_entry.domain = self.sample_rss["domain"]
            rss_entry.pub_date = self.sample_rss["pub_date"]
            rss_entry.fetch_date = self.sample_rss["fetch_date"]

        assert story.raw_html().encoding is None
        assert story.raw_html().unicode is None

        local_html = "html_fixtures/641939920-rep-payne-jr-opposes-republican-budget-bill-to-benefit-the-wealthy-and-punish-the-middle-class"
        with open(local_html, "rb") as html_fixture:
            html = html_fixture.read()
            with story.raw_html() as story_html:
                story_html.html = html

        assert story.raw_html().encoding == "UTF-8"
        assert story.raw_html().unicode is not None


class TestDiskStory:
    sample_rss = {
        "link": "https://hudsontoday.com/stories/641939920-rep-payne-jr-opposes-republican-budget-bill-to-benefit-the-wealthy-and-punish-the-middle-class",
        "title": "Rep. Payne, Jr. Opposes Republican Budget Bill to Benefit the Wealthy and Punish the Middle Class",
        "domain": "hudsontoday.com",
        "pub_date": "Sun, 30 Apr 2023 23:08:47 -0000",
        "fetch_date": "2023-05-01",
    }

    test_html = b"<html> <body> abracadabra </body> </html>"
    test_http_metadata = 200

    @pytest.fixture(scope="class", autouse=True)
    def set_env(self) -> None:
        os.environ["DATAROOT"] = TEST_DATA_DIR

    # We want this to be cmdline toggleable probably.
    @pytest.fixture(scope="class", autouse=True)
    def teardown_test_datadir(self, request: Any) -> Generator:
        yield
        shutil.rmtree(TEST_DATA_DIR)

    def test_write_disk_story(self) -> None:
        story: DiskStory = DiskStory()

        with story.rss_entry() as rss_entry:
            rss_entry.link = self.sample_rss["link"]
            rss_entry.title = self.sample_rss["title"]
            rss_entry.domain = self.sample_rss["domain"]
            rss_entry.pub_date = self.sample_rss["pub_date"]
            rss_entry.fetch_date = self.sample_rss["fetch_date"]

        rss_entry = story.rss_entry()
        assert rss_entry.link == self.sample_rss["link"]
        assert rss_entry.title == self.sample_rss["title"]
        assert rss_entry.domain == self.sample_rss["domain"]
        assert rss_entry.pub_date == self.sample_rss["pub_date"]

    def test_dump_story(self) -> None:
        story: DiskStory = DiskStory()
        with story.rss_entry() as rss_entry:
            rss_entry.link = self.sample_rss["link"]
            rss_entry.title = self.sample_rss["title"]
            rss_entry.domain = self.sample_rss["domain"]
            rss_entry.pub_date = self.sample_rss["pub_date"]
            rss_entry.fetch_date = self.sample_rss["fetch_date"]

        dumped: bytes = story.dump()

        new_story: DiskStory = DiskStory.load(dumped)

        rss_entry = new_story.rss_entry()
        assert rss_entry.link == self.sample_rss["link"]
        assert rss_entry.title == self.sample_rss["title"]
        assert rss_entry.domain == self.sample_rss["domain"]
        assert rss_entry.pub_date == self.sample_rss["pub_date"]

    def test_no_init_date(self) -> None:
        # This test fails because diskstory requires a 'fetch_date' to save
        with pytest.raises(RuntimeError):
            story: DiskStory = DiskStory()
            with story.rss_entry() as rss_entry:
                rss_entry.link = self.sample_rss["link"]
                rss_entry.title = self.sample_rss["title"]
                rss_entry.domain = self.sample_rss["domain"]
                rss_entry.pub_date = self.sample_rss["pub_date"]
            rss_entry = story.rss_entry()

    def test_no_rss(self) -> None:
        with pytest.raises(RuntimeError):
            story: DiskStory = DiskStory()
            with story.raw_html() as raw_html:
                raw_html.html = self.test_html

    def test_multiple_fields(self) -> None:
        story: DiskStory = DiskStory()
        with story.rss_entry() as rss_entry:
            rss_entry.link = self.sample_rss["link"]
            rss_entry.title = self.sample_rss["title"]
            rss_entry.domain = self.sample_rss["domain"]
            rss_entry.pub_date = self.sample_rss["pub_date"]
            rss_entry.fetch_date = self.sample_rss["fetch_date"]

        dumped: bytes = story.dump()

        new_story: DiskStory = DiskStory.load(dumped)
        with new_story.raw_html() as raw_html:
            raw_html.html = self.test_html

        with new_story.http_metadata() as http_metadata:
            http_metadata.response_code = self.test_http_metadata

        dumped_again: bytes = new_story.dump()

        third_story: DiskStory = DiskStory.load(dumped_again)
        rss_entry = third_story.rss_entry()
        assert rss_entry.link == self.sample_rss["link"]
        raw_html = third_story.raw_html()
        assert raw_html.html == self.test_html
        http_meta = third_story.http_metadata()
        assert http_meta.response_code == self.test_http_metadata


class TestStoryFactory:
    sample_rss = {
        "link": "https://hudsontoday.com/stories/641939920-rep-payne-jr-opposes-republican-budget-bill-to-benefit-the-wealthy-and-punish-the-middle-class",
        "title": "Rep. Payne, Jr. Opposes Republican Budget Bill to Benefit the Wealthy and Punish the Middle Class",
        "domain": "hudsontoday.com",
        "pub_date": "Sun, 30 Apr 2023 23:08:47 -0000",
        "fetch_date": "2023-05-01",
    }

    def test_story_factory(self) -> None:
        STORY_IFACE = "STORY_FACTORY"
        pre_environ = None
        if STORY_IFACE in os.environ:
            pre_environ = os.environ[STORY_IFACE]
            del os.environ[STORY_IFACE]

        Story = StoryFactory()

        story: BaseStory = Story()
        assert isinstance(story, BaseStory)
        assert not isinstance(story, DiskStory)

        os.environ[STORY_IFACE] = "DiskStory"

        Story = StoryFactory()
        story1: BaseStory = Story()
        assert isinstance(story1, DiskStory)

        if pre_environ is not None:
            del os.environ[STORY_IFACE]

    def test_story_factory_load(self) -> None:
        Story = StoryFactory()

        story: BaseStory = Story()
        with story.rss_entry() as rss_entry:
            rss_entry.link = self.sample_rss["link"]
            rss_entry.title = self.sample_rss["title"]
            rss_entry.domain = self.sample_rss["domain"]
            rss_entry.pub_date = self.sample_rss["pub_date"]
            rss_entry.fetch_date = self.sample_rss["fetch_date"]

        dumped: bytes = story.dump()
        new_story: BaseStory = Story.load(dumped)

        rss_entry = new_story.rss_entry()
        assert rss_entry.link == self.sample_rss["link"]
        assert rss_entry.title == self.sample_rss["title"]
        assert rss_entry.domain == self.sample_rss["domain"]
        assert rss_entry.pub_date == self.sample_rss["pub_date"]
