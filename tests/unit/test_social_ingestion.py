"""Unit tests for social media ingestion pipeline.

Tests that the social media fetch functions (HN, Reddit, Bluesky, Mastodon, Twitter/X)
produce correctly normalized article dicts with source_type='social'.
"""
import asyncio
import os
import sys
import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from ingestion import (
    parse_hn_story,
    parse_reddit_post,
    fetch_hn_top_stories,
    fetch_reddit_posts,
    fetch_bluesky_posts,
    fetch_mastodon_feeds,
    fetch_twitter_rss,
    fetch_tiktok_trending,
    fetch_instagram_posts,
    parse_rss_feed,
    strip_tracking_params,
    clean_html,
    IngestionRunner,
)


# --- Hacker News Tests ---

class TestHackerNews:
    def test_parse_hn_story_basic(self):
        item = {
            "id": 12345,
            "title": "Fed Raises Interest Rates by 0.25%",
            "url": "https://reuters.com/article/fed-rates",
            "text": "",
            "score": 500,
            "descendants": 200,
            "by": "journalist",
            "time": 1700000000,
            "type": "story",
        }
        result = parse_hn_story(item)
        assert result["title"] == "Fed Raises Interest Rates by 0.25%"
        assert result["publisher"] == "Hacker News"
        assert result["source_type"] == "social"
        assert result["social_coverage"] == 700  # score + descendants
        assert result["author"] == "journalist"
        assert result["url"] == "https://reuters.com/article/fed-rates"

    def test_parse_hn_story_no_url_uses_hn_link(self):
        item = {
            "id": 99999,
            "title": "Ask HN: What's happening with markets?",
            "url": "",
            "text": "Discussion about market volatility",
            "score": 100,
            "descendants": 50,
            "by": "asker",
            "time": 1700000000,
            "type": "story",
        }
        result = parse_hn_story(item)
        assert "news.ycombinator.com/item?id=99999" in result["url"]
        assert "Discussion about market volatility" in result["text"]

    def test_parse_hn_story_strips_tracking_params(self):
        item = {
            "id": 1,
            "title": "Test",
            "url": "https://example.com/article?utm_source=hn&utm_medium=social&real_param=1",
            "text": "",
            "score": 10,
            "descendants": 5,
            "by": "user",
            "time": 1700000000,
            "type": "story",
        }
        result = parse_hn_story(item)
        assert "utm_source" not in result["url"]
        assert "utm_medium" not in result["url"]
        assert "real_param=1" in result["url"]

    @pytest.mark.asyncio
    async def test_fetch_hn_top_stories_success(self):
        mock_stories = [1, 2, 3]
        mock_items = [
            {"id": 1, "title": "Story 1", "url": "https://example.com/1", "score": 100,
             "descendants": 50, "by": "user1", "time": 1700000000, "type": "story"},
            {"id": 2, "title": "Story 2", "url": "https://example.com/2", "score": 200,
             "descendants": 80, "by": "user2", "time": 1700000001, "type": "story"},
            {"id": 3, "title": "", "url": "", "score": 0, "descendants": 0,
             "by": "", "time": 1700000002, "type": "story"},
        ]

        async def mock_get(url, **kwargs):
            resp = MagicMock()
            resp.status_code = 200
            if "topstories" in url:
                resp.json = lambda: mock_stories
            else:
                item_id = int(url.split("/")[-1].replace(".json", ""))
                resp.json = lambda iid=item_id: mock_items[iid - 1]
            return resp

        with patch("ingestion.httpx.AsyncClient") as mock_client_cls:
            mock_client = AsyncMock()
            mock_client.get = mock_get
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=False)
            mock_client_cls.return_value = mock_client

            articles = await fetch_hn_top_stories(limit=3)
            # Should get 2 articles (3rd has empty title)
            assert len(articles) == 2
            assert all(a["source_type"] == "social" for a in articles)
            assert all(a["publisher"] == "Hacker News" for a in articles)

    @pytest.mark.asyncio
    async def test_fetch_hn_handles_api_error(self):
        async def mock_get(url, **kwargs):
            resp = MagicMock()
            resp.status_code = 503
            return resp

        with patch("ingestion.httpx.AsyncClient") as mock_client_cls:
            mock_client = AsyncMock()
            mock_client.get = mock_get
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=False)
            mock_client_cls.return_value = mock_client

            articles = await fetch_hn_top_stories()
            assert articles == []


# --- Reddit Tests ---

class TestReddit:
    def test_parse_reddit_post_basic(self):
        post = {
            "title": "S&P 500 hits all-time high",
            "url": "https://www.reddit.com/r/stocks/comments/abc123",
            "selftext": "The S&P 500 reached a new record today...",
            "score": 5000,
            "num_comments": 300,
            "author": "trader123",
            "created_utc": 1700000000,
            "permalink": "/r/stocks/comments/abc123/sp500_hits_ath",
            "subreddit": "stocks",
        }
        result = parse_reddit_post(post)
        assert result["title"] == "S&P 500 hits all-time high"
        assert result["publisher"] == "Reddit/r/stocks"
        assert result["source_type"] == "social"
        assert result["social_coverage"] == 5300  # score + num_comments
        assert result["author"] == "u/trader123"

    def test_parse_reddit_post_external_url(self):
        post = {
            "title": "Breaking: Fed announcement",
            "url": "https://reuters.com/fed-announcement?utm_source=reddit",
            "selftext": "",
            "score": 1000,
            "num_comments": 100,
            "author": "news_bot",
            "created_utc": 1700000000,
            "permalink": "/r/news/comments/xyz",
            "subreddit": "news",
        }
        result = parse_reddit_post(post)
        # External URL should be preserved and tracking stripped
        assert "reuters.com" in result["url"]
        assert "utm_source" not in result["url"]

    @pytest.mark.asyncio
    async def test_fetch_reddit_posts_success(self):
        mock_response_data = {
            "data": {
                "children": [
                    {
                        "data": {
                            "title": "Market Update",
                            "url": "https://www.reddit.com/r/finance/comments/abc",
                            "selftext": "Big market moves today",
                            "score": 500,
                            "num_comments": 100,
                            "author": "analyst",
                            "created_utc": 1700000000,
                            "permalink": "/r/finance/comments/abc",
                            "subreddit": "finance",
                            "stickied": False,
                        }
                    },
                    {
                        "data": {
                            "title": "Stickied post",
                            "stickied": True,
                            "score": 0,
                        }
                    },
                ]
            }
        }

        async def mock_get(url, **kwargs):
            resp = MagicMock()
            resp.status_code = 200
            resp.json = lambda: mock_response_data
            return resp

        with patch("ingestion.httpx.AsyncClient") as mock_client_cls:
            mock_client = AsyncMock()
            mock_client.get = mock_get
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=False)
            mock_client_cls.return_value = mock_client

            articles = await fetch_reddit_posts(subreddits=["finance"])
            assert len(articles) == 1  # stickied excluded
            assert articles[0]["source_type"] == "social"
            assert articles[0]["publisher"] == "Reddit/r/finance"


# --- Bluesky Tests ---

class TestBluesky:
    @pytest.mark.asyncio
    async def test_fetch_bluesky_posts_success(self):
        mock_response = {
            "posts": [
                {
                    "uri": "at://did:plc:abc123/app.bsky.feed.post/rkey1",
                    "author": {
                        "handle": "journalist.bsky.social",
                        "displayName": "Jane Journalist",
                    },
                    "record": {
                        "text": "Breaking: Major earnings miss for tech giant. Stock down 8% after hours. Full analysis thread below.",
                        "createdAt": "2024-01-15T10:30:00Z",
                    },
                    "likeCount": 150,
                    "replyCount": 45,
                    "repostCount": 80,
                },
                {
                    "uri": "at://did:plc:xyz/app.bsky.feed.post/rkey2",
                    "author": {"handle": "short.bsky", "displayName": "Short"},
                    "record": {"text": "hi", "createdAt": "2024-01-15T10:00:00Z"},
                    "likeCount": 1,
                    "replyCount": 0,
                    "repostCount": 0,
                },
            ]
        }

        async def mock_get(url, **kwargs):
            resp = MagicMock()
            resp.status_code = 200
            resp.json = lambda: mock_response
            return resp

        with patch("ingestion.httpx.AsyncClient") as mock_client_cls:
            mock_client = AsyncMock()
            mock_client.get = mock_get
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=False)
            mock_client_cls.return_value = mock_client

            articles = await fetch_bluesky_posts(search_terms=["breaking news"])
            assert len(articles) == 1  # second post too short (<20 chars)
            art = articles[0]
            assert art["source_type"] == "social"
            assert art["publisher"] == "Bluesky"
            assert art["social_coverage"] == 275  # 150 + 45 + 80
            assert "bsky.app/profile/journalist.bsky.social/post/rkey1" in art["url"]

    @pytest.mark.asyncio
    async def test_fetch_bluesky_handles_api_error(self):
        async def mock_get(url, **kwargs):
            resp = MagicMock()
            resp.status_code = 500
            return resp

        with patch("ingestion.httpx.AsyncClient") as mock_client_cls:
            mock_client = AsyncMock()
            mock_client.get = mock_get
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=False)
            mock_client_cls.return_value = mock_client

            articles = await fetch_bluesky_posts(search_terms=["test"])
            assert articles == []


# --- Mastodon Tests ---

class TestMastodon:
    @pytest.mark.asyncio
    async def test_fetch_mastodon_feeds_success(self):
        mock_rss = """<?xml version="1.0" encoding="UTF-8"?>
        <rss version="2.0">
            <channel>
                <title>Reuters on Mastodon</title>
                <item>
                    <title>Global markets rally on trade deal</title>
                    <link>https://mastodon.social/@reuters/123</link>
                    <description>Markets surged worldwide as new trade agreement reached.</description>
                    <pubDate>Mon, 15 Jan 2024 10:00:00 GMT</pubDate>
                </item>
            </channel>
        </rss>"""

        async def mock_get(url, **kwargs):
            resp = MagicMock()
            resp.status_code = 200
            resp.text = mock_rss
            return resp

        with patch("ingestion.httpx.AsyncClient") as mock_client_cls:
            mock_client = AsyncMock()
            mock_client.get = mock_get
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=False)
            mock_client_cls.return_value = mock_client

            with patch("ingestion.MASTODON_FEEDS", ["https://mastodon.social/@reuters.rss"]):
                articles = await fetch_mastodon_feeds()
                assert len(articles) >= 1
                assert all(a["source_type"] == "social" for a in articles)


# --- Twitter/X (via Nitter RSS) Tests ---

class TestTwitter:
    @pytest.mark.asyncio
    async def test_fetch_twitter_rss_success(self):
        mock_rss = """<?xml version="1.0" encoding="UTF-8"?>
        <rss version="2.0" xmlns:dc="http://purl.org/dc/elements/1.1/">
            <channel>
                <title>@ReutersWorld / Reuters World</title>
                <item>
                    <title>BREAKING: EU announces new sanctions package targeting energy sector</title>
                    <link>https://nitter.net/ReutersWorld/status/123456</link>
                    <description>EU announces new sanctions package targeting energy sector. Full details expected this afternoon.</description>
                    <pubDate>Mon, 15 Jan 2024 14:30:00 GMT</pubDate>
                    <dc:creator>@ReutersWorld</dc:creator>
                </item>
            </channel>
        </rss>"""

        async def mock_get(url, **kwargs):
            resp = MagicMock()
            if "nitter" in url:
                resp.status_code = 200
                resp.text = mock_rss
            else:
                resp.status_code = 404
                resp.text = ""
            return resp

        with patch("ingestion.httpx.AsyncClient") as mock_client_cls:
            mock_client = AsyncMock()
            mock_client.get = mock_get
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=False)
            mock_client_cls.return_value = mock_client

            articles = await fetch_twitter_rss(accounts=["ReutersWorld"])
            assert len(articles) >= 1
            art = articles[0]
            assert art["source_type"] == "social"
            assert art["publisher"].startswith("Twitter/X")


# --- IngestionRunner Social Loop Tests ---

class TestIngestionRunnerSocial:
    @pytest.mark.asyncio
    async def test_poll_social_loop_calls_all_sources(self):
        runner = IngestionRunner()
        received = []

        async def on_article(article):
            received.append(article)

        runner.on_new_article = on_article
        runner._running = True

        call_count = 0

        async def fake_sleep(duration):
            nonlocal call_count
            call_count += 1
            runner._running = False  # Stop after first iteration

        with patch("ingestion.fetch_hn_top_stories", new_callable=AsyncMock) as mock_hn, \
             patch("ingestion.fetch_reddit_posts", new_callable=AsyncMock) as mock_reddit, \
             patch("ingestion.fetch_bluesky_posts", new_callable=AsyncMock) as mock_bsky, \
             patch("ingestion.fetch_mastodon_feeds", new_callable=AsyncMock) as mock_mastodon, \
             patch("ingestion.fetch_twitter_rss", new_callable=AsyncMock) as mock_twitter, \
             patch("asyncio.sleep", side_effect=fake_sleep):

            mock_hn.return_value = [
                {"title": "HN Story", "url": "https://hn.com/1", "text": "HN",
                 "publisher": "Hacker News", "source_type": "social", "social_coverage": 100,
                 "author": "u", "timestamp": 1700000000}
            ]
            mock_reddit.return_value = [
                {"title": "Reddit Post", "url": "https://reddit.com/1", "text": "Reddit",
                 "publisher": "Reddit/r/news", "source_type": "social", "social_coverage": 200,
                 "author": "u", "timestamp": 1700000000}
            ]
            mock_bsky.return_value = [
                {"title": "Bsky Post", "url": "https://bsky.app/1", "text": "Bluesky post",
                 "publisher": "Bluesky", "source_type": "social", "social_coverage": 50,
                 "author": "@u", "timestamp": 1700000000}
            ]
            mock_mastodon.return_value = [
                {"title": "Mastodon Post", "url": "https://mastodon.social/1", "text": "Toot",
                 "publisher": "Mastodon", "source_type": "social", "social_coverage": 0,
                 "author": "m", "timestamp": 1700000000}
            ]
            mock_twitter.return_value = [
                {"title": "Tweet", "url": "https://x.com/1", "text": "Tweet text",
                 "publisher": "Twitter/X/@reuters", "source_type": "social", "social_coverage": 300,
                 "author": "@reuters", "timestamp": 1700000000}
            ]

            await runner._poll_social_loop()

            # All social sources should have been called
            mock_hn.assert_called_once()
            mock_reddit.assert_called_once()
            mock_bsky.assert_called_once()
            mock_mastodon.assert_called_once()
            mock_twitter.assert_called_once()

            # Articles should have been passed to callback
            assert len(received) == 5
            publishers = {a["publisher"] for a in received}
            assert "Hacker News" in publishers
            assert "Reddit/r/news" in publishers
            assert "Bluesky" in publishers
            assert "Mastodon" in publishers
            assert "Twitter/X/@reuters" in publishers


# --- Social Source Normalization Tests ---

class TestSocialNormalization:
    """Verify all social sources produce correctly normalized article dicts."""

    def _validate_article(self, article):
        """Common validation for social-sourced articles."""
        assert article["source_type"] == "social"
        assert isinstance(article.get("title"), str) and article["title"]
        assert isinstance(article.get("publisher"), str) and article["publisher"]
        assert isinstance(article.get("timestamp"), (int, float))
        assert isinstance(article.get("social_coverage"), (int, float))
        assert isinstance(article.get("text"), str)
        assert isinstance(article.get("author"), str)

    def test_hn_normalization(self):
        item = {"id": 1, "title": "Test", "url": "https://example.com",
                "text": "", "score": 50, "descendants": 10, "by": "user", "time": 1700000000, "type": "story"}
        self._validate_article(parse_hn_story(item))

    def test_reddit_normalization(self):
        post = {"title": "Test", "url": "https://reddit.com/r/test", "selftext": "body",
                "score": 50, "num_comments": 10, "author": "user", "created_utc": 1700000000,
                "permalink": "/r/test/1", "subreddit": "test"}
        self._validate_article(parse_reddit_post(post))


# --- Verify social source config ---

class TestSocialSourceConfig:
    """Verify social source configuration is complete."""

    def test_all_social_platforms_have_implementations(self):
        """Check that fetch functions exist for all social platforms."""
        from ingestion import (
            fetch_hn_top_stories,
            fetch_reddit_posts,
            fetch_bluesky_posts,
            fetch_mastodon_feeds,
            fetch_twitter_rss,
        )
        assert callable(fetch_hn_top_stories)
        assert callable(fetch_reddit_posts)
        assert callable(fetch_bluesky_posts)
        assert callable(fetch_mastodon_feeds)
        assert callable(fetch_twitter_rss)

    def test_twitter_accounts_configured(self):
        from ingestion import TWITTER_ACCOUNTS, NITTER_INSTANCES
        assert len(TWITTER_ACCOUNTS) >= 5, "Should have at least 5 Twitter accounts"
        assert len(NITTER_INSTANCES) >= 1, "Should have at least 1 Nitter instance"

    def test_subreddits_configured(self):
        from ingestion import DEFAULT_SUBREDDITS
        assert len(DEFAULT_SUBREDDITS) >= 10, "Should have at least 10 subreddits"

    def test_bluesky_search_terms_configured(self):
        from ingestion import BLUESKY_SEARCH_TERMS
        assert len(BLUESKY_SEARCH_TERMS) >= 5, "Should have at least 5 search terms"

    def test_mastodon_feeds_configured(self):
        from ingestion import MASTODON_FEEDS
        assert len(MASTODON_FEEDS) >= 1, "Should have at least 1 Mastodon feed"

    def test_tiktok_and_instagram_fetch_functions_exist(self):
        assert callable(fetch_tiktok_trending)
        assert callable(fetch_instagram_posts)


# --- TikTok Tests ---

class TestTikTok:
    @pytest.mark.asyncio
    async def test_fetch_tiktok_trending_success(self):
        mock_rss = """<?xml version="1.0" encoding="UTF-8"?>
        <rss version="2.0">
            <channel>
                <title>TikTok Trending</title>
                <item>
                    <title>Viral finance tip about index funds</title>
                    <link>https://www.tiktok.com/@user/video/123</link>
                    <description>Index funds outperform active management 90% of the time.</description>
                    <pubDate>Mon, 15 Jan 2024 10:00:00 GMT</pubDate>
                </item>
                <item>
                    <title>Market crash prediction goes viral</title>
                    <link>https://www.tiktok.com/@analyst/video/456</link>
                    <description>Technical analysis shows bearish patterns forming.</description>
                    <pubDate>Mon, 15 Jan 2024 09:00:00 GMT</pubDate>
                </item>
            </channel>
        </rss>"""

        async def mock_get(url, **kwargs):
            resp = MagicMock()
            resp.status_code = 200
            resp.text = mock_rss
            return resp

        with patch("ingestion.httpx.AsyncClient") as mock_client_cls:
            mock_client = AsyncMock()
            mock_client.get = mock_get
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=False)
            mock_client_cls.return_value = mock_client

            articles = await fetch_tiktok_trending(limit=5)
            assert len(articles) >= 1
            assert all(a["source_type"] == "social" for a in articles)
            assert all(a["publisher"] == "TikTok" for a in articles)

    @pytest.mark.asyncio
    async def test_fetch_tiktok_handles_error(self):
        async def mock_get(url, **kwargs):
            resp = MagicMock()
            resp.status_code = 503
            return resp

        with patch("ingestion.httpx.AsyncClient") as mock_client_cls:
            mock_client = AsyncMock()
            mock_client.get = mock_get
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=False)
            mock_client_cls.return_value = mock_client

            articles = await fetch_tiktok_trending()
            assert articles == []


# --- Instagram Tests ---

class TestInstagram:
    @pytest.mark.asyncio
    async def test_fetch_instagram_posts_success(self):
        mock_rss = """<?xml version="1.0" encoding="UTF-8"?>
        <rss version="2.0">
            <channel>
                <title>Instagram Explore</title>
                <item>
                    <title>Breaking: CEO resignation announcement</title>
                    <link>https://www.instagram.com/p/abc123</link>
                    <description>Major tech CEO stepping down effective immediately.</description>
                    <pubDate>Mon, 15 Jan 2024 12:00:00 GMT</pubDate>
                </item>
            </channel>
        </rss>"""

        async def mock_get(url, **kwargs):
            resp = MagicMock()
            resp.status_code = 200
            resp.text = mock_rss
            return resp

        with patch("ingestion.httpx.AsyncClient") as mock_client_cls:
            mock_client = AsyncMock()
            mock_client.get = mock_get
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=False)
            mock_client_cls.return_value = mock_client

            articles = await fetch_instagram_posts(limit=5)
            assert len(articles) >= 1
            assert all(a["source_type"] == "social" for a in articles)
            assert all(a["publisher"] == "Instagram" for a in articles)

    @pytest.mark.asyncio
    async def test_fetch_instagram_handles_error(self):
        async def mock_get(url, **kwargs):
            resp = MagicMock()
            resp.status_code = 500
            return resp

        with patch("ingestion.httpx.AsyncClient") as mock_client_cls:
            mock_client = AsyncMock()
            mock_client.get = mock_get
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=False)
            mock_client_cls.return_value = mock_client

            articles = await fetch_instagram_posts()
            assert articles == []


# --- Full Social Loop with TikTok and Instagram ---

class TestIngestionRunnerSocialComplete:
    @pytest.mark.asyncio
    async def test_poll_social_loop_calls_tiktok_and_instagram(self):
        """Verify the social loop also calls TikTok and Instagram fetchers."""
        runner = IngestionRunner()
        received = []

        async def on_article(article):
            received.append(article)

        runner.on_new_article = on_article
        runner._running = True

        async def fake_sleep(duration):
            runner._running = False

        with patch("ingestion.fetch_hn_top_stories", new_callable=AsyncMock) as mock_hn, \
             patch("ingestion.fetch_reddit_posts", new_callable=AsyncMock) as mock_reddit, \
             patch("ingestion.fetch_bluesky_posts", new_callable=AsyncMock) as mock_bsky, \
             patch("ingestion.fetch_mastodon_feeds", new_callable=AsyncMock) as mock_mastodon, \
             patch("ingestion.fetch_twitter_rss", new_callable=AsyncMock) as mock_twitter, \
             patch("ingestion.fetch_tiktok_trending", new_callable=AsyncMock) as mock_tiktok, \
             patch("ingestion.fetch_instagram_posts", new_callable=AsyncMock) as mock_instagram, \
             patch("asyncio.sleep", side_effect=fake_sleep):

            mock_hn.return_value = []
            mock_reddit.return_value = []
            mock_bsky.return_value = []
            mock_mastodon.return_value = []
            mock_twitter.return_value = []
            mock_tiktok.return_value = [
                {"title": "TikTok Trend", "url": "https://tiktok.com/1", "text": "Trending",
                 "publisher": "TikTok", "source_type": "social", "social_coverage": 10000,
                 "author": "@creator", "timestamp": 1700000000}
            ]
            mock_instagram.return_value = [
                {"title": "IG Post", "url": "https://instagram.com/p/1", "text": "Breaking news",
                 "publisher": "Instagram", "source_type": "social", "social_coverage": 5000,
                 "author": "@news", "timestamp": 1700000000}
            ]

            await runner._poll_social_loop()

            mock_tiktok.assert_called_once()
            mock_instagram.assert_called_once()
            assert len(received) == 2
            publishers = {a["publisher"] for a in received}
            assert "TikTok" in publishers
            assert "Instagram" in publishers
