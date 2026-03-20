"""Ingestion service for News Monkey.

Handles multiple ingestion pipelines running as concurrent background tasks:
  - RSS feed polling (78+ feeds from major publishers)
  - Article scraping (readability-style full-text extraction from URLs)
  - Prediction market integration:
      * Polymarket API (trending markets, volume tracking, unusual bets)
      * CallSheet API (prediction markets from callsheet.com)
  - Social media ingestion:
      * Hacker News (top stories via Firebase API)
      * Reddit (18+ subreddits via JSON API)
      * Bluesky (public search API)
      * Mastodon (RSS feeds from news accounts)
      * Twitter/X (via Nitter RSS bridge)
      * TikTok (trending content via RSSHub bridge)
      * Instagram (explore content via RSSHub bridge)
  - NewsAPI integration (optional, when NEWSAPI_KEY is set)

All sources are normalized to a common article format and processed through
deduplication, fact extraction, and clustering pipelines.
"""
import asyncio
import hashlib
import html
import logging
import os
import re
import time
import defusedxml.ElementTree as ET
from typing import Optional
from urllib.parse import urlparse, urlencode, parse_qs, urlunparse

import httpx

logger = logging.getLogger(__name__)

# Minimum word count to consider RSS description as "full text"
MIN_FULL_TEXT_WORDS = 100


def _to_str(val) -> str:
    """Coerce a value to string — handles lists by joining."""
    if isinstance(val, list):
        return "; ".join(str(v) for v in val)
    return str(val) if val else ""

# --- Configuration ---
RSS_POLL_INTERVAL = int(os.environ.get("RSS_POLL_INTERVAL", "300"))  # 5 min default
POLYMARKET_POLL_INTERVAL = int(os.environ.get("POLYMARKET_POLL_INTERVAL", "600"))  # 10 min default
SOCIAL_POLL_INTERVAL = int(os.environ.get("SOCIAL_POLL_INTERVAL", "300"))  # 5 min default
POLYMARKET_API_URL = os.environ.get("POLYMARKET_API_URL", "https://gamma-api.polymarket.com")
CALLSHEET_API_URL = os.environ.get("CALLSHEET_API_URL", "https://callsheet.com/api/v1")
CALLSHEET_POLL_INTERVAL = int(os.environ.get("CALLSHEET_POLL_INTERVAL", "600"))  # 10 min default
KALSHI_API_URL = os.environ.get("KALSHI_API_URL", "https://trading-api.kalshi.com/trade-api/v2")
KALSHI_API_KEY = os.environ.get("KALSHI_API_KEY", "")
# Kalshi polls on the same interval as Polymarket (POLYMARKET_POLL_INTERVAL)
HN_API_URL = "https://hacker-news.firebaseio.com/v0"

# TikTok and Instagram via RSSHub bridge
TIKTOK_RSS_URL = os.environ.get("TIKTOK_RSS_URL", "https://rsshub.app/tiktok/trend/en")
INSTAGRAM_RSS_URL = os.environ.get("INSTAGRAM_RSS_URL", "https://rsshub.app/instagram/explore")

# Reddit subreddits to track (configurable via env)
DEFAULT_SUBREDDITS = [
    "worldnews", "news", "economics", "technology", "finance",
    "investing", "stocks", "wallstreetbets", "geopolitics", "energy",
    "business", "cryptocurrency", "economy", "MachineLearning",
    "artificial", "tech", "neutralnews", "TrueReddit",
]

# Mastodon instances/accounts for social tracking
MASTODON_FEEDS = [
    "https://mastodon.social/@reuters.rss",
    "https://mastodon.social/@baborabek.rss",
    "https://mastodon.social/@nytimes.rss",
]

# Bluesky public search terms for news tracking
BLUESKY_SEARCH_TERMS = [
    "breaking news", "market crash", "federal reserve", "earnings report",
    "stock market", "crypto crash", "interest rates", "inflation report",
    "AI regulation", "tech layoffs",
]

# Twitter/X accounts to track via Nitter RSS (public, no auth required)
# Nitter instances provide RSS feeds for Twitter/X accounts
NITTER_INSTANCES = [
    "https://nitter.privacydev.net",
    "https://nitter.poast.org",
    "https://nitter.net",
]
TWITTER_ACCOUNTS = [
    "Reuters", "AP", "BBCBreaking", "CNBCnow", "markets",
    "business", "WSJ", "FT", "BloombergTV", "ReutersBiz",
    "FinancialTimes", "federalreserve",
    "SECGov", "WhiteHouse", "NYTBusiness", "TheEconomist",
]

# Default RSS feeds — configurable via environment
DEFAULT_FEEDS = [
    # Major wire services / broadsheets
    "https://feeds.reuters.com/reuters/topNews",
    "https://feeds.reuters.com/reuters/businessNews",
    "https://feeds.reuters.com/reuters/technologyNews",
    "https://feeds.reuters.com/reuters/worldNews",
    "https://feeds.bbci.co.uk/news/rss.xml",
    "https://feeds.bbci.co.uk/news/business/rss.xml",
    "https://feeds.bbci.co.uk/news/technology/rss.xml",
    "https://feeds.bbci.co.uk/news/world/rss.xml",
    "https://rss.nytimes.com/services/xml/rss/nyt/HomePage.xml",
    "https://rss.nytimes.com/services/xml/rss/nyt/Business.xml",
    "https://rss.nytimes.com/services/xml/rss/nyt/Technology.xml",
    "https://feeds.npr.org/1001/rss.xml",
    "https://feeds.npr.org/1006/rss.xml",  # NPR Business
    "https://www.theguardian.com/world/rss",
    "https://www.theguardian.com/business/rss",
    "https://www.theguardian.com/technology/rss",
    # Wire services
    "https://rsshub.app/apnews/topics/apf-topnews",
    "https://rsshub.app/apnews/topics/apf-business",
    # Financial / market-focused
    "https://feeds.bloomberg.com/markets/news.rss",
    "https://feeds.marketwatch.com/marketwatch/topstories/",
    "https://feeds.marketwatch.com/marketwatch/marketpulse/",
    "https://www.cnbc.com/id/100003114/device/rss/rss.html",
    "https://www.cnbc.com/id/10001147/device/rss/rss.html",  # CNBC Markets
    "https://seekingalpha.com/market_currents.xml",
    "https://finance.yahoo.com/news/rssindex",
    "https://www.investing.com/rss/news.rss",
    "https://rsshub.app/apnews/topics/apf-technology",
    "https://www.ft.com/?format=rss",
    # Crypto / fintech
    "https://cointelegraph.com/rss",
    "https://www.coindesk.com/arc/outboundfeeds/rss/",
    "https://decrypt.co/feed",
    # Technology
    "https://feeds.arstechnica.com/arstechnica/index",
    "https://www.wired.com/feed/rss",
    "https://techcrunch.com/feed/",
    "https://www.theverge.com/rss/index.xml",
    "https://www.engadget.com/rss.xml",
    "https://www.zdnet.com/news/rss.xml",
    "https://feeds.feedburner.com/TheHackersNews",
    "https://www.technologyreview.com/feed/",
    # Geopolitics / world affairs
    "https://rss.nytimes.com/services/xml/rss/nyt/World.xml",
    "https://feeds.washingtonpost.com/rss/world",
    "https://feeds.washingtonpost.com/rss/business",
    "https://www.aljazeera.com/xml/rss/all.xml",
    "https://rss.dw.com/rdf/rss-en-all",
    "https://www.france24.com/en/rss",
    "https://feeds.skynews.com/feeds/rss/world.xml",
    # Economics / policy
    "https://www.economist.com/finance-and-economics/rss.xml",
    "https://www.economist.com/science-and-technology/rss.xml",
    "https://www.nakedcapitalism.com/feed",  # Naked Capitalism (canonical)
    "https://www.calculatedriskblog.com/feeds/posts/default?alt=rss",
    # Additional financial / market-focused
    "https://feeds.marketwatch.com/marketwatch/realtimeheadlines/",
    "https://www.barrons.com/feed",
    "https://fortune.com/feed/",
    "https://www.businessinsider.com/rss",
    "https://www.federalreserve.gov/feeds/press_all.xml",
    # Additional broadsheets / wire services
    "https://rss.nytimes.com/services/xml/rss/nyt/Politics.xml",
    "https://feeds.washingtonpost.com/rss/politics",
    "https://feeds.bbci.co.uk/news/science_and_environment/rss.xml",
    "https://feeds.nbcnews.com/nbcnews/public/news",
    "https://feeds.abcnews.com/abcnews/topstories",
    "https://feeds.cbsnews.com/CBSNewsMain",
    # Politico feed removed (returns 403)
    "https://thehill.com/feed/",
    # International
    "https://www.scmp.com/rss/91/feed",
    "https://www.japantimes.co.jp/feed/",
    "https://timesofindia.indiatimes.com/rssfeedstopstories.cms",
    "https://www.rt.com/rss/news/",
    # BBC Arabic and Al Arabiya feeds removed (return 403/404)
    "https://www.channelnewsasia.com/api/v1/rss-outbound-feed?_format=xml",
    # Additional financial / commodities / energy
    "https://oilprice.com/rss/main",
    "https://www.mining.com/feed/",
    # ZeroHedge feed removed (returns 404)
    "https://wolfstreet.com/feed/",
    # risk.net and institutionalinvestor.com feeds removed (return 404)
    # Central banks / policy
    # ECB feed removed (not valid RSS)
    "https://www.imf.org/en/News/Rss",
    "https://www.worldbank.org/en/news/rss.xml",
    # BIS feed removed (returns 404)
    # Science / health
    "https://www.nature.com/nature.rss",
    "https://www.sciencedaily.com/rss/all.xml",
    "https://www.statnews.com/feed/",
    # Additional tech / AI
    "https://openai.com/blog/rss.xml",
    "https://blog.google/rss/",
    # AnandTech feed removed (site defunct)
    "https://siliconangle.com/feed/",
    "https://venturebeat.com/feed/",
    # InfoWorld feed removed (returns 404)
    # Defense / security
    "https://www.defensenews.com/arc/outboundfeeds/rss/?outputType=xml",
    # Janes feed removed (returns 404)
    # Real estate / housing
    "https://www.housingwire.com/feed/",
    "https://therealdeal.com/feed/",
    # Substack — independent journalism and analysis
    "https://mattstoller.substack.com/feed",          # BIG by Matt Stoller (monopoly/antitrust)
    "https://www.thefp.com/feed",                      # The Free Press (Bari Weiss)
    "https://www.slowboring.com/feed",                 # Slow Boring (Matt Yglesias, policy)
    "https://www.platformer.news/feed",                # Platformer (Casey Newton, tech platforms)
    "https://www.construction-physics.com/feed",       # Construction Physics (Brian Potter)
    "https://www.noahpinion.blog/feed",                # Noahpinion (Noah Smith, economics)
    "https://www.astralcodexten.com/feed",             # Astral Codex Ten (Scott Alexander)
    "https://www.newcomer.co/feed",                    # Newcomer (Eric Newcomer, startups/VC)
    "https://www.lennysnewsletter.com/feed",           # Lenny's Newsletter (product/growth)
    "https://www.honest-broker.com/feed",               # The Honest Broker (Ted Gioia, culture/tech)
    "https://sinocism.com/feed",                       # Sinocism (Bill Bishop, China)
    # Carbon Brief feed removed (returns errors)
    "https://www.thediff.co/feed",                     # The Diff (Byrne Hobart, finance/tech)
]

# Tracking params to strip from URLs
TRACKING_PARAMS = {
    "utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content",
    "fbclid", "gclid", "ref", "source", "mc_cid", "mc_eid",
}


def get_configured_feeds() -> list[str]:
    """Get RSS feed URLs from environment or defaults."""
    env_feeds = os.environ.get("NEWS_MONKEY_FEEDS", "")
    if env_feeds:
        return [f.strip() for f in env_feeds.split(",") if f.strip()]
    return DEFAULT_FEEDS


# --- URL Cleaning ---

def strip_tracking_params(url: str) -> str:
    """Remove tracking parameters from a URL."""
    try:
        parsed = urlparse(url)
        params = parse_qs(parsed.query, keep_blank_values=False)
        cleaned = {k: v for k, v in params.items() if k.lower() not in TRACKING_PARAMS}
        new_query = urlencode(cleaned, doseq=True) if cleaned else ""
        return urlunparse(parsed._replace(query=new_query))
    except Exception:
        return url


# --- HTML Cleaning ---

def clean_html(raw_html: str) -> str:
    """Strip HTML tags and normalize whitespace to get clean body text."""
    if not raw_html:
        return ""
    # Remove script and style blocks
    text = re.sub(r'<(script|style)[^>]*>.*?</\1>', '', raw_html, flags=re.DOTALL | re.IGNORECASE)
    # Remove HTML tags
    text = re.sub(r'<[^>]+>', ' ', text)
    # Decode HTML entities
    text = html.unescape(text)
    # Normalize whitespace
    text = re.sub(r'\s+', ' ', text).strip()
    return text


def strip_boilerplate(text: str) -> str:
    """Remove newsletter chrome, social embeds, ads, and boilerplate from article text."""
    # Remove common newsletter/sharing patterns
    patterns = [
        r'Share this article.*?(?:\n|$)',
        r'Follow us on.*?(?:\n|$)',
        r'Subscribe to.*?(?:\n|$)',
        r'Sign up for.*?(?:\n|$)',
        r'Related articles?:.*?(?:\n|$)',
        r'Read more:.*?(?:\n|$)',
        r'Click here to.*?(?:\n|$)',
        r'Advertisement\s*',
        r'Sponsored content\s*',
        r'©\s*\d{4}.*?(?:\n|$)',
        r'All rights reserved.*?(?:\n|$)',
    ]
    for pattern in patterns:
        text = re.sub(pattern, '', text, flags=re.IGNORECASE)
    return text.strip()


# --- Article Body Scraping ---

async def scrape_article_body(url: str, client: Optional[httpx.AsyncClient] = None) -> Optional[str]:
    """Scrape full article text from a URL using readability-style extraction.

    Fetches the page HTML and extracts the main article content by looking
    for common article body selectors and paragraph density heuristics.
    Returns cleaned text or None on failure.
    """
    if not url:
        return None

    close_client = False
    if client is None:
        client = httpx.AsyncClient(
            headers={"User-Agent": "NewsMonkey/1.0 (news aggregator)"},
            follow_redirects=True,
        )
        close_client = True

    try:
        resp = await client.get(url, timeout=15)
        if resp.status_code != 200:
            logger.debug("Scrape %s returned status %d", url, resp.status_code)
            return None

        html_content = resp.text
        return extract_article_content(html_content)
    except Exception as e:
        logger.debug("Scrape error for %s: %s", url, e)
        return None
    finally:
        if close_client:
            await client.aclose()


def extract_article_content(html_content: str) -> Optional[str]:
    """Extract main article content from HTML using paragraph density heuristics.

    Implements a readability-style extraction:
    1. Remove script/style/nav/header/footer/aside elements
    2. Find the densest block of <p> tags (likely the article body)
    3. Clean and join paragraph text
    """
    if not html_content:
        return None

    # Remove non-content elements
    cleaned = re.sub(
        r'<(script|style|nav|header|footer|aside|iframe|noscript|svg|form)[^>]*>.*?</\1>',
        '', html_content, flags=re.DOTALL | re.IGNORECASE
    )

    # Extract all paragraph text
    paragraphs = re.findall(r'<p[^>]*>(.*?)</p>', cleaned, flags=re.DOTALL | re.IGNORECASE)

    if not paragraphs:
        # Fall back: try <article> tag content
        article_match = re.search(r'<article[^>]*>(.*?)</article>', cleaned, flags=re.DOTALL | re.IGNORECASE)
        if article_match:
            paragraphs = re.findall(r'<p[^>]*>(.*?)</p>', article_match.group(1), flags=re.DOTALL | re.IGNORECASE)

    if not paragraphs:
        return None

    # Clean each paragraph
    clean_paragraphs = []
    for p in paragraphs:
        text = clean_html(p)
        text = strip_boilerplate(text)
        # Skip very short paragraphs (likely captions, labels)
        if len(text.split()) >= 5:
            clean_paragraphs.append(text)

    if not clean_paragraphs:
        return None

    full_text = " ".join(clean_paragraphs)

    # Only return if we got meaningful content
    if len(full_text.split()) < 30:
        return None

    return full_text


# --- Named Entity Extraction (simple heuristic) ---

_ENTITY_STOPWORDS = {
    # Determiners/pronouns/conjunctions that appear capitalized at sentence starts
    "The", "This", "That", "These", "Those", "Here", "There", "When",
    "Where", "What", "Which", "Who", "How", "Why", "Its", "Their",
    "Some", "Many", "Most", "Each", "Every", "Both", "Such", "Other",
    "And", "But", "Or", "Not", "Nor", "For", "So", "If", "As", "At",
    "By", "To", "In", "On", "Of", "Up", "No", "An", "With",
    # Common verbs/adjectives often capitalized at sentence starts
    "Just", "Also", "Still", "Even", "More", "Less", "Much", "Very",
    "Only", "Already", "Again", "Yet", "Now", "Then", "Well", "About",
    "After", "Before", "Between", "During", "Since", "Until", "While",
    "Hold", "Apart", "According", "However", "Meanwhile", "Furthermore",
    "Moreover", "Nevertheless", "Although", "Despite", "Related", "High",
    "Low", "New", "Old", "Big", "Top", "Read", "Watch", "Click", "Sign",
    "View", "Share", "Open", "Close", "Free", "Live", "Breaking", "Update",
    "Report", "Source", "Sources", "Photo", "Video", "Image", "Audio",
    "Exclusive", "Opinion", "Analysis", "Editorial", "Repo", "Companies",
    "Workers", "People", "Markets", "Stocks", "Shares",
    # Adverbs/conjunctions that leak through at sentence boundaries
    "Eventually", "Importantly", "Therefore", "Because", "Elsewhere",
    "Considering", "Especially", "Additionally", "Consequently", "Regardless",
    "Apparently", "Presumably", "Certainly", "Essentially", "Generally",
    "Recently", "Previously", "Currently", "Initially", "Finally",
    "Says", "Said", "Gets", "Got", "Has", "Had", "Was", "Were", "Are",
    "Been", "Being", "Having", "Does", "Did", "Make", "Made", "Take",
    "Took", "Give", "Gave", "Come", "Came", "Goes", "Gone", "Went",
    # Emotive/descriptive words that aren't entities
    "Fears", "Hundreds", "Thousands", "Millions", "Billions",
    "Heavy", "Several", "Various", "Potential", "Possible", "Likely",
    "Unlikely", "Major", "Minor", "Significant", "Critical", "Key",
    "Really", "Quite", "Rather", "Fairly", "Nearly", "Almost",
    "Whats", "Heres", "Thats",
    # Days and months
    "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday",
    "January", "February", "March", "April", "May", "June",
    "July", "August", "September", "October", "November", "December",
    # Common verbs/words that appear in multi-word title-case fragments
    "See", "Will", "Hike", "Spike", "Hit", "Rise", "Fall", "Drop", "Jump",
    "Surge", "Slide", "Slip", "Gain", "Lose", "Cut", "Set", "Prices",
    "Rates", "Over", "Under", "Lower", "Higher", "Highest", "Lowest",
    "Trending", "Topics", "Searches", "Recent", "Latest",
    # Common abbreviations that aren't entities
    "RSS", "API", "URL", "HTTP", "HTTPS", "HTML", "CSS", "PDF", "FAQ",
    "EST", "PST", "GMT", "UTC", "ETF", "IPO", "CEO", "CFO", "COO",
}


def _is_title_case(text: str) -> bool:
    """Detect if text is title-cased (most words capitalized, typical of headlines)."""
    words = text.split()
    if len(words) < 3:
        return False
    cap_count = sum(1 for w in words if w[0:1].isupper())
    return cap_count / len(words) > 0.6


def extract_entities(text: str) -> list[str]:
    """Extract named entities using capitalization heuristics.

    Returns unique entity strings found in the text.
    Handles title-case headlines by only extracting known-pattern entities from them.
    """
    entities = set()

    # Split into sentences to detect title-cased lines (headlines)
    lines = text.split('\n')
    body_parts = []
    headline_parts = []
    for line in lines:
        line = line.strip()
        if not line:
            continue
        if _is_title_case(line):
            headline_parts.append(line)
        else:
            body_parts.append(line)

    # If the entire text looks like one title-cased line (no body), treat first
    # sentence as headline and rest as body
    if not body_parts and headline_parts:
        # For pure headline text, only extract abbreviations and known patterns
        for part in headline_parts:
            abbrevs = re.findall(r'\b([A-Z]{3,})\b', part)
            for a in abbrevs:
                if a not in _ENTITY_STOPWORDS:
                    entities.add(a)
        body_text = " ".join(headline_parts)
    else:
        body_text = " ".join(body_parts)
        # Extract abbreviations from headlines too
        for part in headline_parts:
            abbrevs = re.findall(r'\b([A-Z]{3,})\b', part)
            for a in abbrevs:
                if a not in _ENTITY_STOPWORDS:
                    entities.add(a)

    # Multi-word capitalized phrases from body text (e.g., "Federal Reserve", "New York")
    multi = re.findall(r'\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+)+)\b', body_text)
    # Single capitalized words that look like proper nouns (not sentence starters)
    singles = re.findall(r'(?<=[.!?]\s)([A-Z][a-z]{2,})', body_text)
    for phrase in multi:
        words = phrase.split()
        # Filter phrases with 4+ words (likely sentence fragments)
        if len(words) > 3:
            continue
        # Strip leading/trailing stopwords
        while words and words[0] in _ENTITY_STOPWORDS:
            words = words[1:]
        while words and words[-1] in _ENTITY_STOPWORDS:
            words = words[:-1]
        if not words:
            continue
        # Reject if majority of remaining words are stopwords
        non_stop = [w for w in words if w not in _ENTITY_STOPWORDS]
        if len(non_stop) < len(words) / 2:
            continue
        phrase = " ".join(words)
        if phrase:
            entities.add(phrase)
    for s in singles:
        if s not in _ENTITY_STOPWORDS:
            entities.add(s)
    # Also detect all-caps abbreviations (3+ chars) from body
    abbrevs = re.findall(r'\b([A-Z]{3,})\b', body_text)
    for a in abbrevs:
        if a not in _ENTITY_STOPWORDS:
            entities.add(a)
    return sorted(entities)[:20]  # Cap at 20 entities


# --- RSS Feed Parsing ---

def parse_rss_feed(xml_content: str) -> list[dict]:
    """Parse RSS/Atom feed XML into normalized article dicts.

    Returns list of {title, author, publisher, timestamp, url, text}.
    """
    articles = []
    try:
        root = ET.fromstring(xml_content)
    except ET.ParseError:
        logger.error("Failed to parse RSS feed XML")
        return articles

    # Detect Atom vs RSS
    ns = {"atom": "http://www.w3.org/2005/Atom"}

    # Try RSS 2.0
    channel = root.find("channel")
    if channel is not None:
        publisher = channel.findtext("title", "")
        for item in channel.findall("item"):
            title = item.findtext("title", "").strip()
            link = item.findtext("link", "").strip()
            description = item.findtext("description", "")
            author = item.findtext("author", "") or item.findtext("{http://purl.org/dc/elements/1.1/}creator", "")
            pub_date = item.findtext("pubDate", "")

            if not title:
                continue

            text = clean_html(description)
            text = strip_boilerplate(text)
            url = strip_tracking_params(link)

            articles.append({
                "title": title,
                "author": author.strip(),
                "publisher": publisher.strip(),
                "timestamp": _parse_rss_date(pub_date),
                "url": url,
                "text": text,
            })
        return articles

    # Try Atom
    feed_title = root.findtext("atom:title", "", ns) or root.findtext("title", "")
    entries = root.findall("atom:entry", ns) or root.findall("entry")
    for entry in entries:
        title = (entry.findtext("atom:title", "", ns) or entry.findtext("title", "")).strip()
        link_el = entry.find("atom:link", ns)
        if link_el is None:
            link_el = entry.find("link")
        link = link_el.get("href", "") if link_el is not None else ""
        summary = entry.findtext("atom:summary", "", ns) or entry.findtext("summary", "")
        content = entry.findtext("atom:content", "", ns) or entry.findtext("content", "")
        author_el = entry.find("atom:author", ns) or entry.find("author")
        author = ""
        if author_el is not None:
            author = (author_el.findtext("atom:name", "", ns) or author_el.findtext("name", "")).strip()
        updated = entry.findtext("atom:updated", "", ns) or entry.findtext("updated", "")

        if not title:
            continue

        text = clean_html(content or summary)
        text = strip_boilerplate(text)
        url = strip_tracking_params(link)

        articles.append({
            "title": title,
            "author": author,
            "publisher": feed_title.strip(),
            "timestamp": _parse_rss_date(updated),
            "url": url,
            "text": text,
        })

    return articles


def _parse_rss_date(date_str: str) -> float:
    """Parse RSS date string to unix timestamp. Returns current time on failure."""
    if not date_str:
        return time.time()
    # Try common RSS date formats
    import email.utils
    try:
        parsed = email.utils.parsedate_to_datetime(date_str)
        return parsed.timestamp()
    except Exception:
        pass
    # Try ISO 8601
    try:
        from datetime import datetime, timezone
        # Remove trailing Z and replace with +00:00
        clean = date_str.replace("Z", "+00:00")
        dt = datetime.fromisoformat(clean)
        return dt.timestamp()
    except Exception:
        pass
    return time.time()


async def fetch_feed(url: str, client: httpx.AsyncClient) -> list[dict]:
    """Fetch and parse a single RSS feed."""
    try:
        resp = await client.get(url, timeout=15, follow_redirects=True)
        if resp.status_code == 200:
            return parse_rss_feed(resp.text)
        logger.warning("Feed %s returned status %d", url, resp.status_code)
    except Exception as e:
        logger.error("Error fetching feed %s: %s", url, e)
    return []


async def poll_all_feeds() -> list[dict]:
    """Poll all configured RSS feeds and return normalized articles."""
    feeds = get_configured_feeds()
    all_articles = []
    async with httpx.AsyncClient() as client:
        tasks = [fetch_feed(url, client) for url in feeds]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for result in results:
            if isinstance(result, list):
                all_articles.extend(result)
            elif isinstance(result, Exception):
                logger.error("Feed polling error: %s", result)
    return all_articles


# --- Polymarket API Client ---

async def fetch_polymarket_markets(
    limit: int = 50,
    order: str = "volume24hr",
    ascending: bool = False,
) -> list[dict]:
    """Fetch trending/high-volume markets from Polymarket via the events API.

    Uses the /events endpoint which provides event-level slugs that resolve
    to valid URLs (https://polymarket.com/event/{event_slug}).

    Returns list of normalized market dicts:
    {question, probability, volume, price_history, resolution_criteria, slug}
    """
    results = []
    try:
        async with httpx.AsyncClient() as client:
            params = {
                "limit": limit,
                "order": order,
                "ascending": str(ascending).lower(),
                "active": "true",
                "closed": "false",
            }
            resp = await client.get(
                f"{POLYMARKET_API_URL}/events",
                params=params,
                timeout=15,
            )
            if resp.status_code != 200:
                logger.warning("Polymarket API returned status %d", resp.status_code)
                return results

            data = resp.json()
            if not isinstance(data, list):
                data = data.get("data", data.get("events", []))

            for event in data:
                if not isinstance(event, dict):
                    continue
                event_slug = event.get("slug", "")
                event_title = event.get("title", "")
                event_end_date = event.get("endDate", "")
                event_description = event.get("description", "")

                # Process each sub-market within the event
                sub_markets = event.get("markets", [])
                if not sub_markets:
                    continue

                for market in sub_markets:
                    if not isinstance(market, dict):
                        continue
                    question = market.get("question", "")
                    if not question:
                        continue

                    # Extract outcome prices (probability)
                    outcomes = market.get("outcomePrices", market.get("outcomes", []))
                    probability = _parse_probability(outcomes)

                    volume = float(market.get("volume", 0) or 0)
                    volume_24h = float(market.get("volume24hr", 0) or 0)

                    # Filter out extreme probabilities (gaming contracts)
                    if probability <= 0.05 or probability >= 0.95:
                        continue

                    results.append({
                        "question": question,
                        "probability": probability,
                        "volume": volume,
                        "volume_24h": volume_24h,
                        "resolution_criteria": event_description or market.get("description", ""),
                        "slug": event_slug,  # Use event slug for valid URLs
                        "end_date": market.get("endDate", "") or event_end_date,
                        "source": "polymarket",
                    })

    except Exception as e:
        logger.error("Polymarket API error: %s", e)

    return results


def _parse_probability(outcomes) -> float:
    """Parse probability from Polymarket outcome prices."""
    if isinstance(outcomes, list) and outcomes:
        try:
            return float(outcomes[0]) if isinstance(outcomes[0], (int, float, str)) else 0.0
        except (ValueError, TypeError):
            return 0.0
    elif isinstance(outcomes, str):
        try:
            import json as _json
            parsed = _json.loads(outcomes)
            if isinstance(parsed, list) and parsed:
                return float(parsed[0])
        except Exception:
            pass
    return 0.0


async def fetch_callsheet_markets(
    limit: int = 20,
    order: str = "volume",
    ascending: bool = False,
) -> list[dict]:
    """Fetch markets from CallSheet prediction market.

    Currently disabled — CallSheet API is not publicly available.
    Returns empty list.
    """
    # CallSheet API endpoint is not publicly accessible.
    # Prediction market coverage is provided by Polymarket and Kalshi.
    return []


async def fetch_kalshi_markets(
    limit: int = 20,
) -> list[dict]:
    """Fetch trending/high-volume markets from Kalshi prediction market.

    Requires KALSHI_API_KEY environment variable to be set.
    Returns list of normalized market dicts matching the Polymarket format:
    {question, probability, volume, volume_24h, resolution_criteria, slug, end_date, source}
    """
    markets = []
    if not KALSHI_API_KEY:
        logger.debug("Kalshi API key not configured (set KALSHI_API_KEY env var) — skipping")
        return markets
    try:
        headers = {
            "Authorization": f"Bearer {KALSHI_API_KEY}",
            "Accept": "application/json",
        }
        async with httpx.AsyncClient(headers=headers) as client:
            params = {
                "limit": limit,
                "status": "open",
            }
            resp = await client.get(
                f"{KALSHI_API_URL}/markets",
                params=params,
                timeout=15,
            )
            if resp.status_code == 401:
                logger.debug("Kalshi API key invalid or expired — check KALSHI_API_KEY")
                return markets
            if resp.status_code != 200:
                logger.warning("Kalshi API returned status %d", resp.status_code)
                return markets

            data = resp.json()
            market_list = data.get("markets", [])
            if not isinstance(market_list, list):
                market_list = []

            for market in market_list:
                if not isinstance(market, dict):
                    continue
                question = market.get("title", "") or market.get("subtitle", "")
                if not question:
                    continue

                # Kalshi uses yes_bid/yes_ask as cents (0-100)
                probability = 0.0
                yes_bid = market.get("yes_bid")
                yes_ask = market.get("yes_ask")
                last_price = market.get("last_price")
                if yes_bid is not None and yes_ask is not None:
                    try:
                        probability = (float(yes_bid) + float(yes_ask)) / 2 / 100.0
                    except (ValueError, TypeError):
                        pass
                elif last_price is not None:
                    try:
                        probability = float(last_price) / 100.0
                    except (ValueError, TypeError):
                        pass

                volume = float(market.get("volume", 0) or 0)
                volume_24h = float(market.get("volume_24h", 0) or 0)

                # Filter out extreme probabilities (gaming contracts)
                if probability <= 0.05 or probability >= 0.95:
                    continue

                ticker = market.get("ticker", "")
                end_date = market.get("close_time", market.get("expiration_time", ""))

                markets.append({
                    "question": question,
                    "probability": probability,
                    "volume": volume,
                    "volume_24h": volume_24h,
                    "resolution_criteria": _to_str(market.get("rules_primary", market.get("settlement_sources", ""))),
                    "slug": ticker,
                    "end_date": end_date,
                    "source": "kalshi",
                    "url": f"https://kalshi.com/markets/{ticker}" if ticker else "",
                })

    except Exception as e:
        logger.error("Kalshi API error: %s", e)

    return markets


def detect_probability_shift(current: float, history: list[dict], hours: int = 24) -> dict:
    """Detect significant probability shifts in market data.

    Returns {shift, is_significant, direction} where shift >10% = significant.
    """
    if not history:
        return {"shift": 0.0, "is_significant": False, "direction": "flat"}

    cutoff = time.time() - (hours * 3600)
    recent = [p for p in history if p.get("timestamp", 0) >= cutoff]
    if not recent:
        return {"shift": 0.0, "is_significant": False, "direction": "flat"}

    oldest = min(recent, key=lambda p: p["timestamp"])
    shift = current - oldest.get("probability", current)
    return {
        "shift": round(shift, 4),
        "is_significant": abs(shift) >= 0.10,
        "direction": "up" if shift > 0.01 else "down" if shift < -0.01 else "flat",
    }


def match_market_to_events(market: dict, event_headlines: list[str], event_entities: list[list[str]]) -> Optional[int]:
    """Try to match a Polymarket market to an existing event cluster.

    Returns the index of the best matching event, or None if no match.
    Uses entity overlap + keyword overlap with strict thresholds to avoid
    false matches (e.g., sports bets matching to unrelated news events).
    """
    question = market["question"].lower()
    question_words = set(re.findall(r'\b[a-z]\w{2,}\b', question))  # Must start with letter
    # Remove common/generic words that cause false matches
    stop_words = {"will", "the", "and", "for", "that", "this", "with", "from",
                  "have", "been", "are", "was", "were", "not", "but", "what",
                  "before", "after", "about", "into", "than", "more", "most",
                  "win", "over", "under", "end", "year", "next", "any", "may",
                  "one", "two", "new", "can", "how", "who", "when", "where",
                  "which", "does", "did", "its", "all", "would", "could",
                  "should", "between", "during", "through", "hit", "by"}
    question_words -= stop_words

    if len(question_words) < 2:
        return None  # Too few meaningful words to match

    # Extract entities from market question for stricter matching
    market_entities = set()
    for ent in market.get("entities", []):
        market_entities.update(re.findall(r'\b\w{3,}\b', ent.lower()))

    best_score = 0
    best_idx = None

    for idx, (headline, entities) in enumerate(zip(event_headlines, event_entities)):
        headline_words = set(re.findall(r'\b[a-z]\w{2,}\b', headline.lower()))
        entity_words = set()
        for e in entities:
            entity_words.update(re.findall(r'\b[a-z]\w{2,}\b', e.lower()))

        # Check if event entities appear in the market question (strongest signal)
        event_entity_match = len(question_words & entity_words)

        # Word overlap between question and headline+entities
        combined = headline_words | entity_words
        word_overlap = question_words & combined
        word_score = len(word_overlap) / max(len(question_words), 1)

        # Market entity overlap (if market has entities field)
        market_entity_match = len(market_entities & entity_words) if market_entities else 0

        # Match criteria (require strong evidence to prevent false positives):
        # 1. Entity from event appears in question + some word overlap
        # 2. High word overlap alone (60%+)
        total_entity_match = event_entity_match + market_entity_match
        if (total_entity_match >= 1 and word_score >= 0.3) or word_score >= 0.6:
            score = word_score + (total_entity_match * 0.2)
            if score > best_score:
                best_score = score
                best_idx = idx

    return best_idx


# ============================================================================
# Social Media Ingestion Pipeline
# Each function fetches from a real social platform API and returns normalized
# article dicts with source_type='social'. All are wired into _poll_social_loop()
# which runs as a background task alongside RSS and Polymarket polling.
# ============================================================================

# --- Hacker News API ---

async def fetch_hn_top_stories(limit: int = 30) -> list[dict]:
    """Fetch top stories from Hacker News Firebase API.

    Makes real HTTP calls to https://hacker-news.firebaseio.com/v0/.
    Returns list of normalized article dicts with source_type='social'.
    """
    articles = []
    try:
        async with httpx.AsyncClient() as client:
            # Get top story IDs
            resp = await client.get(f"{HN_API_URL}/topstories.json", timeout=15)
            if resp.status_code != 200:
                logger.warning("HN API returned status %d", resp.status_code)
                return articles
            story_ids = resp.json()[:limit]

            # Fetch each story's details (batch with concurrency limit)
            tasks = [_fetch_hn_item(client, sid) for sid in story_ids]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for result in results:
                if isinstance(result, dict) and result.get("title"):
                    articles.append(result)
    except Exception as e:
        logger.error("HN API error: %s", e)
    return articles


def parse_hn_story(item: dict) -> dict:
    """Parse a Hacker News story item into a normalized article dict."""
    title = item.get("title", "")
    url = item.get("url", "")
    text = item.get("text", "")
    score = item.get("score", 0)
    descendants = item.get("descendants", 0)
    author = item.get("by", "")
    item_id = item.get("id", 0)
    timestamp = float(item.get("time", time.time()))

    if url:
        url = strip_tracking_params(url)
    else:
        url = f"https://news.ycombinator.com/item?id={item_id}"

    if text:
        text = clean_html(text)

    return {
        "title": title,
        "author": author,
        "publisher": "Hacker News",
        "timestamp": timestamp,
        "url": url,
        "text": text or f"HN discussion: {title}",
        "source_type": "social",
        "social_coverage": score + descendants,
    }


async def _fetch_hn_item(client: httpx.AsyncClient, item_id: int) -> Optional[dict]:
    """Fetch a single HN item and normalize it."""
    try:
        resp = await client.get(f"{HN_API_URL}/item/{item_id}.json", timeout=10)
        if resp.status_code != 200:
            return None
        item = resp.json()
        if not item or item.get("type") != "story":
            return None
        return parse_hn_story(item)
    except Exception as e:
        logger.debug("Error fetching HN item %d: %s", item_id, e)
        return None


# --- Reddit API ---

def get_configured_subreddits() -> list[str]:
    """Get subreddits to track from environment or defaults."""
    env_subs = os.environ.get("NEWS_MONKEY_SUBREDDITS", "")
    if env_subs:
        return [s.strip() for s in env_subs.split(",") if s.strip()]
    return DEFAULT_SUBREDDITS


def parse_reddit_post(post: dict) -> dict:
    """Parse a Reddit post data dict into a normalized article dict."""
    title = post.get("title", "")
    url = post.get("url", "")
    selftext = post.get("selftext", "")
    score = post.get("score", 0)
    num_comments = post.get("num_comments", 0)
    author = post.get("author", "")
    created_utc = float(post.get("created_utc", time.time()))
    permalink = post.get("permalink", "")
    subreddit = post.get("subreddit", "unknown")

    if url and not url.startswith("https://www.reddit.com"):
        url = strip_tracking_params(url)
    else:
        url = f"https://www.reddit.com{permalink}" if permalink else ""

    text = selftext if selftext else f"Reddit r/{subreddit}: {title}"
    text = clean_html(text)
    text = strip_boilerplate(text)

    return {
        "title": title,
        "author": f"u/{author}" if author else "",
        "publisher": f"Reddit/r/{subreddit}",
        "timestamp": created_utc,
        "url": url,
        "text": text,
        "source_type": "social",
        "social_coverage": score + num_comments,
    }


async def fetch_reddit_posts(subreddits: Optional[list[str]] = None, limit: int = 10) -> list[dict]:
    """Fetch hot posts from Reddit subreddits using public JSON API.

    Returns list of normalized article dicts with source_type='social'.
    """
    if subreddits is None:
        subreddits = get_configured_subreddits()

    articles = []
    headers = {"User-Agent": "NewsMonkey/1.0 (news aggregator)"}

    try:
        async with httpx.AsyncClient(headers=headers) as client:
            for sub in subreddits:
                try:
                    resp = await client.get(
                        f"https://www.reddit.com/r/{sub}/hot.json",
                        params={"limit": limit, "raw_json": 1},
                        timeout=15,
                        follow_redirects=True,
                    )
                    if resp.status_code != 200:
                        logger.warning("Reddit /r/%s returned status %d", sub, resp.status_code)
                        continue

                    data = resp.json()
                    posts = data.get("data", {}).get("children", [])
                    for post_wrapper in posts:
                        post = post_wrapper.get("data", {})
                        if not post or post.get("stickied"):
                            continue
                        if not post.get("title"):
                            continue
                        post["subreddit"] = sub
                        articles.append(parse_reddit_post(post))
                except Exception as e:
                    logger.error("Error fetching Reddit /r/%s: %s", sub, e)
    except Exception as e:
        logger.error("Reddit API error: %s", e)

    return articles


# --- Bluesky Public API ---

BLUESKY_API_URL = "https://public.api.bsky.app"


async def fetch_bluesky_posts(search_terms: Optional[list[str]] = None, limit: int = 20) -> list[dict]:
    """Fetch posts from Bluesky using the public search API (no auth required).

    Returns list of normalized article dicts with source_type='social'.
    """
    if search_terms is None:
        search_terms = BLUESKY_SEARCH_TERMS

    articles = []
    try:
        async with httpx.AsyncClient() as client:
            for term in search_terms:
                try:
                    resp = await client.get(
                        f"{BLUESKY_API_URL}/xrpc/app.bsky.feed.searchPosts",
                        params={"q": term, "limit": min(limit, 25), "sort": "latest"},
                        timeout=15,
                    )
                    if resp.status_code != 200:
                        logger.debug("Bluesky search '%s' returned status %d", term, resp.status_code)
                        continue

                    data = resp.json()
                    for post in data.get("posts", []):
                        record = post.get("record", {})
                        text = record.get("text", "")
                        if not text or len(text) < 20:
                            continue

                        author_info = post.get("author", {})
                        handle = author_info.get("handle", "")
                        display_name = author_info.get("displayName", handle)
                        created_at = record.get("createdAt", "")
                        uri = post.get("uri", "")
                        # Convert AT URI to web URL
                        post_url = ""
                        if uri and handle:
                            parts = uri.split("/")
                            if len(parts) >= 5:
                                rkey = parts[-1]
                                post_url = f"https://bsky.app/profile/{handle}/post/{rkey}"

                        # Engagement metrics
                        like_count = post.get("likeCount", 0)
                        reply_count = post.get("replyCount", 0)
                        repost_count = post.get("repostCount", 0)
                        social_coverage = like_count + reply_count + repost_count

                        timestamp = time.time()
                        if created_at:
                            timestamp = _parse_rss_date(created_at)

                        # Use first line as title, rest as text
                        lines = text.strip().split("\n")
                        title = lines[0][:120]

                        articles.append({
                            "title": title,
                            "author": f"@{handle}" if handle else display_name,
                            "publisher": "Bluesky",
                            "timestamp": timestamp,
                            "url": post_url,
                            "text": text,
                            "source_type": "social",
                            "social_coverage": social_coverage,
                        })
                except Exception as e:
                    logger.debug("Bluesky search error for '%s': %s", term, e)
    except Exception as e:
        logger.error("Bluesky API error: %s", e)

    return articles


# --- Mastodon RSS Feeds ---

async def fetch_mastodon_feeds() -> list[dict]:
    """Fetch posts from Mastodon accounts via their public RSS feeds.

    Returns list of normalized article dicts with source_type='social'.
    """
    articles = []
    try:
        async with httpx.AsyncClient(
            headers={"User-Agent": "NewsMonkey/1.0 (news aggregator)"},
            follow_redirects=True,
        ) as client:
            for feed_url in MASTODON_FEEDS:
                try:
                    resp = await client.get(feed_url, timeout=15)
                    if resp.status_code != 200:
                        logger.debug("Mastodon feed %s returned status %d", feed_url, resp.status_code)
                        continue

                    feed_articles = parse_rss_feed(resp.text)
                    for article in feed_articles:
                        article["source_type"] = "social"
                        if not article.get("publisher") or article["publisher"] == "":
                            article["publisher"] = "Mastodon"
                        articles.append(article)
                except Exception as e:
                    logger.debug("Mastodon feed error for %s: %s", feed_url, e)
    except Exception as e:
        logger.error("Mastodon feed error: %s", e)

    return articles


# --- Twitter/X via Nitter RSS ---

async def fetch_twitter_rss(
    accounts: Optional[list[str]] = None,
    instances: Optional[list[str]] = None,
) -> list[dict]:
    """Fetch tweets from Twitter/X accounts via Nitter RSS feeds (no auth required).

    Tries multiple Nitter instances as fallback. Returns list of normalized
    article dicts with source_type='social'.
    """
    if accounts is None:
        accounts = TWITTER_ACCOUNTS
    if instances is None:
        instances = NITTER_INSTANCES

    articles = []
    try:
        async with httpx.AsyncClient(
            headers={"User-Agent": "NewsMonkey/1.0 (news aggregator)"},
            follow_redirects=True,
            timeout=15,
        ) as client:
            for account in accounts:
                fetched = False
                for instance in instances:
                    if fetched:
                        break
                    try:
                        url = f"{instance}/{account}/rss"
                        resp = await client.get(url, timeout=10)
                        if resp.status_code != 200:
                            continue

                        feed_articles = parse_rss_feed(resp.text)
                        for article in feed_articles:
                            article["source_type"] = "social"
                            article["publisher"] = f"Twitter/X/@{account}"
                            if not article.get("author"):
                                article["author"] = f"@{account}"
                            # Convert nitter URLs to x.com URLs
                            if article.get("url"):
                                for inst in instances:
                                    if inst in article["url"]:
                                        article["url"] = article["url"].replace(inst, "https://x.com")
                                        break
                            articles.append(article)
                        fetched = True
                        if feed_articles:
                            logger.debug("Twitter/X: fetched %d tweets from @%s via %s",
                                         len(feed_articles), account, instance)
                    except Exception as e:
                        logger.debug("Twitter/X: error fetching @%s from %s: %s", account, instance, e)
    except Exception as e:
        logger.error("Twitter/X RSS error: %s", e)

    return articles


# --- TikTok via RSSHub Bridge ---

async def fetch_tiktok_trending(limit: int = 20) -> list[dict]:
    """Fetch trending TikTok content via RSSHub RSS bridge.

    Uses the public RSSHub endpoint for TikTok trending content.
    Returns list of normalized article dicts with source_type='social'.
    """
    articles = []
    try:
        async with httpx.AsyncClient(
            headers={"User-Agent": "NewsMonkey/1.0 (news aggregator)"},
            follow_redirects=True,
        ) as client:
            resp = await client.get(TIKTOK_RSS_URL, timeout=15)
            if resp.status_code != 200:
                logger.warning("TikTok RSS bridge returned status %d", resp.status_code)
                return articles

            feed_articles = parse_rss_feed(resp.text)
            for article in feed_articles[:limit]:
                article["source_type"] = "social"
                article["publisher"] = "TikTok"
                if not article.get("social_coverage"):
                    article["social_coverage"] = 0
                articles.append(article)

            if articles:
                logger.debug("TikTok: fetched %d trending items", len(articles))
    except Exception as e:
        logger.error("TikTok RSS bridge error: %s", e)

    return articles


# --- Instagram via RSSHub Bridge ---

async def fetch_instagram_posts(limit: int = 20) -> list[dict]:
    """Fetch Instagram explore/trending content via RSSHub RSS bridge.

    Uses the public RSSHub endpoint for Instagram explore content.
    Returns list of normalized article dicts with source_type='social'.
    """
    articles = []
    try:
        async with httpx.AsyncClient(
            headers={"User-Agent": "NewsMonkey/1.0 (news aggregator)"},
            follow_redirects=True,
        ) as client:
            resp = await client.get(INSTAGRAM_RSS_URL, timeout=15)
            if resp.status_code != 200:
                logger.warning("Instagram RSS bridge returned status %d", resp.status_code)
                return articles

            feed_articles = parse_rss_feed(resp.text)
            for article in feed_articles[:limit]:
                article["source_type"] = "social"
                article["publisher"] = "Instagram"
                if not article.get("social_coverage"):
                    article["social_coverage"] = 0
                articles.append(article)

            if articles:
                logger.debug("Instagram: fetched %d explore items", len(articles))
    except Exception as e:
        logger.error("Instagram RSS bridge error: %s", e)

    return articles


# --- Publisher API: NewsAPI ---

NEWSAPI_KEY = os.environ.get("NEWSAPI_KEY", "")
NEWSAPI_URL = "https://newsapi.org/v2/top-headlines"
NEWSAPI_POLL_INTERVAL = int(os.environ.get("NEWSAPI_POLL_INTERVAL", "600"))  # 10 min default


async def fetch_newsapi_articles(
    country: str = "us",
    category: str = "",
    page_size: int = 20,
) -> list[dict]:
    """Fetch articles from NewsAPI publisher API.

    Requires NEWSAPI_KEY environment variable.
    Returns list of normalized article dicts with source_type='api'.
    """
    if not NEWSAPI_KEY:
        return []

    articles = []
    try:
        params = {
            "apiKey": NEWSAPI_KEY,
            "country": country,
            "pageSize": page_size,
        }
        if category:
            params["category"] = category

        async with httpx.AsyncClient() as client:
            resp = await client.get(NEWSAPI_URL, params=params, timeout=15)
            if resp.status_code != 200:
                logger.warning("NewsAPI returned status %d", resp.status_code)
                return articles

            data = resp.json()
            if data.get("status") != "ok":
                logger.warning("NewsAPI error: %s", data.get("message", "unknown"))
                return articles

            for item in data.get("articles", []):
                title = item.get("title", "")
                if not title or title == "[Removed]":
                    continue

                url = item.get("url", "")
                description = item.get("description", "") or ""
                content = item.get("content", "") or ""
                # NewsAPI truncates content with "[+N chars]" — use description as fallback
                text = content if len(content) > len(description) else description
                text = re.sub(r'\[\+\d+ chars\]$', '', text).strip()

                source = item.get("source", {})
                publisher = source.get("name", "")
                author = item.get("author", "") or ""
                pub_date = item.get("publishedAt", "")

                timestamp = time.time()
                if pub_date:
                    timestamp = _parse_rss_date(pub_date)

                articles.append({
                    "title": title,
                    "author": author,
                    "publisher": publisher,
                    "timestamp": timestamp,
                    "url": strip_tracking_params(url) if url else "",
                    "text": clean_html(text),
                    "source_type": "api",
                })
    except Exception as e:
        logger.error("NewsAPI error: %s", e)

    return articles


# --- Ingestion Runner ---

class IngestionRunner:
    """Background ingestion runner that polls feeds and Polymarket on intervals."""

    def __init__(self):
        self._running = False
        self._rss_task: Optional[asyncio.Task] = None
        self._polymarket_task: Optional[asyncio.Task] = None
        self._social_task: Optional[asyncio.Task] = None
        self._newsapi_task: Optional[asyncio.Task] = None
        self._seen_urls: dict[str, float] = {}  # hash -> timestamp for LRU eviction
        self._MAX_SEEN_URLS = 50000  # Cap to prevent unbounded growth
        self.on_new_article = None  # Callback: async def(article_dict)
        self.on_new_market = None   # Callback: async def(market_dict)

    def _trim_seen_urls(self):
        """Evict oldest half of seen URLs cache if it exceeds the cap."""
        if len(self._seen_urls) > self._MAX_SEEN_URLS:
            sorted_keys = sorted(self._seen_urls, key=self._seen_urls.get)
            half = len(sorted_keys) // 2
            for k in sorted_keys[:half]:
                del self._seen_urls[k]
            logger.info("Evicted %d oldest entries from _seen_urls cache", half)

    def _preseed_seen_urls(self):
        """Pre-seed _seen_urls from existing article URLs in database to prevent re-ingestion on restart."""
        try:
            import database as db
            existing_hashes = db.get_existing_article_url_hashes()
            now = time.time()
            for h in existing_hashes:
                self._seen_urls[h] = now
            if existing_hashes:
                logger.info("Pre-seeded %d URL hashes from database into _seen_urls cache", len(existing_hashes))
        except Exception as e:
            logger.error("Failed to pre-seed _seen_urls from database: %s", e)

    async def start(self):
        """Start background polling tasks."""
        self._running = True
        self._preseed_seen_urls()
        self._rss_task = asyncio.create_task(self._poll_rss_loop())
        self._polymarket_task = asyncio.create_task(self._poll_polymarket_loop())
        self._social_task = asyncio.create_task(self._poll_social_loop())
        if NEWSAPI_KEY:
            self._newsapi_task = asyncio.create_task(self._poll_newsapi_loop())
            logger.info("NewsAPI polling enabled (interval: %ds)", NEWSAPI_POLL_INTERVAL)
        logger.info("Ingestion runner started (RSS: %ds, Polymarket: %ds, Social: %ds)",
                     RSS_POLL_INTERVAL, POLYMARKET_POLL_INTERVAL, SOCIAL_POLL_INTERVAL)

    async def stop(self):
        """Stop background polling tasks."""
        self._running = False
        for task in [self._rss_task, self._polymarket_task, self._social_task, self._newsapi_task]:
            if task:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
        logger.info("Ingestion runner stopped")

    async def _poll_rss_loop(self):
        """Continuously poll RSS feeds on interval."""
        while self._running:
            try:
                articles = await poll_all_feeds()
                async with httpx.AsyncClient(
                    headers={"User-Agent": "NewsMonkey/1.0 (news aggregator)"},
                    follow_redirects=True,
                ) as scrape_client:
                    for article in articles:
                        url = article.get("url", "")
                        clean_url = strip_tracking_params(url) if url else ""
                        url_hash = hashlib.md5(clean_url.encode()).hexdigest() if clean_url else ""
                        if url_hash and url_hash not in self._seen_urls:
                            self._seen_urls[url_hash] = time.time()
                            # Scrape full article body if RSS only gave short excerpt
                            text = article.get("text", "")
                            if len(text.split()) < MIN_FULL_TEXT_WORDS and url:
                                scraped = await scrape_article_body(url, client=scrape_client)
                                if scraped:
                                    article["text"] = scraped
                                    article["source_type"] = "scrape"
                                    logger.debug("Scraped full text for: %s", article.get("title", "")[:60])
                            if self.on_new_article:
                                await self.on_new_article(article)
                            # Yield to event loop so HTTP requests can be served
                            await asyncio.sleep(0)
            except Exception as e:
                logger.error("RSS poll loop error: %s", e)
            self._trim_seen_urls()
            await asyncio.sleep(RSS_POLL_INTERVAL)

    async def _poll_polymarket_loop(self):
        """Continuously poll Polymarket and CallSheet prediction markets on interval."""
        while self._running:
            try:
                # Fetch by 24h volume (trending) and total volume (established)
                for sort_order in ["volume24hr", "volume"]:
                    markets = await fetch_polymarket_markets(order=sort_order)
                    for market in markets:
                        if self.on_new_market:
                            await self.on_new_market(market)
            except Exception as e:
                logger.error("Polymarket poll loop error: %s", e)

            try:
                callsheet_markets = await fetch_callsheet_markets()
                for market in callsheet_markets:
                    if self.on_new_market:
                        await self.on_new_market(market)
                if callsheet_markets:
                    logger.info("CallSheet poll: ingested %d markets", len(callsheet_markets))
            except Exception as e:
                logger.error("CallSheet poll loop error: %s", e)

            try:
                kalshi_markets = await fetch_kalshi_markets()
                for market in kalshi_markets:
                    if self.on_new_market:
                        await self.on_new_market(market)
                if kalshi_markets:
                    logger.info("Kalshi poll: ingested %d markets", len(kalshi_markets))
            except Exception as e:
                logger.error("Kalshi poll loop error: %s", e)

            self._trim_seen_urls()
            await asyncio.sleep(POLYMARKET_POLL_INTERVAL)

    async def _poll_newsapi_loop(self):
        """Continuously poll NewsAPI publisher API on interval."""
        while self._running:
            try:
                for category in ["general", "business", "technology"]:
                    articles = await fetch_newsapi_articles(category=category)
                    async with httpx.AsyncClient(
                        headers={"User-Agent": "NewsMonkey/1.0 (news aggregator)"},
                        follow_redirects=True,
                    ) as scrape_client:
                        for article in articles:
                            url = article.get("url", "")
                            clean_url = strip_tracking_params(url) if url else ""
                            url_hash = hashlib.md5(clean_url.encode()).hexdigest() if clean_url else ""
                            if url_hash and url_hash not in self._seen_urls:
                                self._seen_urls[url_hash] = time.time()
                                # Scrape full body since NewsAPI truncates content
                                text = article.get("text", "")
                                if len(text.split()) < MIN_FULL_TEXT_WORDS and url:
                                    scraped = await scrape_article_body(url, client=scrape_client)
                                    if scraped:
                                        article["text"] = scraped
                                logger.debug("NewsAPI article: %s", article.get("title", "")[:60])
                                if self.on_new_article:
                                    await self.on_new_article(article)
            except Exception as e:
                logger.error("NewsAPI poll loop error: %s", e)
            self._trim_seen_urls()
            await asyncio.sleep(NEWSAPI_POLL_INTERVAL)

    async def _poll_social_loop(self):
        """Continuously poll all 7 social media sources on interval.

        Sources polled each cycle:
          1. Hacker News (Firebase API)
          2. Reddit (Public JSON API)
          3. Bluesky (AT Protocol search API)
          4. Mastodon (RSS feeds from news accounts)
          5. Twitter/X (Nitter RSS bridge)
          6. TikTok (RSSHub bridge)
          7. Instagram (RSSHub bridge)

        Each source uses real HTTP calls via httpx to external APIs.
        Articles are normalized to source_type='social' and deduped by URL hash.
        """
        while self._running:
            try:
                # Fetch from Hacker News
                hn_articles = await fetch_hn_top_stories(limit=30)
                new_hn = 0
                for article in hn_articles:
                    url = article.get("url", "")
                    clean_url = strip_tracking_params(url) if url else ""
                    url_hash = hashlib.md5(clean_url.encode()).hexdigest() if clean_url else ""
                    if url_hash and url_hash not in self._seen_urls:
                        self._seen_urls[url_hash] = time.time()
                        new_hn += 1
                        if self.on_new_article:
                            await self.on_new_article(article)
                if new_hn:
                    logger.info("Social poll: ingested %d new HN stories", new_hn)
            except Exception as e:
                logger.error("HN poll error: %s", e)

            try:
                # Fetch from Reddit
                reddit_articles = await fetch_reddit_posts()
                new_reddit = 0
                for article in reddit_articles:
                    url = article.get("url", "")
                    clean_url = strip_tracking_params(url) if url else ""
                    url_hash = hashlib.md5(clean_url.encode()).hexdigest() if clean_url else ""
                    if url_hash and url_hash not in self._seen_urls:
                        self._seen_urls[url_hash] = time.time()
                        new_reddit += 1
                        if self.on_new_article:
                            await self.on_new_article(article)
                if new_reddit:
                    logger.info("Social poll: ingested %d new Reddit posts", new_reddit)
            except Exception as e:
                logger.error("Reddit poll error: %s", e)

            try:
                # Fetch from Bluesky
                bsky_articles = await fetch_bluesky_posts()
                new_bsky = 0
                for article in bsky_articles:
                    url = article.get("url", "")
                    clean_url = strip_tracking_params(url) if url else ""
                    url_hash = hashlib.md5(clean_url.encode()).hexdigest() if clean_url else ""
                    if url_hash and url_hash not in self._seen_urls:
                        self._seen_urls[url_hash] = time.time()
                        new_bsky += 1
                        if self.on_new_article:
                            await self.on_new_article(article)
                if new_bsky:
                    logger.info("Social poll: ingested %d new Bluesky posts", new_bsky)
            except Exception as e:
                logger.error("Bluesky poll error: %s", e)

            try:
                # Fetch from Mastodon
                mastodon_articles = await fetch_mastodon_feeds()
                new_mastodon = 0
                for article in mastodon_articles:
                    url = article.get("url", "")
                    clean_url = strip_tracking_params(url) if url else ""
                    url_hash = hashlib.md5(clean_url.encode()).hexdigest() if clean_url else ""
                    if url_hash and url_hash not in self._seen_urls:
                        self._seen_urls[url_hash] = time.time()
                        new_mastodon += 1
                        if self.on_new_article:
                            await self.on_new_article(article)
                if new_mastodon:
                    logger.info("Social poll: ingested %d new Mastodon posts", new_mastodon)
            except Exception as e:
                logger.error("Mastodon poll error: %s", e)

            try:
                # Fetch from Twitter/X via Nitter RSS
                twitter_articles = await fetch_twitter_rss()
                new_twitter = 0
                for article in twitter_articles:
                    url = article.get("url", "")
                    clean_url = strip_tracking_params(url) if url else ""
                    url_hash = hashlib.md5(clean_url.encode()).hexdigest() if clean_url else ""
                    if url_hash and url_hash not in self._seen_urls:
                        self._seen_urls[url_hash] = time.time()
                        new_twitter += 1
                        if self.on_new_article:
                            await self.on_new_article(article)
                if new_twitter:
                    logger.info("Social poll: ingested %d new Twitter/X posts", new_twitter)
            except Exception as e:
                logger.error("Twitter/X poll error: %s", e)

            try:
                # Fetch from TikTok via RSSHub bridge
                tiktok_articles = await fetch_tiktok_trending()
                new_tiktok = 0
                for article in tiktok_articles:
                    url = article.get("url", "")
                    clean_url = strip_tracking_params(url) if url else ""
                    url_hash = hashlib.md5(clean_url.encode()).hexdigest() if clean_url else ""
                    if url_hash and url_hash not in self._seen_urls:
                        self._seen_urls[url_hash] = time.time()
                        new_tiktok += 1
                        if self.on_new_article:
                            await self.on_new_article(article)
                if new_tiktok:
                    logger.info("Social poll: ingested %d new TikTok trending items", new_tiktok)
            except Exception as e:
                logger.error("TikTok poll error: %s", e)

            try:
                # Fetch from Instagram via RSSHub bridge
                instagram_articles = await fetch_instagram_posts()
                new_instagram = 0
                for article in instagram_articles:
                    url = article.get("url", "")
                    clean_url = strip_tracking_params(url) if url else ""
                    url_hash = hashlib.md5(clean_url.encode()).hexdigest() if clean_url else ""
                    if url_hash and url_hash not in self._seen_urls:
                        self._seen_urls[url_hash] = time.time()
                        new_instagram += 1
                        if self.on_new_article:
                            await self.on_new_article(article)
                if new_instagram:
                    logger.info("Social poll: ingested %d new Instagram posts", new_instagram)
            except Exception as e:
                logger.error("Instagram poll error: %s", e)

            self._trim_seen_urls()
            await asyncio.sleep(SOCIAL_POLL_INTERVAL)


# Singleton ingestion runner
ingestion_runner = IngestionRunner()
