"""Data models for News Monkey."""
import time
import uuid
from dataclasses import dataclass, field
from typing import Optional


def _now() -> float:
    return time.time()


def _id() -> str:
    return uuid.uuid4().hex[:12]


@dataclass
class Article:
    id: str = field(default_factory=_id)
    title: str = ""
    author: str = ""
    publisher: str = ""
    timestamp: float = field(default_factory=_now)
    url: str = ""
    text: str = ""
    word_count: int = 0
    entities: list[str] = field(default_factory=list)
    key_sentences: list[str] = field(default_factory=list)
    fact_density: float = 0.0
    sensationalism_score: float = 0.0
    source_type: str = "rss"  # rss, social, api, scrape
    social_coverage: int = 0  # estimated social media mentions
    cluster_id: Optional[str] = None


@dataclass
class Claim:
    id: str = field(default_factory=_id)
    who: str = ""
    what: str = ""
    when: str = ""
    where: str = ""
    numbers: list[str] = field(default_factory=list)
    direct_quotes: list[str] = field(default_factory=list)
    source_article_id: str = ""
    uncertainty: str = ""


@dataclass
class EventCluster:
    id: str = field(default_factory=_id)
    headline: str = ""
    summary: str = ""
    entities: list[str] = field(default_factory=list)
    earliest_timestamp: float = field(default_factory=_now)
    latest_timestamp: float = field(default_factory=_now)
    source_count: int = 0
    confidence: float = 0.0
    impact: str = "medium"  # high, medium, low
    article_ids: list[str] = field(default_factory=list)
    claims: list[dict] = field(default_factory=list)
    market_odds: Optional[float] = None
    market_question: Optional[str] = None
    price_history: list[dict] = field(default_factory=list)  # [{timestamp, probability}]
    market_volume: Optional[float] = None
    resolution_criteria: str = ""
    timeline: list[dict] = field(default_factory=list)
    disputed_claims: list[dict] = field(default_factory=list)
    novel_facts: list[str] = field(default_factory=list)
    topic: str = ""
    geography: str = ""
    impact_score: Optional[float] = None  # computed impact score
    neutral_headline: str = ""  # LLM or rule-based neutralized headline
    social_score: float = 0.0  # social media coverage score
