"""Database layer using SQLite with JSON storage for News Monkey."""
import hashlib
import json
import logging
import math
import re
import sqlite3
import threading
import time
import os
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

from models import Article, EventCluster, Claim
import processing

DB_PATH = Path(os.environ.get("NEWS_MONKEY_DB", Path(__file__).parent / "data" / "news_monkey.db"))

# --- Summary / Event Cache ---
# TTL-based cache for cluster summaries and event data to avoid recomputation
_summary_cache: dict[str, tuple[float, dict]] = {}  # cluster_id -> (expiry_time, data)
_summary_cache_lock = threading.Lock()
CACHE_TTL = 30  # seconds
_MAX_CACHE_SIZE = 1000


def _cache_get(key: str) -> Optional[dict]:
    """Get value from cache if not expired."""
    with _summary_cache_lock:
        if key in _summary_cache:
            expiry, data = _summary_cache[key]
            if time.time() < expiry:
                return data
            del _summary_cache[key]
    return None


def _cache_set(key: str, data: dict):
    """Set value in cache with TTL."""
    with _summary_cache_lock:
        if len(_summary_cache) >= _MAX_CACHE_SIZE:
            sorted_keys = sorted(_summary_cache, key=lambda k: _summary_cache[k][0])
            for k in sorted_keys[:len(sorted_keys) // 2]:
                del _summary_cache[k]
        _summary_cache[key] = (time.time() + CACHE_TTL, data)


def _cache_invalidate(cluster_id: str):
    """Invalidate cache entries for a cluster."""
    with _summary_cache_lock:
        _summary_cache.pop(f"cluster:{cluster_id}", None)
        stale = [k for k in _summary_cache if k.startswith("clusters:")]
        for k in stale:
            del _summary_cache[k]


def _strip_urls(text: str) -> str:
    """Strip URLs and URL fragments from text."""
    if not text:
        return ""
    text = re.sub(r'https?://\S+', '', text)
    return re.sub(r'\s{2,}', ' ', text).strip()


def get_db() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=5000")
    return conn


def init_db():
    conn = get_db()
    try:
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS articles (
            id TEXT PRIMARY KEY,
            title TEXT NOT NULL DEFAULT '',
            author TEXT DEFAULT '',
            publisher TEXT DEFAULT '',
            timestamp REAL NOT NULL,
            url TEXT DEFAULT '',
            text TEXT DEFAULT '',
            word_count INTEGER DEFAULT 0,
            entities TEXT DEFAULT '[]',
            key_sentences TEXT DEFAULT '[]',
            fact_density REAL DEFAULT 0.0,
            sensationalism_score REAL DEFAULT 0.0,
            neutral_title TEXT DEFAULT '',
            source_type TEXT DEFAULT 'rss',
            social_coverage INTEGER DEFAULT 0,
            cluster_id TEXT,
            FOREIGN KEY (cluster_id) REFERENCES event_clusters(id)
        );

        CREATE TABLE IF NOT EXISTS event_clusters (
            id TEXT PRIMARY KEY,
            headline TEXT NOT NULL DEFAULT '',
            summary TEXT DEFAULT '',
            entities TEXT DEFAULT '[]',
            earliest_timestamp REAL NOT NULL,
            latest_timestamp REAL NOT NULL,
            source_count INTEGER DEFAULT 0,
            confidence REAL DEFAULT 0.0,
            impact TEXT DEFAULT 'medium',
            article_ids TEXT DEFAULT '[]',
            claims TEXT DEFAULT '[]',
            market_odds REAL,
            market_question TEXT,
            price_history TEXT DEFAULT '[]',
            market_volume REAL,
            resolution_criteria TEXT DEFAULT '',
            timeline TEXT DEFAULT '[]',
            disputed_claims TEXT DEFAULT '[]',
            novel_facts TEXT DEFAULT '[]',
            topic TEXT DEFAULT '',
            geography TEXT DEFAULT '',
            impact_score REAL,
            neutral_headline TEXT DEFAULT '',
            social_score REAL DEFAULT 0.0
        );

        CREATE TABLE IF NOT EXISTS claims (
            id TEXT PRIMARY KEY,
            who TEXT DEFAULT '',
            what TEXT DEFAULT '',
            when_occurred TEXT DEFAULT '',
            where_occurred TEXT DEFAULT '',
            numbers TEXT DEFAULT '[]',
            direct_quotes TEXT DEFAULT '[]',
            source_article_id TEXT,
            uncertainty TEXT DEFAULT '',
            cluster_id TEXT,
            FOREIGN KEY (source_article_id) REFERENCES articles(id),
            FOREIGN KEY (cluster_id) REFERENCES event_clusters(id)
        );

        CREATE TABLE IF NOT EXISTS polymarket_bets (
            id TEXT PRIMARY KEY,
            question TEXT NOT NULL DEFAULT '',
            probability REAL DEFAULT 0.0,
            volume REAL DEFAULT 0.0,
            volume_24h REAL DEFAULT 0.0,
            resolution_criteria TEXT DEFAULT '',
            slug TEXT DEFAULT '',
            end_date TEXT DEFAULT '',
            timestamp REAL NOT NULL,
            is_unusual INTEGER DEFAULT 0,
            unusual_reason TEXT DEFAULT '',
            linked_cluster_id TEXT,
            FOREIGN KEY (linked_cluster_id) REFERENCES event_clusters(id)
        );

        CREATE INDEX IF NOT EXISTS idx_clusters_impact ON event_clusters(impact);
        CREATE INDEX IF NOT EXISTS idx_clusters_timestamp ON event_clusters(latest_timestamp DESC);
        CREATE INDEX IF NOT EXISTS idx_articles_cluster ON articles(cluster_id);
        CREATE INDEX IF NOT EXISTS idx_bets_unusual ON polymarket_bets(is_unusual);
        CREATE INDEX IF NOT EXISTS idx_bets_timestamp ON polymarket_bets(timestamp DESC);
        CREATE INDEX IF NOT EXISTS idx_claims_cluster ON claims(cluster_id);
        CREATE INDEX IF NOT EXISTS idx_claims_source_article ON claims(source_article_id);
        CREATE INDEX IF NOT EXISTS idx_articles_source_type ON articles(source_type);
        CREATE INDEX IF NOT EXISTS idx_clusters_headline ON event_clusters(headline);
    """)
        conn.commit()

        # Schema migrations for new columns
        _migrate_add_column(conn, "event_clusters", "resolution_criteria", "TEXT DEFAULT ''")
        _migrate_add_column(conn, "articles", "neutral_title", "TEXT DEFAULT ''")
        _migrate_add_column(conn, "articles", "source_type", "TEXT DEFAULT 'rss'")
        _migrate_add_column(conn, "articles", "social_coverage", "INTEGER DEFAULT 0")
        _migrate_add_column(conn, "event_clusters", "social_score", "REAL DEFAULT 0.0")
        _migrate_add_column(conn, "polymarket_bets", "source", "TEXT DEFAULT 'polymarket'")
        _migrate_add_column(conn, "event_clusters", "ai_relevant", "INTEGER")

        # Clean up expired prediction market bets
        _purge_expired_bets(conn)

        # Strip embedded URLs from neutral_headline fields
        _strip_urls_from_headlines(conn)

        # Clean up bad entity data (sentence fragments from title-cased headlines)
        _clean_bad_entities(conn)

        # Re-classify topics with improved keyword set
        _reclassify_topics(conn)

        # One-time migration: remove duplicate articles with same URL (keep earliest)
        _deduplicate_existing_articles(conn)

        # Add unique index on URL after dedup cleanup
        try:
            conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_articles_url_unique ON articles(url) WHERE url IS NOT NULL AND url != ''")
            conn.commit()
        except sqlite3.IntegrityError:
            logger.warning("Could not create unique URL index — duplicates may still exist")
    finally:
        conn.close()


def _purge_expired_bets(conn):
    """Remove prediction market bets whose end_date is in the past."""
    try:
        from datetime import datetime, timezone
        now_iso = datetime.now(timezone.utc).isoformat()
        # Delete bets with a parseable end_date that is before now
        rows = conn.execute(
            "SELECT id, end_date FROM polymarket_bets WHERE end_date IS NOT NULL AND end_date != ''"
        ).fetchall()
        expired_ids = []
        for r in rows:
            try:
                dt = datetime.fromisoformat(r["end_date"].replace("Z", "+00:00"))
                if dt < datetime.now(timezone.utc):
                    expired_ids.append(r["id"])
            except (ValueError, TypeError):
                pass
        if expired_ids:
            batch_size = 500
            for i in range(0, len(expired_ids), batch_size):
                batch = expired_ids[i:i + batch_size]
                placeholders = ",".join("?" * len(batch))
                conn.execute(f"DELETE FROM polymarket_bets WHERE id IN ({placeholders})", batch)
            conn.commit()
            logger.info("Purged %d expired prediction market bets", len(expired_ids))
    except Exception as e:
        logger.warning("Expired bet cleanup skipped: %s", e)


def _strip_urls_from_headlines(conn):
    """Strip embedded URLs from neutral_headline and headline fields."""
    try:
        rows = conn.execute(
            "SELECT id, headline, neutral_headline FROM event_clusters WHERE headline LIKE '%http%' OR neutral_headline LIKE '%http%'"
        ).fetchall()
        if not rows:
            return
        updated = 0
        for r in rows:
            clean_h = re.sub(r'https?://\S+', '', r["headline"]).strip()
            clean_h = re.sub(r'\s{2,}', ' ', clean_h)
            clean_n = ""
            if r["neutral_headline"]:
                clean_n = re.sub(r'https?://\S+', '', r["neutral_headline"]).strip()
                clean_n = re.sub(r'\s{2,}', ' ', clean_n)
            if clean_h != r["headline"] or clean_n != (r["neutral_headline"] or ""):
                conn.execute(
                    "UPDATE event_clusters SET headline = ?, neutral_headline = ? WHERE id = ?",
                    (clean_h, clean_n, r["id"])
                )
                updated += 1
        if updated > 0:
            conn.commit()
            logger.info("Stripped URLs from %d cluster headlines", updated)
    except Exception as e:
        logger.warning("URL headline cleanup skipped: %s", e)


def _clean_bad_entities(conn):
    """Remove sentence-fragment entities from cluster data.

    Targets multi-word entities that are clearly not proper names —
    e.g., "Oil Prices Spike Over", "Traders Now See Chance".
    """
    try:
        from ingestion import _ENTITY_STOPWORDS
        rows = conn.execute(
            "SELECT id, entities FROM event_clusters WHERE entities IS NOT NULL AND entities != '[]'"
        ).fetchall()
        updated = 0
        for r in rows:
            try:
                entities = json.loads(r["entities"])
            except (json.JSONDecodeError, TypeError):
                continue
            if not isinstance(entities, list):
                continue
            cleaned = []
            for ent in entities:
                words = ent.split()
                # Skip 4+ word phrases (likely sentence fragments)
                if len(words) > 3:
                    continue
                # Strip leading/trailing stopwords
                while words and words[0] in _ENTITY_STOPWORDS:
                    words = words[1:]
                while words and words[-1] in _ENTITY_STOPWORDS:
                    words = words[:-1]
                if not words:
                    continue
                # Skip if half or more of remaining words are stopwords
                non_stop = [w for w in words if w not in _ENTITY_STOPWORDS]
                if len(non_stop) <= len(words) / 2:
                    continue
                cleaned.append(" ".join(words))
            if cleaned != entities:
                conn.execute(
                    "UPDATE event_clusters SET entities = ? WHERE id = ?",
                    (json.dumps(cleaned), r["id"])
                )
                updated += 1
        if updated > 0:
            conn.commit()
            logger.info("Cleaned bad entities from %d clusters", updated)
    except Exception as e:
        logger.warning("Entity cleanup skipped: %s", e)


def _reclassify_topics(conn):
    """Re-classify cluster topics using the improved keyword set."""
    try:
        rows = conn.execute(
            "SELECT id, entities, headline, neutral_headline, topic FROM event_clusters WHERE topic IS NOT NULL AND topic != ''"
        ).fetchall()
        updated = 0
        for r in rows:
            try:
                ent_str = r["entities"] if r["entities"] else "[]"
                entities = json.loads(ent_str)
            except (json.JSONDecodeError, TypeError):
                entities = []
            headline = r["headline"] if r["headline"] else ""
            neutral = r["neutral_headline"] if r["neutral_headline"] else ""
            text = neutral or headline
            old_topic = r["topic"] if r["topic"] else ""
            new_topic = _infer_topic(entities, text)
            if new_topic and new_topic != old_topic:
                conn.execute("UPDATE event_clusters SET topic = ? WHERE id = ?", (new_topic, r["id"]))
                updated += 1
        if updated > 0:
            conn.commit()
            logger.info("Reclassified topics for %d clusters", updated)
    except Exception as e:
        logger.warning("Topic reclassification skipped: %s", e)


def _deduplicate_existing_articles(conn):
    """Remove duplicate articles sharing the same URL, keeping the earliest by rowid."""
    try:
        dupes = conn.execute("""
            SELECT id, cluster_id FROM articles
            WHERE url IS NOT NULL AND url != ''
              AND rowid NOT IN (
                  SELECT MIN(rowid) FROM articles
                  WHERE url IS NOT NULL AND url != ''
                  GROUP BY url
              )
        """).fetchall()
        if not dupes:
            return
        dupe_ids = [r["id"] for r in dupes]
        affected_clusters = {r["cluster_id"] for r in dupes if r["cluster_id"]}
        # Delete in batches to avoid SQLite variable limit
        batch_size = 500
        for i in range(0, len(dupe_ids), batch_size):
            batch = dupe_ids[i:i + batch_size]
            placeholders = ",".join("?" * len(batch))
            conn.execute(f"DELETE FROM claims WHERE source_article_id IN ({placeholders})", batch)
            conn.execute(f"DELETE FROM articles WHERE id IN ({placeholders})", batch)
        # Update source_count for affected clusters
        for cid in affected_clusters:
            count = conn.execute(
                "SELECT COUNT(*) FROM articles WHERE cluster_id=?", (cid,)
            ).fetchone()[0]
            conn.execute(
                "UPDATE event_clusters SET source_count=? WHERE id=?", (count, cid)
            )
        conn.commit()
        logger.info("Removed %d duplicate articles during migration, updated %d clusters", len(dupe_ids), len(affected_clusters))
    except Exception as e:
        logger.warning("Duplicate article cleanup skipped: %s", e)


_ALLOWED_TABLES = {"event_clusters", "articles", "claims", "polymarket_bets"}


_ALLOWED_COL_TYPES = {"TEXT", "INTEGER", "REAL", "BLOB"}


def _migrate_add_column(conn, table: str, column: str, col_type: str):
    """Add a column if it doesn't exist (schema migration)."""
    if table not in _ALLOWED_TABLES:
        raise ValueError(f"Invalid table for migration: {table}")
    if not re.match(r'^[a-z_]+$', column):
        raise ValueError(f"Invalid column name for migration: {column}")
    base_type = col_type.split()[0].upper()
    if base_type not in _ALLOWED_COL_TYPES:
        raise ValueError(f"Invalid column type for migration: {col_type}")
    cols = [row[1] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()]
    if column not in cols:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {col_type}")
        conn.commit()


def _row_to_cluster(row: sqlite3.Row) -> dict:
    d = dict(row)
    for field in ("entities", "article_ids", "claims", "timeline", "disputed_claims", "novel_facts", "price_history"):
        if d.get(field):
            try:
                d[field] = json.loads(d[field])
            except (json.JSONDecodeError, TypeError):
                d[field] = []
        else:
            d[field] = []
    return d


def _row_to_article(row: sqlite3.Row) -> dict:
    d = dict(row)
    for field in ("entities", "key_sentences"):
        if d.get(field):
            try:
                d[field] = json.loads(d[field])
            except (json.JSONDecodeError, TypeError):
                d[field] = []
        else:
            d[field] = []
    # Compute information density flag
    density = d.get("fact_density", 0)
    d["low_density"] = density < 0.001 and d.get("word_count", 0) > 100
    return d


# --- Event Cluster CRUD ---

def compute_impact_score(cluster: EventCluster) -> float:
    """Compute impact score from multiple components per requirements.

    Components: source count, authority weighting, prediction market probability/volume,
    magnitude (people affected, money involved), novelty score, content signals.
    Returns a 0-100 score.

    Calibrated so typical events spread across the 0-100 range:
    - 1-2 sources, no keywords, recent: ~20-30 (low)
    - 3-5 sources or keyword matches: ~35-50 (medium)
    - 5+ sources with keywords or market data: ~50+ (high)
    """
    score = 0.0

    # Source count component (0-25): more independent sources = higher impact
    # Scale: 1 src=3, 2=6, 4=12, 8=25 (saturates at 8)
    source_score = min(cluster.source_count / 8.0, 1.0) * 25
    score += source_score

    # Authority weighting via confidence (0-10)
    score += cluster.confidence * 10

    # Social coverage component (0-10): social media amplification
    social = getattr(cluster, 'social_score', 0) or 0
    social_score = min(social / 30.0, 1.0) * 10
    score += social_score

    # Prediction market component (0-20)
    if cluster.market_odds is not None:
        # Probability far from 0.5 = higher certainty = higher impact
        odds_distance = abs(cluster.market_odds - 0.5) * 2  # 0-1
        market_prob_score = odds_distance * 8
        volume_score = min((cluster.market_volume or 0) / 5_000_000, 1.0) * 7
        shift = _compute_probability_shift(cluster.price_history)
        shift_score = min(abs(shift) / 0.15, 1.0) * 5
        score += market_prob_score + volume_score + shift_score

    # Magnitude / novelty from content signals (0-15)
    novel_score = min(len(cluster.novel_facts) / 3.0, 1.0) * 6
    disputed_score = min(len(cluster.disputed_claims) / 2.0, 1.0) * 3
    timeline_score = min(len(cluster.timeline) / 3.0, 1.0) * 3
    entity_score = min(len(cluster.entities) / 2.0, 1.0) * 3
    score += novel_score + disputed_score + timeline_score + entity_score

    # Content-based magnitude heuristics (0-15): detect high-impact keywords
    headline = (cluster.headline or "").lower()
    summary = (cluster.summary or "").lower()
    content = headline + " " + summary
    _HIGH_IMPACT_KEYWORDS = [
        "federal reserve", "rate cut", "rate hike", "inflation", "gdp",
        "earnings", "ipo", "acquisition", "merger", "bankruptcy",
        "sanctions", "tariff", "regulation", "indictment", "resign",
        "crash", "rally", "recession", "war", "ceasefire", "election",
        "breaking", "urgent", "ai act", "antitrust", "sec ",
        "layoff", "hack", "breach", "default", "downgrade", "upgrade",
        "coup", "assassination", "pandemic", "earthquake", "hurricane",
        "trillion", "billion", "executive order", "supreme court",
    ]
    keyword_hits = sum(1 for kw in _HIGH_IMPACT_KEYWORDS if kw in content)
    # 1 keyword = 5pts, 2 = 10pts, 3+ = 15pts
    content_score = min(keyword_hits / 3.0, 1.0) * 15
    score += content_score

    # Meta/newsletter content penalty: briefing titles, front page previews,
    # and "what you need to know" digests are not actual news events
    _META_PATTERNS = [
        "here's what you need to know", "here is an early look",
        "front page of the", "get up to speed", "just published:",
        "today's briefing", "daily dose of", "here's the latest",
        "here are the major earnings", "latest news and analysis on",
        "start your day", "what's moving global markets",
    ]
    if any(pat in headline for pat in _META_PATTERNS):
        score -= 20  # Heavy penalty for meta-content

    # Recency boost (0-15): fresh news matters more
    age_hours = (time.time() - cluster.latest_timestamp) / 3600
    recency_score = max(0, 1.0 - age_hours / 48) * 15  # Decay over 48h not 168h
    score += recency_score

    return round(max(0, min(score, 100.0)), 1)


def _compute_probability_shift(price_history: list[dict]) -> float:
    """Compute 24h probability shift from price history."""
    if not price_history or len(price_history) < 2:
        return 0.0
    now = time.time()
    cutoff = now - 86400
    recent = [p for p in price_history if p.get("timestamp", 0) >= cutoff]
    if not recent:
        return 0.0
    oldest = min(recent, key=lambda p: p["timestamp"])
    newest = max(recent, key=lambda p: p["timestamp"])
    return newest.get("probability", 0) - oldest.get("probability", 0)


def impact_label_from_score(score: float, probability_shift: float = 0.0, source_count: int = 0) -> str:
    """Convert numeric impact score to label.

    Per requirements: probability shift >10% = high impact signal.
    Require at least 3 independent sources for "high" impact.
    Thresholds calibrated to produce ~15-20% high, ~30-40% medium, ~40-50% low.
    """
    if abs(probability_shift) >= 0.10 and source_count >= 3:
        return "high"
    if score >= 60 and source_count >= 3:
        return "high"
    elif score >= 32:
        return "medium"
    return "low"


def recalculate_all_impact_scores() -> int:
    """Recalculate impact scores for all clusters using current scoring formula.

    Called on startup to propagate scoring changes to existing data.
    Returns the number of clusters updated.
    """
    conn = get_db()
    try:
        rows = conn.execute(
            "SELECT id, headline, summary, source_count, confidence, market_odds, "
            "market_volume, price_history, novel_facts, disputed_claims, timeline, "
            "entities, latest_timestamp, topic, geography, social_score "
            "FROM event_clusters"
        ).fetchall()
        updated = 0
        for r in rows:
            try:
                price_history = json.loads(r["price_history"]) if r["price_history"] else []
                ec = EventCluster(
                    headline=r["headline"] or "",
                    summary=r["summary"] or "",
                    entities=json.loads(r["entities"]) if r["entities"] else [],
                    earliest_timestamp=r["latest_timestamp"] or time.time(),
                    latest_timestamp=r["latest_timestamp"] or time.time(),
                    source_count=r["source_count"] or 1,
                    confidence=r["confidence"] or 0.5,
                    market_odds=r["market_odds"],
                    market_volume=r["market_volume"],
                    price_history=price_history,
                    novel_facts=json.loads(r["novel_facts"]) if r["novel_facts"] else [],
                    disputed_claims=json.loads(r["disputed_claims"]) if r["disputed_claims"] else [],
                    timeline=json.loads(r["timeline"]) if r["timeline"] else [],
                    topic=r["topic"] or "",
                    geography=r["geography"] or "",
                    social_score=r["social_score"] or 0,
                )
                new_score = compute_impact_score(ec)
                prob_shift = _compute_probability_shift(price_history)
                new_impact = impact_label_from_score(new_score, prob_shift, source_count=ec.source_count)
                conn.execute(
                    "UPDATE event_clusters SET impact_score=?, impact=? WHERE id=?",
                    (new_score, new_impact, r["id"])
                )
                updated += 1
            except Exception:
                continue
        conn.commit()
        with _summary_cache_lock:
            _summary_cache.clear()
        return updated
    finally:
        conn.close()


def create_cluster(cluster: EventCluster, skip_llm: bool = False) -> dict:
    conn = get_db()
    try:
        # Compute impact score
        impact_score = compute_impact_score(cluster)
        prob_shift = _compute_probability_shift(cluster.price_history)
        impact = impact_label_from_score(impact_score, prob_shift, source_count=cluster.source_count)
        # Generate neutral headline — strip URLs first to prevent tracking artifacts
        clean_headline = _strip_urls(cluster.headline)
        neutral = _strip_urls(cluster.neutral_headline) if cluster.neutral_headline else ""
        if not neutral:
            if not skip_llm:
                import ollama_client
                neutral = ollama_client.generate_neutral_headline(clean_headline, cluster.summary)
            if not neutral:
                neutral = processing.generate_neutral_headline(clean_headline)

        social_score = getattr(cluster, 'social_score', 0) or 0
        conn.execute(
            """INSERT INTO event_clusters
            (id, headline, summary, entities, earliest_timestamp, latest_timestamp,
             source_count, confidence, impact, article_ids, claims, market_odds,
             market_question, price_history, market_volume, resolution_criteria,
             timeline, disputed_claims,
             novel_facts, topic, geography, impact_score, neutral_headline, social_score)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (cluster.id, cluster.headline, cluster.summary, json.dumps(cluster.entities),
             cluster.earliest_timestamp, cluster.latest_timestamp, cluster.source_count,
             cluster.confidence, impact, json.dumps(cluster.article_ids),
             json.dumps(cluster.claims), cluster.market_odds, cluster.market_question,
             json.dumps(cluster.price_history), cluster.market_volume,
             cluster.resolution_criteria,
             json.dumps(cluster.timeline), json.dumps(cluster.disputed_claims),
             json.dumps(cluster.novel_facts), cluster.topic, cluster.geography, impact_score,
             neutral, social_score)
        )
        conn.commit()
        row = conn.execute("SELECT * FROM event_clusters WHERE id=?", (cluster.id,)).fetchone()
        return _row_to_cluster(row)
    finally:
        conn.close()


def find_clusters_by_entities(entities: list[str], time_range_seconds: int = 86400, limit: int = 20) -> list[dict]:
    """Find recent clusters that share entities with the given list.

    Uses LIKE matching on the JSON entities column for each entity.
    Returns clusters sorted by latest_timestamp descending.
    """
    if not entities:
        return []

    conn = get_db()
    try:
        cutoff = time.time() - time_range_seconds

        # Use the top 5 most distinctive entities (longer names = more distinctive)
        top_entities = sorted(entities, key=len, reverse=True)[:5]
        entity_conditions = []
        params: list = [cutoff]
        for ent in top_entities:
            entity_conditions.append("entities LIKE ? ESCAPE '\\'")
            # Escape LIKE wildcards in entity values to prevent pattern injection
            safe_ent = ent.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
            params.append(f"%{safe_ent}%")

        entity_where = " OR ".join(entity_conditions)
        rows = conn.execute(f"""
            SELECT *
            FROM event_clusters
            WHERE latest_timestamp >= ? AND ({entity_where})
            ORDER BY latest_timestamp DESC
            LIMIT ?
        """, (*params, limit)).fetchall()

        return [_row_to_cluster(r) for r in rows]
    finally:
        conn.close()


def get_clusters(
    time_range: str = "24h",
    impact: Optional[str] = None,
    min_sources: int = 1,
    keyword: Optional[str] = None,
    market_moving: bool = False,
    custom_start: Optional[float] = None,
    custom_end: Optional[float] = None,
    topic: Optional[str] = None,
    geography: Optional[str] = None,
    limit: int = 100,
    offset: int = 0,
) -> list[dict]:
    conn = get_db()
    try:
        conditions = []
        params: list = []

        # Time range filter
        now = time.time()
        if time_range == "custom" and custom_start is not None:
            conditions.append("latest_timestamp >= ?")
            params.append(custom_start)
            if custom_end is not None:
                conditions.append("latest_timestamp <= ?")
                params.append(custom_end)
        else:
            range_map = {"1h": 3600, "6h": 21600, "24h": 86400, "7d": 604800}
            seconds = range_map.get(time_range, 86400)
            conditions.append("latest_timestamp >= ?")
            params.append(now - seconds)

        if impact:
            conditions.append("impact = ?")
            params.append(impact)

        if min_sources > 1:
            conditions.append("source_count >= ?")
            params.append(min_sources)

        if keyword:
            conditions.append("(headline LIKE ? ESCAPE '\\' OR summary LIKE ? ESCAPE '\\' OR entities LIKE ? ESCAPE '\\')")
            # Escape LIKE wildcards in user input to prevent pattern injection
            safe_kw = keyword.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
            kw = f"%{safe_kw}%"
            params.extend([kw, kw, kw])

        if market_moving:
            conditions.append("market_odds IS NOT NULL")

        if topic == "Market & Economy":
            # Special composite filter: Economy topic OR has market data
            conditions.append("(topic = 'Economy' OR market_odds IS NOT NULL)")
        elif topic:
            conditions.append("topic = ?")
            params.append(topic)

        if geography:
            conditions.append("geography = ?")
            params.append(geography)

        where = " AND ".join(conditions) if conditions else "1=1"
        # Sort by impact tier (high > medium > low), then recency within each tier.
        # Within high tier, boost recent and market/economy events.
        # Ensure all three tiers are represented per requirements.
        recent_threshold = now - 21600  # 6 hours

        # If no specific impact filter is set, use tiered fetch to guarantee
        # all three tiers are represented per requirements.
        if not impact:
            tier_order = """
                latest_timestamp DESC,
                COALESCE(impact_score, 0) DESC
            """
            total_needed = limit + offset
            # Reserve minimum slots: 10% for low tier, rest proportional
            min_low = min(max(2, total_needed // 10), total_needed // 3)
            # Fetch each tier separately
            high_q = f"SELECT * FROM event_clusters WHERE {where} AND impact = 'high' ORDER BY {tier_order} LIMIT ?"
            med_q = f"SELECT * FROM event_clusters WHERE {where} AND impact = 'medium' ORDER BY {tier_order} LIMIT ?"
            low_q = f"SELECT * FROM event_clusters WHERE {where} AND impact = 'low' ORDER BY {tier_order} LIMIT ?"

            high_rows = conn.execute(high_q, params + [total_needed]).fetchall()
            med_rows = conn.execute(med_q, params + [total_needed]).fetchall()
            low_rows = conn.execute(low_q, params + [total_needed]).fetchall()

            # Allocate: high gets what it needs, low gets its reserved min,
            # medium fills the rest
            n_high = min(len(high_rows), total_needed)
            n_low = min(len(low_rows), min_low)
            n_med = min(len(med_rows), total_needed - n_high - n_low)
            # If medium didn't fill its share, give extra to low
            remaining = total_needed - n_high - n_med - n_low
            if remaining > 0 and len(low_rows) > n_low:
                n_low = min(len(low_rows), n_low + remaining)

            all_rows = list(high_rows[:n_high]) + list(med_rows[:n_med]) + list(low_rows[:n_low])
            rows = all_rows[offset:offset + limit]
        else:
            query = f"""SELECT * FROM event_clusters WHERE {where}
                ORDER BY
                    CASE WHEN impact = 'high' AND (latest_timestamp >= ?) THEN 0
                         WHEN impact = 'high' AND (market_odds IS NOT NULL OR topic = 'Economy') THEN 1
                         WHEN impact = 'high' THEN 2
                         WHEN impact = 'medium' AND (latest_timestamp >= ?) THEN 3
                         WHEN impact = 'medium' THEN 4
                         WHEN impact = 'low' AND (latest_timestamp >= ?) THEN 5
                         ELSE 6 END,
                    latest_timestamp DESC,
                    COALESCE(impact_score, 0) DESC
                LIMIT ? OFFSET ?"""
            params.extend([recent_threshold, recent_threshold, recent_threshold, limit, offset])
            rows = conn.execute(query, params).fetchall()

        # Attach source_url: primary (earliest) article URL for every event
        clusters = [_row_to_cluster(r) for r in rows]
        cluster_ids = [c["id"] for c in clusters]
        if cluster_ids:
            placeholders = ",".join("?" for _ in cluster_ids)
            url_rows = conn.execute(
                f"SELECT cluster_id, url FROM articles WHERE cluster_id IN ({placeholders}) AND url IS NOT NULL AND url != '' ORDER BY timestamp ASC",
                cluster_ids,
            ).fetchall()
            url_map: dict[str, str] = {}
            for r in url_rows:
                if r["cluster_id"] not in url_map:
                    url_map[r["cluster_id"]] = r["url"]
            for c in clusters:
                if c["id"] in url_map:
                    c["source_url"] = url_map[c["id"]]

        # Group similar topic clusters using embeddings + SimHash/Jaccard
        clusters = _group_similar_clusters(clusters)

        return clusters
    finally:
        conn.close()


# AI-related keywords for filtering (used with SQL LIKE %keyword%)
# IMPORTANT: Short terms like "TPU" match inside words (e.g. "outPUt"), so
# only include terms that are unambiguous as substrings.
AI_KEYWORDS = [
    "artificial intelligence", "machine learning", "deep learning", "neural network",
    "large language model", "LLM", "generative AI", "gen AI",
    # Companies — unambiguous AI company names
    "OpenAI", "Anthropic", "DeepMind", "Google AI", "Meta AI", "Microsoft AI",
    "xAI", "Mistral AI", "Stability AI", "Hugging Face",
    # Products — unambiguous ones kept broad, ambiguous ones narrowed
    "ChatGPT", "GPT-4", "GPT-5", "Claude AI", "LLaMA", "GitHub Copilot",
    # Infrastructure — only AI-specific context
    "AI chip", "AI data center", "Google TPU",
    # Power/energy for AI
    "nuclear power AI", "energy AI", "power consumption AI",
    # Open source AI
    "open source AI", "open-source model", "open weight",
    # AI regulation/policy
    "AI regulation", "AI safety", "AI policy", "AI governance",
    # Robotics / automation
    "AI robot", "humanoid robot", "autonomous vehicle AI",
]

# Phrases that tend to indicate hype rather than substance
AI_HYPE_PHRASES = [
    "will destroy", "will replace all", "end of humanity", "singularity is here",
    "terrifying", "mind-blowing", "you won't believe",
]


def get_ai_clusters(
    time_range: str = "24h",
    impact: Optional[str] = None,
    custom_start: Optional[float] = None,
    custom_end: Optional[float] = None,
    limit: int = 100,
    offset: int = 0,
) -> list[dict]:
    """Get event clusters related to AI, filtering out hype/sensationalism."""
    conn = get_db()
    try:
        conditions = []
        params: list = []

        # Time range filter
        now = time.time()
        if time_range == "custom" and custom_start is not None:
            conditions.append("latest_timestamp >= ?")
            params.append(custom_start)
            if custom_end is not None:
                conditions.append("latest_timestamp <= ?")
                params.append(custom_end)
        else:
            range_map = {"1h": 3600, "6h": 21600, "24h": 86400, "7d": 604800}
            seconds = range_map.get(time_range, 86400)
            conditions.append("latest_timestamp >= ?")
            params.append(now - seconds)

        if impact:
            conditions.append("impact = ?")
            params.append(impact)

        # AI keyword matching — match any keyword in headline, summary, or entities
        keyword_conditions = []
        for kw in AI_KEYWORDS:
            safe_kw = kw.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
            pattern = f"%{safe_kw}%"
            keyword_conditions.append(
                "(headline LIKE ? ESCAPE '\\' OR summary LIKE ? ESCAPE '\\' OR entities LIKE ? ESCAPE '\\')"
            )
            params.extend([pattern, pattern, pattern])
        if keyword_conditions:
            conditions.append(f"({' OR '.join(keyword_conditions)})")

        where = " AND ".join(conditions) if conditions else "1=1"
        recent_threshold = now - 21600
        # Fetch more candidates than requested since many get filtered by heuristic
        fetch_limit = max(limit * 5, 500)
        query = f"""SELECT * FROM event_clusters WHERE {where}
            ORDER BY
                CASE WHEN ai_relevant = 1 THEN 0 ELSE 1 END,
                CASE WHEN impact = 'high' AND (latest_timestamp >= ?) THEN 0
                     WHEN impact = 'high' THEN 1
                     WHEN impact = 'medium' AND (latest_timestamp >= ?) THEN 2
                     WHEN impact = 'medium' THEN 3
                     WHEN impact = 'low' AND (latest_timestamp >= ?) THEN 4
                     ELSE 5 END,
                latest_timestamp DESC,
                COALESCE(impact_score, 0) DESC
            LIMIT ? OFFSET ?"""
        params.extend([recent_threshold, recent_threshold, recent_threshold, fetch_limit, offset])

        rows = conn.execute(query, params).fetchall()
        clusters = [_row_to_cluster(r) for r in rows]

        # Filter out hype and non-AI content using LLM classification
        import ollama_client
        filtered = []
        uncached = []
        for c in clusters:
            headline = (c.get("neutral_headline") or c.get("headline") or "").lower()
            is_hype = any(phrase in headline for phrase in AI_HYPE_PHRASES)
            if is_hype:
                continue

            # Check cached LLM verdict first
            if c.get("ai_relevant") is not None:
                if c["ai_relevant"] == 1:
                    filtered.append(c)
                continue

            uncached.append(c)

        # Classify uncached clusters via fast heuristic and cache results
        for c in uncached:
            verdict = ollama_client.classify_ai_relevance(
                c.get("neutral_headline") or c.get("headline") or "",
                c.get("summary") or "",
            )
            # Cache the verdict in the database
            try:
                conn.execute(
                    "UPDATE event_clusters SET ai_relevant = ? WHERE id = ?",
                    (1 if verdict else 0, c["id"]),
                )
                conn.commit()
            except Exception as e:
                logger.error("Failed to cache ai_relevant for cluster %s: %s", c["id"], e)
            if verdict:
                filtered.append(c)

        # Attach source URLs
        cluster_ids = [c["id"] for c in filtered]
        if cluster_ids:
            placeholders = ",".join("?" for _ in cluster_ids)
            url_rows = conn.execute(
                f"SELECT cluster_id, url FROM articles WHERE cluster_id IN ({placeholders}) AND url IS NOT NULL AND url != '' ORDER BY timestamp ASC",
                cluster_ids,
            ).fetchall()
            url_map: dict[str, str] = {}
            for r in url_rows:
                if r["cluster_id"] not in url_map:
                    url_map[r["cluster_id"]] = r["url"]
            for c in filtered:
                if c["id"] in url_map:
                    c["source_url"] = url_map[c["id"]]

        # Group similar clusters to reduce duplication on the AI page
        grouped = _group_similar_ai_clusters(filtered)

        # Apply the original requested limit after filtering
        return grouped[:limit]
    finally:
        conn.close()


def _group_similar_ai_clusters(clusters: list[dict]) -> list[dict]:
    """Group AI clusters with similar headlines to reduce duplication.

    Uses dual-signal matching: SimHash similarity on headlines plus word Jaccard.
    Group if SimHash >= 0.75 (strong structural match) OR
    (SimHash >= 0.65 AND word Jaccard >= 0.30) (moderate match with shared vocabulary).
    The representative is the one with highest impact + most recent timestamp.
    """
    if not clusters:
        return []
    if len(clusters) == 1:
        c = clusters[0].copy()
        c["related_stories"] = []
        return [c]

    import processing

    impact_rank = {"high": 0, "medium": 1, "low": 2}

    # Precompute SimHash and word sets for each cluster headline
    # Strip URLs to prevent tracking params from inflating Jaccard scores
    hashes = {}
    word_sets: dict[str, set[str]] = {}
    centroids: dict[str, list] = {}
    headlines_for_embed: dict[str, str] = {}
    for c in clusters:
        headline = _strip_urls(
            (c.get("neutral_headline") or c.get("headline") or "").strip().lower()
        )
        hashes[c["id"]] = processing.simhash(headline)
        word_sets[c["id"]] = set(re.findall(r'\b\w+\b', headline))
        centroid = processing.vector_store.get_centroid(c["id"])
        if centroid:
            centroids[c["id"]] = centroid
        else:
            headlines_for_embed[c["id"]] = headline

    # Compute embeddings on-the-fly for clusters without cached centroids
    if headlines_for_embed:
        import ollama_client
        ids_to_embed = list(headlines_for_embed.keys())
        texts_to_embed = [headlines_for_embed[cid] for cid in ids_to_embed]
        embeddings = ollama_client.embed_batch(texts_to_embed)
        if embeddings and len(embeddings) == len(ids_to_embed):
            for cid, emb in zip(ids_to_embed, embeddings):
                if emb:
                    centroids[cid] = emb

    # Stopwords to exclude from Jaccard — improves signal for content words
    _stopwords = {"the", "a", "an", "in", "on", "at", "to", "for", "of", "is", "as",
                  "and", "or", "but", "by", "with", "from", "its", "has", "have", "had",
                  "are", "was", "were", "be", "been", "will", "it", "that", "this", "s",
                  "after", "over", "about", "into", "than", "not", "no", "so", "up",
                  "out", "just", "how", "what", "when", "where", "who", "why", "which",
                  "could", "would", "should", "may", "might", "can", "do", "does", "did",
                  "new", "says", "said", "following", "amid", "during", "between"}

    # Build content word sets (excluding stopwords) for better Jaccard
    content_words: dict[str, set[str]] = {}
    for cid, ws in word_sets.items():
        content_words[cid] = {w for w in ws if w not in _stopwords and len(w) > 1}

    def _should_group(id_a: str, id_b: str) -> bool:
        cw_a, cw_b = content_words[id_a], content_words[id_b]
        cw_jaccard = (len(cw_a & cw_b) / len(cw_a | cw_b)) if (cw_a and cw_b) else 0.0

        # Primary: embedding cosine similarity (semantic match)
        # Always require word overlap to guard against centroid drift in large clusters
        if id_a in centroids and id_b in centroids:
            cos_sim = processing.cosine_similarity(centroids[id_a], centroids[id_b])
            if cos_sim >= 0.92 and cw_jaccard >= 0.15:
                return True
            if cos_sim >= 0.82 and cw_jaccard >= 0.25:
                return True
            if cos_sim < 0.70:
                return False
            return cw_jaccard >= 0.35

        # Fallback: SimHash + word Jaccard
        sim = processing.simhash_similarity(hashes[id_a], hashes[id_b])
        if not cw_a or not cw_b:
            return False
        if sim >= 0.75:
            return cw_jaccard >= 0.15
        if sim >= 0.65:
            return cw_jaccard >= 0.25
        # Low SimHash but very high content overlap (paraphrased headlines)
        if cw_jaccard >= 0.50:
            return True
        return False

    _MAX_GROUP_SIZE = 8

    # Grouping — check against representative only to prevent transitive chaining
    groups: list[list[dict]] = []

    for c in clusters:
        cid = c["id"]
        assigned = False
        for gi, group in enumerate(groups):
            if len(group) >= _MAX_GROUP_SIZE:
                continue
            if _should_group(cid, group[0]["id"]):
                group.append(c)
                assigned = True
                break
        if not assigned:
            groups.append([c])

    # Pick representative for each group and attach related stories
    result = []
    for group in groups:
        # Sort: highest impact first, then most recent
        group.sort(key=lambda c: (
            impact_rank.get(c.get("impact", "low"), 2),
            -(c.get("latest_timestamp") or 0),
        ))
        representative = group[0].copy()
        if len(group) > 1:
            # Aggregate source count across related clusters
            total_sources = sum(c.get("source_count", 0) for c in group)
            representative["source_count"] = total_sources
            representative["related_stories"] = [
                {
                    "id": c["id"],
                    "headline": c.get("neutral_headline") or c.get("headline") or "",
                    "source_count": c.get("source_count", 0),
                    "latest_timestamp": c.get("latest_timestamp"),
                    "impact": c.get("impact", "low"),
                    "source_url": c.get("source_url", ""),
                }
                for c in group[1:]
            ]
        else:
            representative["related_stories"] = []
        result.append(representative)

    # Re-sort the result by the same ordering as the input query
    result.sort(key=lambda c: (
        impact_rank.get(c.get("impact", "low"), 2),
        -(c.get("latest_timestamp") or 0),
    ))

    return result


def _group_similar_clusters(clusters: list[dict]) -> list[dict]:
    """Group news feed clusters with similar topics to reduce duplication.

    Uses embedding cosine similarity as primary signal (from vector store centroids),
    falling back to on-the-fly Ollama embeddings, then SimHash + word Jaccard.
    """
    if not clusters:
        return []
    if len(clusters) == 1:
        c = clusters[0].copy()
        c["related_stories"] = []
        return [c]

    import processing

    impact_rank = {"high": 0, "medium": 1, "low": 2}

    # Precompute SimHash and word sets for each cluster headline
    # Strip URLs from headlines before computing similarity to prevent
    # tracking params and domain names from inflating Jaccard scores.
    hashes = {}
    word_sets: dict[str, set] = {}
    centroids: dict[str, list] = {}
    headlines_for_embed: dict[str, str] = {}
    for c in clusters:
        headline = _strip_urls(
            (c.get("neutral_headline") or c.get("headline") or "").strip().lower()
        )
        hashes[c["id"]] = processing.simhash(headline)
        word_sets[c["id"]] = set(re.findall(r'\b\w+\b', headline))
        # Try to get embedding centroid from vector store
        centroid = processing.vector_store.get_centroid(c["id"])
        if centroid:
            centroids[c["id"]] = centroid
        else:
            headlines_for_embed[c["id"]] = headline

    # Compute embeddings on-the-fly for clusters without cached centroids
    if headlines_for_embed:
        import ollama_client
        ids_to_embed = list(headlines_for_embed.keys())
        texts_to_embed = [headlines_for_embed[cid] for cid in ids_to_embed]
        embeddings = ollama_client.embed_batch(texts_to_embed)
        if embeddings and len(embeddings) == len(ids_to_embed):
            for cid, emb in zip(ids_to_embed, embeddings):
                if emb:
                    centroids[cid] = emb

    # Stopwords to exclude from Jaccard for better content matching
    _stopwords = {"the", "a", "an", "in", "on", "at", "to", "for", "of", "is", "as",
                  "and", "or", "but", "by", "with", "from", "its", "has", "have", "had",
                  "are", "was", "were", "be", "been", "will", "it", "that", "this", "s",
                  "after", "over", "about", "into", "than", "not", "no", "so", "up",
                  "out", "just", "how", "what", "when", "where", "who", "why", "which",
                  "could", "would", "should", "may", "might", "can", "do", "does", "did",
                  "new", "says", "said", "following", "amid", "during", "between"}

    content_words: dict[str, set[str]] = {}
    for cid, ws in word_sets.items():
        content_words[cid] = {w for w in ws if w not in _stopwords and len(w) > 1}

    def _should_group(id_a: str, id_b: str) -> bool:
        cw_a, cw_b = content_words[id_a], content_words[id_b]
        cw_jaccard = (len(cw_a & cw_b) / len(cw_a | cw_b)) if (cw_a and cw_b) else 0.0

        # Primary: embedding cosine similarity (semantic match)
        # NOTE: Large-cluster centroids drift toward generic "news" embeddings,
        # so always require word overlap confirmation to prevent false merges.
        if id_a in centroids and id_b in centroids:
            cos_sim = processing.cosine_similarity(centroids[id_a], centroids[id_b])
            if cos_sim >= 0.92 and cw_jaccard >= 0.15:
                return True
            if cos_sim >= 0.82 and cw_jaccard >= 0.25:
                return True
            if cos_sim < 0.70:
                return False
            # Moderate range: require strong word overlap confirmation
            return cw_jaccard >= 0.35

        # Fallback: SimHash + word Jaccard
        sim = processing.simhash_similarity(hashes[id_a], hashes[id_b])
        if sim >= 0.75:
            return cw_jaccard >= 0.15
        if sim >= 0.65:
            return cw_jaccard >= 0.25
        # Low SimHash but very high content overlap (paraphrased)
        if cw_jaccard >= 0.50:
            return True
        return False

    _MAX_GROUP_SIZE = 8

    # Grouping — check against representative only to prevent transitive chaining
    groups: list[list[dict]] = []

    for c in clusters:
        cid = c["id"]
        assigned = False
        for gi, group in enumerate(groups):
            if len(group) >= _MAX_GROUP_SIZE:
                continue
            # Only check against the representative (first member) to avoid
            # transitive closure merging unrelated stories
            if _should_group(cid, group[0]["id"]):
                group.append(c)
                assigned = True
                break
        if not assigned:
            groups.append([c])

    # Pick representative for each group and attach related stories
    result = []
    for group in groups:
        group.sort(key=lambda c: (
            impact_rank.get(c.get("impact", "low"), 2),
            -(c.get("latest_timestamp") or 0),
        ))
        representative = group[0].copy()
        if len(group) > 1:
            total_sources = sum(c.get("source_count", 0) for c in group)
            representative["source_count"] = total_sources
            representative["related_stories"] = [
                {
                    "id": c["id"],
                    "headline": c.get("neutral_headline") or c.get("headline") or "",
                    "source_count": c.get("source_count", 0),
                    "latest_timestamp": c.get("latest_timestamp"),
                    "impact": c.get("impact", "low"),
                    "source_url": c.get("source_url", ""),
                }
                for c in group[1:]
            ]
        else:
            representative["related_stories"] = []
        result.append(representative)

    return result


def get_cluster(cluster_id: str) -> Optional[dict]:
    cached = _cache_get(f"cluster:{cluster_id}")
    if cached:
        return cached
    conn = get_db()
    try:
        row = conn.execute("SELECT * FROM event_clusters WHERE id=?", (cluster_id,)).fetchone()
    finally:
        conn.close()
    if row:
        result = _row_to_cluster(row)
        _cache_set(f"cluster:{cluster_id}", result)
        return result
    return None


def get_cluster_articles(cluster_id: str) -> list[dict]:
    conn = get_db()
    try:
        rows = conn.execute(
            "SELECT * FROM articles WHERE cluster_id=? ORDER BY timestamp ASC",
            (cluster_id,)
        ).fetchall()
        articles = [_row_to_article(r) for r in rows]

        # Attach unique claims count per article (single query instead of N+1)
        if articles:
            article_ids = [a["id"] for a in articles]
            placeholders = ",".join("?" * len(article_ids))
            claim_counts = conn.execute(
                f"SELECT source_article_id, COUNT(*) as cnt FROM claims WHERE source_article_id IN ({placeholders}) GROUP BY source_article_id",
                article_ids
            ).fetchall()
            counts_map = {row[0]: row[1] for row in claim_counts}
            for article in articles:
                article["unique_claims"] = counts_map.get(article["id"], 0)

        return articles
    finally:
        conn.close()


def get_existing_article_url_hashes() -> set[str]:
    """Return MD5 hashes of all existing article URLs for pre-seeding the ingestion seen-cache."""
    conn = get_db()
    try:
        rows = conn.execute(
            "SELECT url FROM articles WHERE url IS NOT NULL AND url != ''"
        ).fetchall()
        return {hashlib.md5(r["url"].encode()).hexdigest() for r in rows}
    finally:
        conn.close()


# --- Article CRUD ---

def create_article(article: Article, skip_llm: bool = False) -> dict:
    conn = get_db()
    try:
        # Skip if an article with the same URL already exists (dedup)
        if article.url and article.url.strip():
            existing = conn.execute(
                "SELECT id FROM articles WHERE url = ? LIMIT 1", (article.url,)
            ).fetchone()
            if existing:
                row = conn.execute("SELECT * FROM articles WHERE id=?", (existing["id"],)).fetchone()
                return _row_to_article(row)

        # Generate neutral title for sensational articles
        neutral_title = ""
        if article.sensationalism_score > 0.3:
            if not skip_llm:
                import ollama_client
                neutral_title = ollama_client.generate_neutral_headline(article.title, article.text[:200]) or ""
            if not neutral_title:
                neutral_title = processing.generate_neutral_headline(article.title)
        source_type = getattr(article, 'source_type', 'rss')
        social_coverage = getattr(article, 'social_coverage', 0)
        conn.execute(
            """INSERT INTO articles
            (id, title, author, publisher, timestamp, url, text, word_count,
             entities, key_sentences, fact_density, sensationalism_score, neutral_title,
             source_type, social_coverage, cluster_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (article.id, article.title, article.author, article.publisher,
             article.timestamp, article.url, article.text, article.word_count,
             json.dumps(article.entities), json.dumps(article.key_sentences),
             article.fact_density, article.sensationalism_score, neutral_title,
             source_type, social_coverage, article.cluster_id)
        )
        conn.commit()
        row = conn.execute("SELECT * FROM articles WHERE id=?", (article.id,)).fetchone()
        return _row_to_article(row)
    finally:
        conn.close()


def get_cluster_claims(cluster_id: str) -> list[dict]:
    conn = get_db()
    try:
        rows = conn.execute(
            "SELECT * FROM claims WHERE cluster_id=? ORDER BY rowid ASC",
            (cluster_id,)
        ).fetchall()
    finally:
        conn.close()
    result = []
    for r in rows:
        d = dict(r)
        for f in ("numbers", "direct_quotes"):
            if d.get(f):
                try:
                    d[f] = json.loads(d[f])
                except (json.JSONDecodeError, TypeError):
                    d[f] = []
            else:
                d[f] = []
        result.append(d)
    return result


def create_claim(claim: Claim, cluster_id: str) -> dict:
    conn = get_db()
    try:
        conn.execute(
            """INSERT INTO claims
            (id, who, what, when_occurred, where_occurred, numbers, direct_quotes,
             source_article_id, uncertainty, cluster_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (claim.id, claim.who, claim.what, claim.when, claim.where,
             json.dumps(claim.numbers), json.dumps(claim.direct_quotes),
             claim.source_article_id or None, claim.uncertainty, cluster_id)
        )
        conn.commit()
        return {"id": claim.id, "who": claim.who, "what": claim.what}
    finally:
        conn.close()


def get_probability_shift(cluster_id: str) -> Optional[dict]:
    """Get 24h probability shift for a cluster's linked market."""
    cluster = get_cluster(cluster_id)
    if not cluster or not cluster.get("price_history"):
        return None
    shift = _compute_probability_shift(cluster["price_history"])
    return {
        "cluster_id": cluster_id,
        "shift_24h": round(shift, 4),
        "is_significant": abs(shift) >= 0.10,
        "direction": "up" if shift > 0 else "down" if shift < 0 else "flat",
    }


def check_market_divergence(cluster_id: str) -> Optional[dict]:
    """Flag divergence between market odds and news sentiment.

    Simple heuristic: if impact is 'high' but market odds are low (or vice versa),
    flag divergence.
    """
    cluster = get_cluster(cluster_id)
    if not cluster or cluster.get("market_odds") is None:
        return None
    odds = cluster["market_odds"]
    impact = cluster.get("impact", "medium")
    impact_score = cluster.get("impact_score", 50)

    divergent = False
    reason = ""
    if impact == "high" and odds < 0.30:
        divergent = True
        reason = "High news impact but low market probability — market may be discounting the event"
    elif impact == "low" and odds > 0.70:
        divergent = True
        reason = "Low news impact but high market probability — market sees higher significance"
    elif impact_score and impact_score > 60 and odds < 0.25:
        divergent = True
        reason = "High computed impact score but low market odds"

    return {
        "cluster_id": cluster_id,
        "market_odds": odds,
        "impact": impact,
        "impact_score": impact_score,
        "divergent": divergent,
        "reason": reason,
    }


def update_cluster_on_article_added(cluster_id: str, article_timestamp: float, article_title: str = "", article_publisher: str = ""):
    """Update cluster source_count, latest_timestamp, confidence, social_score, and timeline when an article is added."""
    conn = get_db()
    try:
        article_count = conn.execute(
            "SELECT COUNT(*) FROM articles WHERE cluster_id=?", (cluster_id,)
        ).fetchone()[0]
        # Aggregate social coverage from all articles in cluster
        social_total = conn.execute(
            "SELECT COALESCE(SUM(social_coverage), 0) FROM articles WHERE cluster_id=? AND source_type='social'",
            (cluster_id,)
        ).fetchone()[0]
        social_article_count = conn.execute(
            "SELECT COUNT(*) FROM articles WHERE cluster_id=? AND source_type='social'",
            (cluster_id,)
        ).fetchone()[0]
        # Social score: log-scaled total coverage + bonus for multiple social sources
        social_score = 0.0
        if social_total > 0:
            social_score = min(math.log10(social_total + 1) * 20, 100) + (social_article_count * 5)
            social_score = min(social_score, 100.0)

        # Auto-populate development timeline entry for this article
        row = conn.execute("SELECT timeline FROM event_clusters WHERE id=?", (cluster_id,)).fetchone()
        timeline = json.loads(row["timeline"]) if row and row["timeline"] else []
        # Build timeline text from article info
        timeline_text = article_title[:120] if article_title else "New source added"
        if article_publisher:
            timeline_text = f"[{article_publisher}] {timeline_text}"
        timeline.append({"timestamp": article_timestamp, "text": timeline_text})
        # Sort timeline chronologically and keep last 50 entries
        timeline.sort(key=lambda t: t.get("timestamp", 0))
        timeline = timeline[-50:]

        # Get actual article IDs for this cluster
        article_id_rows = conn.execute(
            "SELECT id FROM articles WHERE cluster_id=? ORDER BY timestamp ASC",
            (cluster_id,)
        ).fetchall()
        article_ids = [r["id"] for r in article_id_rows]

        conn.execute(
            """UPDATE event_clusters SET
                source_count = ?,
                latest_timestamp = MAX(latest_timestamp, ?),
                confidence = MIN(1.0, ? * 0.1 + 0.5),
                social_score = ?,
                timeline = ?,
                article_ids = ?
            WHERE id = ?""",
            (article_count, article_timestamp, article_count, round(social_score, 1), json.dumps(timeline), json.dumps(article_ids), cluster_id)
        )
        conn.commit()
    finally:
        conn.close()
    _cache_invalidate(cluster_id)


def auto_tag_cluster(cluster_id: str, entities: list[str], text: str):
    """Auto-populate topic and geography from entities and text content."""
    conn = get_db()
    try:
        row = conn.execute("SELECT topic, geography FROM event_clusters WHERE id=?", (cluster_id,)).fetchone()
        if not row:
            return

        topic = row["topic"]
        geography = row["geography"]

        # Only auto-tag if not already set
        if not topic:
            topic = _infer_topic(entities, text)
        if not geography:
            geography = _infer_geography(entities, text)

        if topic or geography:
            conn.execute(
                "UPDATE event_clusters SET topic=?, geography=? WHERE id=?",
                (topic, geography, cluster_id)
            )
            conn.commit()
    finally:
        conn.close()


def _infer_topic(entities: list[str], text: str) -> str:
    """Infer topic from entities and text content."""
    combined = " ".join(entities).lower() + " " + text.lower()
    topic_keywords = {
        "Economy": ["fed", "federal reserve", "gdp", "inflation", "interest rate", "stock", "market", "bitcoin", "etf", "trade", "tariff", "economy", "fiscal", "monetary", "bank", "currency", "bond", "oil", "crude", "barrel", "energy price", "fuel", "earnings", "revenue", "ipo", "investor", "dividend", "s&p", "dow", "nasdaq", "wage", "employment", "recession", "rate hike", "rate cut", "treasury", "debt", "deficit", "budget", "tax", "profit", "loss", "commodity", "gold", "silver", "copper", "wheat", "corn", "natural gas", "opec", "supply chain", "retail", "housing", "mortgage", "real estate", "cpi", "ppi", "jobs report", "payroll", "unemployment", "central bank", "hedge fund", "private equity", "venture capital"],
        "Technology": ["ai", "artificial intelligence", "chip", "semiconductor", "software", "data breach", "cyber", "spacex", "battery", "ev", "tech", "robot", "quantum", "algorithm", "startup", "app", "cloud", "saas", "internet", "social media", "streaming", "5g", "autonomous", "blockchain", "crypto", "nft", "metaverse", "virtual reality", "augmented reality", "drone", "satellite", "telecom", "apple", "google", "microsoft", "amazon", "meta", "nvidia", "tesla", "openai", "anthropic"],
        "Politics": ["election", "congress", "senate", "parliament", "president", "minister", "treaty", "vote", "legislation", "policy", "diplomatic", "sanction", "war", "military", "strike", "conflict", "iran", "ceasefire", "nato", "united nations", "protest", "coup", "rebel", "terror", "refugee", "immigration", "border", "democrat", "republican", "prime minister", "governor", "mayor", "cabinet", "diplomacy", "embassy", "nuclear", "weapon", "army", "navy", "air force", "invasion", "occupation", "liberation", "geopolitics"],
        "Health": ["vaccine", "disease", "health", "hospital", "medical", "patient", "pharma", "drug", "clinical", "pandemic", "epidemic", "virus", "cancer", "surgery", "treatment", "therapy", "fda", "who", "mental health", "diagnosis", "outbreak", "infection", "mortality", "public health"],
        "Environment": ["climate", "wildfire", "hurricane", "earthquake", "flood", "emission", "carbon", "environmental", "renewable", "solar", "wind energy", "drought", "tsunami", "tornado", "volcano", "deforestation", "pollution", "biodiversity", "species", "ocean", "glacier", "temperature", "weather", "storm", "cyclone", "typhoon", "el nino", "la nina", "sea level"],
    }
    scores = {}
    for topic_name, keywords in topic_keywords.items():
        score = sum(1 for kw in keywords if kw in combined)
        if score > 0:
            scores[topic_name] = score
    if scores:
        return max(scores, key=scores.get)
    return ""


def _infer_geography(entities: list[str], text: str) -> str:
    """Infer geography from entities and text content."""
    combined = " ".join(entities).lower() + " " + text.lower()
    geo_keywords = {
        "US": ["united states", "u.s.", "us ", "washington", "california", "new york", "texas", "arizona", "congress", "senate", "federal reserve", "fda", "sec"],
        "Europe": ["eu", "european", "brussels", "london", "berlin", "paris", "uk", "britain", "germany", "france", "european parliament"],
        "Asia": ["china", "japan", "india", "tokyo", "beijing", "seoul", "korea", "taiwan", "tsmc", "toyota", "singapore"],
        "Global": ["global", "world", "international", "united nations", "who", "worldwide", "g20", "g7"],
    }
    scores = {}
    for geo, keywords in geo_keywords.items():
        score = sum(1 for kw in keywords if kw in combined)
        if score > 0:
            scores[geo] = score
    if scores:
        return max(scores, key=scores.get)
    return ""


def detect_disputed_claims(cluster_id: str):
    """Auto-detect potentially disputed claims within a cluster.

    Looks for claims with contradictory 'what' fields across different sources.
    """
    conn = get_db()
    try:
        claims = conn.execute(
            "SELECT * FROM claims WHERE cluster_id=? ORDER BY rowid ASC",
            (cluster_id,)
        ).fetchall()

        if len(claims) < 2:
            return

        disputed = []
        seen = []
        for claim in claims:
            what = claim["what"].lower().strip()
            if not what:
                continue

            for prev_what, prev_full in seen:
                # Check for contradictory signals
                contradiction_pairs = [
                    ("increase", "decrease"), ("rise", "fall"), ("gain", "loss"),
                    ("up", "down"), ("growth", "decline"), ("approve", "reject"),
                    ("confirm", "deny"), ("support", "oppose"), ("pass", "fail"),
                    ("accelerat", "decelerat"), ("improv", "worsen"),
                ]
                for pos, neg in contradiction_pairs:
                    if (pos in what and neg in prev_what) or (neg in what and pos in prev_what):
                        disputed.append({
                            "claim": claim["what"],
                            "contradiction": f"Contradicted by: {prev_full}",
                        })
                        break
            seen.append((what, claim["what"]))

        if disputed:
            existing = conn.execute(
                "SELECT disputed_claims FROM event_clusters WHERE id=?", (cluster_id,)
            ).fetchone()
            existing_list = json.loads(existing["disputed_claims"]) if existing and existing["disputed_claims"] else []
            # Don't duplicate existing disputes
            existing_claims = {d.get("claim", "") for d in existing_list}
            new_disputes = [d for d in disputed if d["claim"] not in existing_claims]
            if new_disputes:
                existing_list.extend(new_disputes)
                conn.execute(
                    "UPDATE event_clusters SET disputed_claims=? WHERE id=?",
                    (json.dumps(existing_list), cluster_id)
                )
                conn.commit()
    finally:
        conn.close()


def detect_novel_facts(cluster_id: str):
    """Detect novel facts in a cluster that are unique to few sources.

    A fact is novel if it appears in only one article within the cluster.
    """
    conn = get_db()
    try:
        claims = conn.execute(
            "SELECT what, source_article_id FROM claims WHERE cluster_id=? AND what != ''",
            (cluster_id,)
        ).fetchall()

        if not claims:
            return

        # Count how many articles mention each claim (by rough similarity)
        claim_sources = {}
        for claim in claims:
            what = claim["what"].strip()
            source = claim["source_article_id"] or "unknown"
            # Simple dedup: normalize
            key = re.sub(r'\s+', ' ', what.lower())
            if key not in claim_sources:
                claim_sources[key] = {"text": what, "sources": set()}
            claim_sources[key]["sources"].add(source)

        # Novel = appears from only 1 source
        novel = [v["text"] for v in claim_sources.values() if len(v["sources"]) == 1]

        if novel:
            existing = conn.execute(
                "SELECT novel_facts FROM event_clusters WHERE id=?", (cluster_id,)
            ).fetchone()
            existing_list = json.loads(existing["novel_facts"]) if existing and existing["novel_facts"] else []
            existing_set = {f.lower() for f in existing_list}
            new_novel = [f for f in novel if f.lower() not in existing_set]
            if new_novel:
                existing_list.extend(new_novel[:10])  # Cap at 10 novel facts
                conn.execute(
                    "UPDATE event_clusters SET novel_facts=? WHERE id=?",
                    (json.dumps(existing_list), cluster_id)
                )
                conn.commit()
    finally:
        conn.close()


def deduplicate_claims(cluster_id: str):
    """Merge and deduplicate claims across articles in a cluster.

    Removes duplicate claims that say the same thing from different sources.
    Keeps the earliest (lowest rowid) version.
    """
    conn = get_db()
    try:
        claims = conn.execute(
            "SELECT id, what FROM claims WHERE cluster_id=? AND what != '' ORDER BY rowid ASC",
            (cluster_id,)
        ).fetchall()

        if len(claims) < 2:
            return

        seen_normalized = {}
        duplicates = []
        for claim in claims:
            key = re.sub(r'\s+', ' ', claim["what"].lower().strip())
            if key in seen_normalized:
                duplicates.append(claim["id"])
            else:
                seen_normalized[key] = claim["id"]

        if duplicates:
            placeholders = ",".join("?" * len(duplicates))
            conn.execute(f"DELETE FROM claims WHERE id IN ({placeholders})", duplicates)
            conn.commit()
    finally:
        conn.close()


def get_stats() -> dict:
    conn = get_db()
    try:
        cluster_count = conn.execute("SELECT COUNT(*) FROM event_clusters").fetchone()[0]
        article_count = conn.execute("SELECT COUNT(*) FROM articles").fetchone()[0]
        claim_count = conn.execute("SELECT COUNT(*) FROM claims").fetchone()[0]
        latest = conn.execute("SELECT MAX(latest_timestamp) FROM event_clusters").fetchone()[0]
        social_article_count = conn.execute("SELECT COUNT(*) FROM articles WHERE source_type='social'").fetchone()[0]
        rss_article_count = conn.execute("SELECT COUNT(*) FROM articles WHERE source_type='rss'").fetchone()[0]
        social_publisher_count = conn.execute("SELECT COUNT(DISTINCT publisher) FROM articles WHERE source_type='social'").fetchone()[0]
        return {
            "cluster_count": cluster_count,
            "article_count": article_count,
            "claim_count": claim_count,
            "latest_update": latest,
            "social_article_count": social_article_count,
            "rss_article_count": rss_article_count,
            "social_publisher_count": social_publisher_count,
        }
    finally:
        conn.close()


def get_sources() -> list[dict]:
    """Get all unique news sources (publishers) with article counts and metadata."""
    conn = get_db()
    try:
        rows = conn.execute("""
            SELECT publisher, source_type,
                   COUNT(*) as article_count,
                   MAX(timestamp) as latest_article,
                   MIN(timestamp) as earliest_article,
                   AVG(sensationalism_score) as avg_sensationalism,
                   AVG(fact_density) as avg_fact_density
            FROM articles
            WHERE publisher != ''
            GROUP BY publisher
            ORDER BY article_count DESC
        """).fetchall()
    finally:
        conn.close()
    sources = []
    for r in rows:
        # Normalize publisher name: strip long RSS descriptions
        pub = r["publisher"]
        # Common prefixes that indicate the real name is after the separator
        generic_prefixes = {"news", "latest articles", "rss feed", "feed", "all releases",
                            "press release", "press releases", "top stories", "headlines"}
        if len(pub) > 40:
            for sep in [" - ", " -- ", " | ", ": "]:
                if sep in pub:
                    parts = pub.split(sep, 1)
                    left, right = parts[0].strip(), parts[1].strip()
                    # Use right side if left is generic prefix, otherwise use left
                    if left.lower() in generic_prefixes or left.lower().startswith("frb"):
                        pub = right
                    else:
                        pub = left
                    break
            # Strip common suffixes
            for suffix in [" RSS", " Feed", " rss", " feed"]:
                if pub.endswith(suffix):
                    pub = pub[:-len(suffix)].strip()
            # Final truncation if still too long
            if len(pub) > 60:
                pub = pub[:57] + "..."
        sources.append({
            "publisher": pub,
            "source_type": r["source_type"] or "rss",
            "article_count": r["article_count"],
            "latest_article": r["latest_article"],
            "earliest_article": r["earliest_article"],
            "avg_sensationalism": round(r["avg_sensationalism"] or 0, 3),
            "avg_fact_density": round(r["avg_fact_density"] or 0, 4),
        })
    return sources


def get_feed_publisher_mapping() -> dict[str, str]:
    """Map RSS feed URLs to publisher names using article URL domain matching.

    Returns dict of feed_url -> publisher_name.
    """
    from urllib.parse import urlparse
    conn = get_db()
    try:
        # Get distinct (publisher, url domain) pairs for RSS articles
        rows = conn.execute("""
            SELECT DISTINCT publisher, url FROM articles
            WHERE source_type = 'rss' AND url != '' AND publisher != ''
            LIMIT 5000
        """).fetchall()
    finally:
        conn.close()
    # Build domain -> publisher map (most common publisher per domain)
    from collections import Counter
    domain_publishers: dict[str, Counter] = {}
    for r in rows:
        try:
            host = urlparse(r["url"]).netloc.lower().replace("www.", "")
            if host:
                domain_publishers.setdefault(host, Counter())[r["publisher"]] += 1
        except Exception:
            continue
    domain_to_pub = {d: c.most_common(1)[0][0] for d, c in domain_publishers.items() if c}
    return domain_to_pub


def get_social_source_stats() -> list[dict]:
    """Get article counts and coverage stats for social media sources."""
    conn = get_db()
    try:
        rows = conn.execute("""
            SELECT publisher, source_type,
                   COUNT(*) as article_count,
                   COALESCE(SUM(social_coverage), 0) as total_coverage,
                   MAX(timestamp) as latest_article
            FROM articles
            WHERE source_type = 'social'
            GROUP BY publisher
            ORDER BY article_count DESC
        """).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def _clean_market_slug(slug: str) -> str:
    """Strip garbage condition-ID suffixes from Polymarket slugs.

    The Gamma API sometimes returns slugs like:
    'us-strikes-iran-by-february-28-2026-227-967-547-688-589-...'
    where the trailing numbers are condition IDs, not part of the event slug.
    """
    if not slug:
        return slug
    # Detect slugs with many trailing 3-digit number segments (condition IDs)
    # e.g. "topic-2026-227-967-547-688-589-491" → strip from first 3-digit segment
    # in a run of 5+ consecutive number-only segments
    # Strategy: find the longest trailing run of -\d{3,} segments and strip if >= 5
    segments = slug.split('-')
    # Walk backwards to find where pure-number trailing run starts
    trail_start = len(segments)
    for i in range(len(segments) - 1, -1, -1):
        if re.match(r'^\d{3,}$', segments[i]):
            trail_start = i
        else:
            break
    trailing_count = len(segments) - trail_start
    if trailing_count >= 8:
        cleaned = '-'.join(segments[:trail_start])
        return cleaned
    return slug


def _validate_market_slug(slug: str, source: str = "polymarket") -> bool:
    """Validate that a market slug looks correct for its source platform.

    Filters out empty, placeholder, or malformed slugs before display.
    """
    if not slug or not isinstance(slug, str):
        return False
    slug = slug.strip()
    if len(slug) < 2 or len(slug) > 120:
        return False
    # Must be alphanumeric with hyphens (standard URL slug format)
    if not re.match(r'^[a-zA-Z0-9][a-zA-Z0-9\-_]*[a-zA-Z0-9]$', slug) and len(slug) > 1:
        # Single char slugs or IDs are also OK
        if not re.match(r'^[a-zA-Z0-9\-_]+$', slug):
            return False
    # Reject slugs with too many consecutive number-only segments (condition IDs)
    number_segments = re.findall(r'-(\d{3,})', slug)
    if len(number_segments) >= 5:
        return False
    return True


def upsert_polymarket_bet(market_dict: dict) -> dict:
    """Insert or update a Polymarket bet record. Detect if it's unusual.

    Validates the market slug before storing — bets with invalid slugs are
    skipped to ensure displayed links resolve to real event pages.
    """
    import uuid
    conn = get_db()
    try:
        question = market_dict.get("question", "")
        slug = _clean_market_slug(market_dict.get("slug", ""))
        source = market_dict.get("source", "polymarket")

        # Validate slug before storing — ensures links will work
        if not _validate_market_slug(slug, source):
            return {}

        # Check if this market already exists by slug or question
        existing = None
        if slug:
            existing = conn.execute("SELECT * FROM polymarket_bets WHERE slug = ?", (slug,)).fetchone()
        if not existing and question:
            existing = conn.execute("SELECT * FROM polymarket_bets WHERE question = ?", (question,)).fetchone()

        volume = float(market_dict.get("volume", 0) or 0)
        volume_24h = float(market_dict.get("volume_24h", 0) or 0)
        probability = float(market_dict.get("probability", 0) or 0)

        # Determine if this bet is unusual
        is_unusual = False
        unusual_reason = ""

        # High total volume threshold (>$5M)
        if volume > 5_000_000:
            is_unusual = True
            unusual_reason = f"High total volume: ${volume/1_000_000:.1f}M"

        # High 24h volume (>$500K in last 24h)
        if volume_24h > 500_000:
            is_unusual = True
            reason = f"High 24h volume: ${volume_24h/1_000_000:.1f}M"
            unusual_reason = f"{unusual_reason}; {reason}" if unusual_reason else reason

        # Extreme probability (very likely or very unlikely but high volume)
        if (probability > 0.95 or probability < 0.05) and volume > 1_000_000:
            is_unusual = True
            reason = f"Extreme odds ({probability*100:.0f}%) with significant volume"
            unusual_reason = f"{unusual_reason}; {reason}" if unusual_reason else reason

        now = time.time()
        if existing:
            # Check for volume spike (>2x previous volume_24h)
            prev_24h = existing["volume_24h"] if existing["volume_24h"] else 0
            if prev_24h > 0 and volume_24h > prev_24h * 2 and volume_24h > 100_000:
                is_unusual = True
                reason = f"Volume spike: {volume_24h/prev_24h:.1f}x increase in 24h volume"
                unusual_reason = f"{unusual_reason}; {reason}" if unusual_reason else reason

            conn.execute("""UPDATE polymarket_bets SET
                probability=?, volume=?, volume_24h=?, timestamp=?,
                is_unusual=?, unusual_reason=?, resolution_criteria=?, end_date=?
                WHERE id=?""",
                (probability, volume, volume_24h, now,
                 1 if is_unusual else 0, unusual_reason,
                 market_dict.get("resolution_criteria", ""),
                 market_dict.get("end_date", ""),
                 existing["id"]))
            conn.commit()
            bet_id = existing["id"]
        else:
            bet_id = uuid.uuid4().hex[:12]
            conn.execute("""INSERT INTO polymarket_bets
                (id, question, probability, volume, volume_24h, resolution_criteria,
                 slug, end_date, timestamp, is_unusual, unusual_reason, source)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (bet_id, question, probability, volume, volume_24h,
                 market_dict.get("resolution_criteria", ""),
                 slug, market_dict.get("end_date", ""), now,
                 1 if is_unusual else 0, unusual_reason, source))
            conn.commit()

        result = conn.execute("SELECT * FROM polymarket_bets WHERE id=?", (bet_id,)).fetchone()
        return dict(result) if result else {}
    finally:
        conn.close()


def _enrich_bet_with_url(bet: dict) -> dict:
    """Add a validated 'url' field to a bet dict.

    Cleans garbage suffixes from slugs and only includes the URL if the
    slug passes validation, ensuring displayed links resolve to real event pages.
    """
    slug = _clean_market_slug(bet.get("slug", ""))
    source = bet.get("source", "polymarket")
    if slug and _validate_market_slug(slug, source):
        if source == "callsheet":
            bet["url"] = f"https://callsheet.com/event/{slug}"
        elif source == "kalshi":
            bet["url"] = f"https://kalshi.com/markets/{slug}"
        else:
            bet["url"] = f"https://polymarket.com/event/{slug}"
    else:
        bet["url"] = ""
    return bet


def _is_expired_bet(bet: dict) -> bool:
    """Check if a bet's end_date is in the past."""
    end_date = bet.get("end_date", "")
    if not end_date:
        return False
    try:
        from datetime import datetime, timezone
        # Handle ISO format like "2026-01-31T00:00:00Z"
        dt = datetime.fromisoformat(end_date.replace("Z", "+00:00"))
        return dt < datetime.now(timezone.utc)
    except (ValueError, TypeError):
        return False


def get_unusual_bets(limit: int = 20) -> list[dict]:
    """Get unusual Polymarket bets sorted by volume.

    Only returns bets with validated slugs to ensure links resolve.
    Filters out expired bets (end_date in the past).
    Each bet includes a pre-validated 'url' field.
    """
    conn = get_db()
    try:
        rows = conn.execute("""
            SELECT * FROM polymarket_bets
            WHERE is_unusual = 1 AND slug IS NOT NULL AND slug != ''
              AND probability > 0.05 AND probability < 0.95
            ORDER BY volume_24h DESC, volume DESC
            LIMIT ?
        """, (limit * 2,)).fetchall()
    finally:
        conn.close()
    results = [_enrich_bet_with_url(dict(r)) for r in rows
               if _validate_market_slug(r["slug"]) and not _is_expired_bet(dict(r))]
    return results[:limit]


def get_all_bets(limit: int = 50) -> list[dict]:
    """Get all tracked Polymarket bets sorted by volume.

    Only returns bets with validated slugs to ensure links resolve.
    Filters out expired bets (end_date in the past).
    Each bet includes a pre-validated 'url' field.
    """
    conn = get_db()
    try:
        rows = conn.execute("""
            SELECT * FROM polymarket_bets
            WHERE slug IS NOT NULL AND slug != ''
              AND probability > 0.05 AND probability < 0.95
            ORDER BY volume DESC
            LIMIT ?
        """, (limit * 2,)).fetchall()
    finally:
        conn.close()
    results = [_enrich_bet_with_url(dict(r)) for r in rows
               if _validate_market_slug(r["slug"]) and not _is_expired_bet(dict(r))]
    return results[:limit]


def get_social_vs_traditional_gaps(limit: int = 20) -> list[dict]:
    """Find gaps between social media and traditional news coverage.

    Returns clusters where one side has significantly more coverage than the other,
    sorted by gap_score descending. Each gap includes impact and importance ratings.
    """
    conn = get_db()
    try:
        cutoff = time.time() - 86400 * 2  # look back 48h to catch recently expired

        rows = conn.execute("""
            SELECT
                ec.id AS cluster_id,
                ec.headline,
                ec.neutral_headline,
                ec.entities,
                ec.topic,
                ec.impact,
                ec.impact_score,
                ec.source_count,
                ec.latest_timestamp,
                ec.market_odds,
                ec.market_volume,
                COUNT(DISTINCT CASE WHEN a.source_type = 'social'
                    AND a.sensationalism_score < 0.5
                    THEN CASE WHEN a.url != '' THEN a.url ELSE a.id END
                    END) AS social_count,
                COUNT(CASE WHEN a.source_type = 'social'
                    AND a.sensationalism_score < 0.5
                    THEN 1 END) AS social_article_rows,
                COUNT(CASE WHEN a.source_type IN ('rss', 'api', 'scrape') THEN 1 END) AS traditional_count,
                COALESCE(SUM(CASE WHEN a.source_type = 'social'
                    AND a.sensationalism_score < 0.5
                    THEN a.social_coverage ELSE 0 END), 0) AS social_coverage
            FROM event_clusters ec
            JOIN articles a ON a.cluster_id = ec.id
            WHERE a.timestamp >= ?
            GROUP BY ec.id
            HAVING social_count + traditional_count > 0
        """, (cutoff,)).fetchall()
    finally:
        conn.close()

    gaps = []
    for r in rows:
        social_count = r["social_count"]  # distinct URLs
        social_article_rows = r["social_article_rows"]  # total rows (may include dupes)
        traditional_count = r["traditional_count"]
        social_coverage = r["social_coverage"]

        # Disambiguate: one viral post vs multiple independent posts
        if social_count <= 1 and social_article_rows > 1:
            social_signal_type = "viral"  # same post scraped multiple times
        elif social_count > 1:
            social_signal_type = "multiple"  # distinct posts about same topic
        else:
            social_signal_type = "single"

        # Determine gap type and score
        # Both sides require >1 source to be considered a significant gap
        if social_count > 1 and traditional_count == 0:
            gap_type = "social_leading"
            gap_score = 1.0
        elif social_count == 1 and traditional_count == 0:
            # Single social posting is not significant enough
            continue
        elif traditional_count > 0 and social_count == 0:
            # User feedback: don't need "covered by traditional not covered by social" section
            continue
        elif social_count == 0 and traditional_count == 0:
            continue
        else:
            # Both sides have coverage — only a real gap if one side has
            # significantly more sources (at least 3× the other side).
            # Engagement metrics (social_coverage) are used as a tiebreaker
            # but source count ratio is the primary signal.
            if social_count >= 3 * traditional_count and social_count > 1:
                gap_type = "social_leading"
                ratio = social_count / max(traditional_count, 1)
                gap_score = min(1.0, (ratio - 1) / (ratio + 1))
            else:
                # Roughly equal coverage — not a meaningful gap
                continue

        entities = []
        try:
            entities = json.loads(r["entities"]) if r["entities"] else []
        except (json.JSONDecodeError, TypeError):
            pass

        # Compute impact and importance ratings based on gap characteristics
        # For gaps, the divergence itself is a signal — a pure gap (one side = 0)
        # with any meaningful coverage is high-impact by definition.
        impact_score = r["impact_score"] or 0
        source_count = r["source_count"] or 0
        cluster_impact = r["impact"] or "low"
        total_sources = social_count + traditional_count
        coverage_signal = math.log1p(social_coverage)

        # Importance: based on gap_score, source count, and coverage magnitude
        # Require meaningful coverage (multiple sources) for high importance
        importance_score = gap_score * 0.3 + min(total_sources / 5, 1.0) * 0.4 + min(coverage_signal / 5, 1.0) * 0.3

        if importance_score >= 0.5:
            importance = "high"
        elif importance_score >= 0.25:
            importance = "medium"
        else:
            importance = "low"

        # Impact: factor in cluster impact, source count, market data, and gap score.
        # A strong gap with meaningful coverage is high-impact by definition.
        has_market = r["market_odds"] and r["market_volume"] and r["market_volume"] > 100000
        if (cluster_impact in ("high", "medium") and total_sources >= 3) or impact_score >= 42 or has_market or total_sources >= 5 or (gap_score >= 0.7 and total_sources >= 2):
            impact = "high"
        elif cluster_impact in ("high", "medium") or impact_score >= 20 or total_sources >= 3 or gap_score >= 0.4:
            impact = "medium"
        else:
            impact = "low"

        # Use neutral headline if available, fall back to raw headline
        # Strip embedded URLs from headlines (ingestion artifacts)
        display_headline = r["neutral_headline"] or r["headline"] or ""
        display_headline = re.sub(r'https?://\S+', '', display_headline).strip()
        # Collapse multiple spaces left after URL removal
        display_headline = re.sub(r'\s{2,}', ' ', display_headline)
        # Truncate overly long headlines (social media snippets)
        if len(display_headline) > 120:
            display_headline = display_headline[:117].rsplit(' ', 1)[0] + '...'

        # Filter out discussion questions and non-newsworthy content
        # These are Reddit/forum posts that aren't real news gaps
        headline_lower = display_headline.lower()
        question_patterns = [
            headline_lower.startswith("how "), headline_lower.startswith("why "),
            headline_lower.startswith("what "), headline_lower.startswith("anyone "),
            headline_lower.startswith("does "), headline_lower.startswith("is it "),
            headline_lower.startswith("can you "), headline_lower.startswith("should "),
            "?" in headline_lower and social_count <= 3 and traditional_count == 0,
        ]
        if any(question_patterns) and traditional_count == 0:
            continue

        gaps.append({
            "cluster_id": r["cluster_id"],
            "headline": display_headline,
            "social_count": social_count,
            "traditional_count": traditional_count,
            "social_coverage": social_coverage,
            "social_signal_type": social_signal_type,
            "gap_type": gap_type,
            "gap_score": round(gap_score, 4),
            "entities": entities,
            "topic": r["topic"] or "",
            "impact": impact,
            "importance": importance,
            "latest_timestamp": r["latest_timestamp"],
        })

    # Split into high-priority and lower-priority for the frontend
    # Per user feedback: "only high-importance + high-impact items" in the main section
    high_gaps = [g for g in gaps if
                 g["impact"] == "high" and g["importance"] == "high"]
    high_gap_ids = {id(g) for g in high_gaps}
    lower_gaps = [g for g in gaps if id(g) not in high_gap_ids]
    high_gaps.sort(key=lambda g: g["gap_score"], reverse=True)
    lower_gaps.sort(key=lambda g: g["gap_score"], reverse=True)
    return {"high": high_gaps[:limit], "lower": lower_gaps[:limit]}


def _merge_clusters_into(conn, primary_id: str, dup_ids: list[str]):
    """Merge duplicate cluster IDs into a primary cluster.

    Reassigns articles, claims, and bets from dup_ids to primary_id,
    deletes the duplicate clusters, and updates primary metadata.
    """
    for dup_id in dup_ids:
        conn.execute("UPDATE articles SET cluster_id = ? WHERE cluster_id = ?", (primary_id, dup_id))
        conn.execute("UPDATE claims SET cluster_id = ? WHERE cluster_id = ?", (primary_id, dup_id))
        conn.execute("UPDATE polymarket_bets SET linked_cluster_id = ? WHERE linked_cluster_id = ?", (primary_id, dup_id))
        conn.execute("DELETE FROM event_clusters WHERE id = ?", (dup_id,))


def _refresh_cluster_metadata(conn, cluster_id: str):
    """Refresh source_count, article_ids, and timestamps for a cluster after merge."""
    count = conn.execute("SELECT COUNT(*) FROM articles WHERE cluster_id=?", (cluster_id,)).fetchone()[0]
    article_id_rows = conn.execute(
        "SELECT id FROM articles WHERE cluster_id=? ORDER BY timestamp ASC", (cluster_id,)
    ).fetchall()
    article_ids = [r["id"] for r in article_id_rows]
    earliest = conn.execute(
        "SELECT MIN(timestamp) FROM articles WHERE cluster_id=?", (cluster_id,)
    ).fetchone()[0]
    latest = conn.execute(
        "SELECT MAX(timestamp) FROM articles WHERE cluster_id=?", (cluster_id,)
    ).fetchone()[0]
    conn.execute(
        """UPDATE event_clusters SET source_count = ?, article_ids = ?,
           earliest_timestamp = COALESCE(?, earliest_timestamp),
           latest_timestamp = COALESCE(?, latest_timestamp)
        WHERE id = ?""",
        (count, json.dumps(article_ids), earliest, latest, cluster_id)
    )


def merge_duplicate_clusters():
    """Retroactively merge clusters with duplicate or near-duplicate headlines.

    Three-phase approach:
    1. Exact headline merge: GROUP BY normalized headline across ALL clusters
    2. SimHash near-duplicate merge: band-bucketing for efficient candidate discovery
    3. Orphan cleanup: delete clusters with source_count=0 and no linked articles
    """
    conn = get_db()
    try:
        total_merged = 0

        # Phase 1: Exact headline merge across ALL clusters
        # Group by normalized (lowered, trimmed) headline
        dup_groups = conn.execute("""
            SELECT LOWER(TRIM(headline)) as norm_headline,
                   GROUP_CONCAT(id) as ids,
                   MIN(earliest_timestamp) as min_ts
            FROM event_clusters
            GROUP BY LOWER(TRIM(headline))
            HAVING COUNT(*) > 1
            ORDER BY COUNT(*) DESC
        """).fetchall()

        exact_merged = 0
        exact_affected: set[str] = set()
        for group in dup_groups:
            ids = group["ids"].split(",")
            if len(ids) < 2:
                continue
            # Pick the cluster with earliest timestamp as primary
            # Query to find the one with min earliest_timestamp
            primary_row = conn.execute(
                "SELECT id FROM event_clusters WHERE id IN ({}) ORDER BY earliest_timestamp ASC LIMIT 1".format(
                    ",".join("?" for _ in ids)
                ), ids
            ).fetchone()
            if not primary_row:
                continue
            primary_id = primary_row["id"]
            dup_ids = [i for i in ids if i != primary_id]
            if dup_ids:
                _merge_clusters_into(conn, primary_id, dup_ids)
                exact_affected.add(primary_id)
                exact_merged += len(dup_ids)

        if exact_merged > 0:
            conn.commit()
            for pid in exact_affected:
                _refresh_cluster_metadata(conn, pid)
            conn.commit()
            logger.info("Phase 1: Merged %d exact-duplicate clusters", exact_merged)
            total_merged += exact_merged

        # Phase 2: SimHash near-duplicate merge on remaining clusters (7-day window)
        now = time.time()
        rows = conn.execute(
            "SELECT id, headline, earliest_timestamp FROM event_clusters WHERE latest_timestamp >= ? ORDER BY earliest_timestamp ASC",
            (now - 604800,)  # 7 days
        ).fetchall()
        clusters = [dict(r) for r in rows]

        if len(clusters) >= 2:
            hashes = {}
            for c in clusters:
                hashes[c["id"]] = processing.simhash(_strip_urls(c["headline"]).strip().lower())

            NUM_BANDS = 8
            BITS_PER_BAND = 8
            bands: list[dict[int, list[str]]] = [{} for _ in range(NUM_BANDS)]
            for cid, h in hashes.items():
                for b in range(NUM_BANDS):
                    band_val = (h >> (b * BITS_PER_BAND)) & ((1 << BITS_PER_BAND) - 1)
                    bands[b].setdefault(band_val, []).append(cid)

            candidate_pairs: set[tuple[str, str]] = set()
            for band in bands:
                for bucket in band.values():
                    if len(bucket) < 2 or len(bucket) > 100:
                        continue
                    for i in range(len(bucket)):
                        for j in range(i + 1, len(bucket)):
                            pair = (bucket[i], bucket[j]) if bucket[i] < bucket[j] else (bucket[j], bucket[i])
                            candidate_pairs.add(pair)

            simhash_merged = 0
            merged_ids: set[str] = set()
            simhash_affected: set[str] = set()
            ts_lookup = {c["id"]: c["earliest_timestamp"] for c in clusters}

            for id1, id2 in candidate_pairs:
                if id1 in merged_ids or id2 in merged_ids:
                    continue
                sim = processing.simhash_similarity(hashes[id1], hashes[id2])
                if sim > 0.85:
                    if ts_lookup[id1] <= ts_lookup[id2]:
                        primary_id, dup_id = id1, id2
                    else:
                        primary_id, dup_id = id2, id1
                    _merge_clusters_into(conn, primary_id, [dup_id])
                    merged_ids.add(dup_id)
                    simhash_affected.add(primary_id)
                    simhash_merged += 1

            if simhash_merged > 0:
                conn.commit()
                for pid in simhash_affected:
                    _refresh_cluster_metadata(conn, pid)
                conn.commit()
                logger.info("Phase 2: Merged %d SimHash-duplicate clusters", simhash_merged)
                total_merged += simhash_merged

        # Phase 3: Clean up orphan clusters (no articles linked)
        orphan_deleted = conn.execute("""
            DELETE FROM event_clusters
            WHERE source_count = 0
            AND id NOT IN (SELECT DISTINCT cluster_id FROM articles WHERE cluster_id IS NOT NULL)
            AND id NOT IN (SELECT DISTINCT linked_cluster_id FROM polymarket_bets WHERE linked_cluster_id IS NOT NULL)
        """).rowcount
        if orphan_deleted > 0:
            conn.commit()
            logger.info("Phase 3: Deleted %d orphan clusters", orphan_deleted)

        return total_merged
    finally:
        conn.close()


def seed_demo_data():
    """Seed the database with demo event clusters and articles for demonstration.

    These baseline events ensure the dashboard always has high-impact, multi-source
    clusters with prediction market data, social coverage, disputed claims, novel facts,
    and development timelines. They demonstrate all three impact tiers (high/medium/low).
    Idempotent: skips if seed data already exists (checked by known headline).
    """
    conn = get_db()
    try:
        existing = conn.execute(
            "SELECT COUNT(*) FROM event_clusters WHERE headline = 'Federal Reserve holds interest rates steady at 4.25-4.50%'"
        ).fetchone()[0]
    finally:
        conn.close()
    if existing > 0:
        return

    now = time.time()

    events = [
        {
            "headline": "Federal Reserve holds interest rates steady at 4.25-4.50%",
            "summary": "The Federal Reserve announced it will maintain the federal funds rate at 4.25-4.50%, citing persistent inflation above the 2% target. Chair Powell indicated rate cuts remain possible later in 2026 if inflation data improves.",
            "entities": ["Federal Reserve", "Jerome Powell", "FOMC"],
            "impact": "high",
            "topic": "Economy",
            "geography": "US",
            "source_count": 14,
            "confidence": 0.97,
            "social_score": 85,
            "market_odds": 0.72,
            "market_question": "Will the Fed cut rates before July 2026?",
            "market_volume": 8500000,
            "resolution_criteria": "Resolves YES if the Federal Reserve announces a rate cut at any FOMC meeting before July 1, 2026.",
            "price_history": [
                {"timestamp": now - 604800, "probability": 0.61},
                {"timestamp": now - 518400, "probability": 0.63},
                {"timestamp": now - 432000, "probability": 0.65},
                {"timestamp": now - 345600, "probability": 0.64},
                {"timestamp": now - 259200, "probability": 0.67},
                {"timestamp": now - 172800, "probability": 0.69},
                {"timestamp": now - 86400, "probability": 0.68},
                {"timestamp": now - 43200, "probability": 0.70},
                {"timestamp": now - 21600, "probability": 0.71},
                {"timestamp": now - 7200, "probability": 0.72},
            ],
            "articles": [
                {"title": "Fed Keeps Rates Unchanged, Signals Patience on Cuts", "publisher": "Reuters", "author": "Howard Schneider", "url": "https://www.reuters.com/markets/us/", "source_type": "rss", "text": "The Federal Reserve held its benchmark interest rate steady at 4.25%-4.50% on Wednesday, as policymakers cited persistent inflation above their 2% target. Chair Jerome Powell said the central bank needs to see more progress on inflation before considering rate cuts. The decision was widely expected by markets. Two members dissented, preferring a 25 basis point cut."},
                {"title": "Federal Reserve holds steady, Powell cites inflation concerns", "publisher": "AP News", "author": "Christopher Rugaber", "url": "https://apnews.com/hub/federal-reserve", "source_type": "rss", "text": "The Federal Reserve kept its key interest rate unchanged Wednesday amid signs that inflation, while cooling, remains stubbornly above the Fed's 2% target. Chair Jerome Powell signaled patience, saying rate cuts remain on the table later this year if price pressures continue to ease. The fed funds rate stays at 4.25%-4.50%."},
                {"title": "BREAKING: Fed rate decision — no change", "publisher": "CNBC", "author": "Jeff Cox", "url": "https://www.cnbc.com/federal-reserve/", "source_type": "rss", "text": "The Federal Reserve on Wednesday held interest rates steady as expected, keeping the federal funds rate in a range of 4.25%-4.50%. Markets had fully priced in no change. The S&P 500 rose 0.3% following the announcement as investors parsed Powell's comments for clues about the timing of future cuts."},
                {"title": "Markets React to Fed's Wait-and-See Approach", "publisher": "Bloomberg", "author": "Craig Torres", "url": "https://www.bloomberg.com/markets", "source_type": "rss", "text": "US stocks edged higher after the Federal Reserve kept rates unchanged and Chair Jerome Powell struck a balanced tone on the inflation outlook. The S&P 500 gained 0.3% while Treasury yields dipped. Powell noted the updated dot plot shows a median expectation of two rate cuts in 2026."},
                {"title": "Fed holds rates — thread on market implications", "publisher": "Hacker News", "author": "FedWatch", "url": "https://news.ycombinator.com/news", "source_type": "social", "social_coverage": 450, "text": "Fed held rates at 4.25-4.50% as expected. Two dissents for a cut. Powell's presser indicated September as earliest possible cut date if inflation data cooperates. Dot plot shifted to median 2 cuts in 2026 from 3 previously. Market reaction muted — already priced in."},
                {"title": "Fed rate decision megathread — implications for tech hiring and VC funding", "publisher": "Reddit/r/economics", "author": "u/macro_watcher", "url": "https://www.reddit.com/r/economics/", "source_type": "social", "social_coverage": 1200, "text": "The FOMC held rates steady at 4.25-4.50%. Key takeaway: Powell signaled patience but left the door open to cuts later this year. Two dissenters wanted a cut now. This likely means continued tight conditions for startups and VC through H1 2026. Mortgage rates expected to remain elevated near 6.5%."},
                {"title": "Fed holds at 4.25-4.50%. Dot plot now shows 2 cuts in 2026. Powell says September earliest for a move.", "publisher": "Bluesky", "author": "@fedwatcher.bsky.social", "url": "https://bsky.app/", "source_type": "social", "social_coverage": 340, "text": "Fed holds at 4.25-4.50%. Dot plot now shows 2 cuts in 2026. Powell says September earliest for a move. Market was already pricing this in — no surprises. Two dissenters wanted a cut. Mortgage rates staying elevated through H1."},
                {"title": "BREAKING: Fed holds at 4.25-4.50%. Two dissenters wanted a cut. Powell says earliest move is Sept. Dot plot shows 2 cuts in 2026, down from 3.", "publisher": "Twitter/X/@ReutersWorld", "author": "@ReutersWorld", "url": "https://x.com/ReutersWorld/status/1893456789012345678", "source_type": "social", "social_coverage": 2800, "text": "BREAKING: Fed holds at 4.25-4.50%. Two dissenters wanted a cut. Powell says earliest move is Sept. Dot plot shows 2 cuts in 2026, down from 3. Markets muted — fully priced in. Treasury yields dip slightly."},
                {"title": "Fed rate decision explained in 60 seconds — why your mortgage isn't going down yet", "publisher": "TikTok", "author": "@financeexplained", "url": "https://www.tiktok.com/@financeexplained/video/7340123456789", "source_type": "social", "social_coverage": 45000, "text": "Fed held rates at 4.25-4.50%. What does this mean for you? Your mortgage rate stays high near 6.5%. Your savings account rate stays good. The Fed wants to see more inflation progress before cutting. Powell said September is the earliest possible move. Two members wanted to cut now."},
                {"title": "Fed decision impact on markets and rates", "publisher": "Mastodon", "author": "@econbriefing@mastodon.social", "url": "https://mastodon.social/@econbriefing/111923456789", "source_type": "social", "social_coverage": 180, "text": "Fed held at 4.25-4.50% as expected. Two dissents for cut. Powell's tone balanced — signaling patience but leaving door open for Sept. Dot plot median now 2 cuts in 2026 vs 3 previously. Markets largely unmoved, already priced in. Key question remains shelter inflation."},
            ],
            "claims": [
                {"who": "Federal Reserve", "what": "Held federal funds rate at 4.25-4.50%", "when": "February 2026", "where": "Washington, D.C.", "numbers": ["4.25%", "4.50%", "2%"], "direct_quotes": ["Inflation remains above our 2 percent longer-run goal"]},
                {"who": "Jerome Powell", "what": "Stated inflation remains above 2% target", "when": "February 19, 2026", "where": "FOMC press conference", "numbers": ["2%", "0.3%"], "direct_quotes": ["We need to see more progress before adjusting rates"], "uncertainty": "According to FOMC press conference transcript"},
            ],
            "timeline": [
                {"timestamp": now - 7200, "text": "FOMC meeting begins"},
                {"timestamp": now - 3600, "text": "Rate decision announced: no change"},
                {"timestamp": now - 3000, "text": "Powell press conference begins"},
                {"timestamp": now - 1800, "text": "Markets digest decision, S&P 500 up 0.3%"},
            ],
            "novel_facts": [
                "Powell hinted at a possible September rate cut if inflation drops below 2.5%",
                "Two FOMC members dissented, favoring a 25bp cut",
                "Fed updated its dot plot showing median expectation of two cuts in 2026",
            ],
            "disputed_claims": [
                {"claim": "Markets expect three rate cuts by end of 2026", "contradiction": "CME FedWatch tool shows only two cuts priced in as of February 20, per Bloomberg data"},
                {"claim": "Inflation is accelerating again", "contradiction": "CPI report shows month-over-month decline from 3.1% to 2.9%, per Bureau of Labor Statistics"},
            ],
        },
        {
            "headline": "EU passes comprehensive AI regulation framework effective 2027",
            "summary": "The European Parliament approved the AI Act implementation rules with a 401-159 vote. The framework requires risk assessments for high-risk AI systems and bans social scoring. Compliance deadline is January 2027.",
            "entities": ["European Parliament", "EU", "AI Act"],
            "impact": "high",
            "topic": "Technology",
            "geography": "Europe",
            "source_count": 9,
            "confidence": 0.94,
            "social_score": 72,
            "articles": [
                {"title": "EU Parliament Approves AI Act Implementation Rules", "publisher": "Reuters", "author": "Foo Yun Chee", "url": "https://www.reuters.com/technology/", "source_type": "rss", "text": "The European Parliament approved implementation rules for the AI Act with a 401-159 vote on Tuesday. The framework mandates risk assessments for high-risk AI systems and bans social scoring. Companies have until January 2027 to comply. Open-source models are largely exempt."},
                {"title": "Europe's AI rules are now law — what companies need to know", "publisher": "TechCrunch", "author": "Natasha Lomas", "url": "https://techcrunch.com/category/artificial-intelligence/", "source_type": "rss", "text": "The EU AI Act implementation rules passed Parliament Tuesday. Key provisions: mandatory risk assessments for high-risk systems, bans on social scoring and real-time biometric surveillance, fines up to 7% of global revenue for non-compliance. The compliance deadline is January 2027."},
                {"title": "EU AI regulation: the key provisions explained", "publisher": "The Guardian", "author": "Dan Milmo", "url": "https://www.theguardian.com/technology/artificialintelligenceai", "source_type": "rss", "text": "The European Parliament voted 401-159 to approve the AI Act's implementation rules. The regulation creates a tiered risk framework for AI systems. High-risk applications in healthcare, law enforcement, and critical infrastructure face the strictest requirements including mandatory audits."},
                {"title": "EU AI Act passes — discussion on impact for open-source projects", "publisher": "Hacker News", "author": "ai_policy", "url": "https://news.ycombinator.com/news", "source_type": "social", "social_coverage": 890, "text": "EU AI Act implementation rules passed 401-159. Good news: open-source models are mostly exempt. Bad news: 7% revenue fines for non-compliance. Compliance deadline is Jan 2027. Biggest impact will be on companies deploying high-risk AI in healthcare and law enforcement."},
            ],
            "claims": [
                {"who": "European Parliament", "what": "Approved AI Act implementation rules with 401-159 vote", "when": "February 2026", "where": "Brussels"},
                {"who": "EU", "what": "Mandates risk assessments for high-risk AI systems", "when": "Effective January 2027", "where": "European Union"},
            ],
            "timeline": [
                {"timestamp": now - 86400, "text": "Final vote scheduled in Parliament"},
                {"timestamp": now - 43200, "text": "Vote passes 401-159"},
                {"timestamp": now - 36000, "text": "Implementation timeline published"},
            ],
            "novel_facts": [
                "Open-source AI models exempt from most compliance requirements",
                "Fines for non-compliance can reach 7% of global annual revenue",
            ],
            "disputed_claims": [
                {"claim": "The AI Act will stifle European AI innovation", "contradiction": "European AI startups raised record $4.2B in Q4 2025, per PitchBook, with founders citing regulatory clarity as an advantage"},
            ],
        },
        {
            "headline": "SpaceX Starship completes first successful orbital cargo delivery",
            "summary": "SpaceX's Starship vehicle completed its first operational cargo delivery to low Earth orbit, deploying 23 Starlink V3 satellites. The booster successfully returned to the launch tower for catch. Total mission time was 97 minutes.",
            "entities": ["SpaceX", "Starship", "Elon Musk", "Starlink"],
            "impact": "high",
            "topic": "Technology",
            "geography": "US",
            "source_count": 11,
            "confidence": 0.96,
            "social_score": 95,
            "articles": [
                {"title": "Starship Delivers First Operational Payload to Orbit", "publisher": "SpaceNews", "author": "Jeff Foust", "url": "https://spacenews.com/tag/starship/", "source_type": "rss", "text": "SpaceX's Starship completed its first operational cargo mission, deploying 23 Starlink V3 satellites into low Earth orbit. The Super Heavy booster successfully returned to the launch tower via the mechanical arm catch system. Total mission time was 97 minutes from liftoff to final satellite deployment."},
                {"title": "SpaceX achieves Starship milestone with orbital cargo run", "publisher": "Ars Technica", "author": "Eric Berger", "url": "https://arstechnica.com/space/", "source_type": "rss", "text": "SpaceX has achieved a major milestone with Starship's first operational cargo delivery. The vehicle deployed 23 next-generation Starlink V3 satellites and the booster performed a successful propulsive landing catch on its first commercial attempt. This demonstrates the vehicle's readiness for regular service."},
                {"title": "Starship completes first commercial mission", "publisher": "Reuters", "author": "Joey Roulette", "url": "https://www.reuters.com/technology/space/", "source_type": "rss", "text": "SpaceX's Starship rocket completed its first commercial cargo delivery on Wednesday, deploying a batch of Starlink satellites to orbit. The mission lasted 97 minutes. The first-stage booster returned to the launch site for a mechanical catch, a capability central to SpaceX's plans for rapid reuse."},
                {"title": "Starship launch and catch — incredible footage and technical breakdown", "publisher": "Reddit/r/space", "author": "u/space_fan", "url": "https://www.reddit.com/r/space/", "source_type": "social", "social_coverage": 12000, "text": "SpaceX Starship just completed its first operational flight. 23 Starlink V3 sats deployed, booster caught on first commercial attempt. 97 minutes total mission time. The V3 satellites are significantly larger than V2 Mini — this is why they needed Starship's payload capacity. Next mission reportedly scheduled for March."},
                {"title": "Starship operational debut — what this means for the space industry", "publisher": "Hacker News", "author": "orbital_mech", "url": "https://news.ycombinator.com/news", "source_type": "social", "social_coverage": 2100, "text": "Starship's first commercial mission is a game-changer. At roughly $10M per launch vs $67M for Falcon 9, the economics of orbital delivery just changed dramatically. 23 Starlink V3 sats deployed in one shot. Booster caught successfully. This was the vehicle's first operational flight after years of test campaigns."},
            ],
            "claims": [
                {"who": "SpaceX", "what": "Completed first operational Starship cargo delivery", "when": "February 2026", "where": "Low Earth orbit"},
                {"who": "SpaceX", "what": "Deployed 23 Starlink V3 satellites", "when": "February 2026", "where": "LEO"},
            ],
            "timeline": [
                {"timestamp": now - 14400, "text": "Launch from Starbase, Texas"},
                {"timestamp": now - 13800, "text": "Stage separation successful"},
                {"timestamp": now - 12600, "text": "Satellite deployment confirmed"},
                {"timestamp": now - 12000, "text": "Booster catch at launch tower"},
            ],
            "novel_facts": [
                "Total mission time was 97 minutes from launch to final satellite deployment",
                "Booster performed a propulsive landing catch on its first operational attempt",
            ],
        },
        {
            "headline": "Global semiconductor shortage eases as TSMC expands Arizona fab",
            "summary": "TSMC announced its Arizona fabrication facility has begun producing 4nm chips ahead of schedule. Initial output is 10,000 wafers per month, expected to reach 50,000 by Q4 2026. This marks the first advanced node production on US soil.",
            "entities": ["TSMC", "Arizona", "semiconductors"],
            "impact": "medium",
            "topic": "Technology",
            "geography": "US",
            "source_count": 7,
            "confidence": 0.91,
            "social_score": 35,
            "market_odds": 0.85,
            "market_question": "Will US chip production exceed 10% of global output by 2028?",
            "market_volume": 3200000,
            "resolution_criteria": "Resolves YES if US semiconductor output exceeds 10% of global wafer production by December 31, 2028, per SIA data.",
            "price_history": [
                {"timestamp": now - 604800, "probability": 0.72},
                {"timestamp": now - 518400, "probability": 0.74},
                {"timestamp": now - 432000, "probability": 0.76},
                {"timestamp": now - 345600, "probability": 0.78},
                {"timestamp": now - 259200, "probability": 0.80},
                {"timestamp": now - 172800, "probability": 0.82},
                {"timestamp": now - 86400, "probability": 0.84},
                {"timestamp": now - 43200, "probability": 0.85},
            ],
            "articles": [
                {"title": "TSMC Arizona Fab Begins 4nm Production Ahead of Schedule", "publisher": "Nikkei Asia", "author": "Cheng Ting-Fang", "url": "https://asia.nikkei.com/Business/Tech/Semiconductors", "source_type": "rss", "text": "TSMC's Arizona fabrication facility began producing 4nm chips ahead of its original schedule. The plant is currently outputting 10,000 wafers per month with plans to ramp to 50,000 by Q4 2026. This marks the first time advanced-node semiconductors have been manufactured on US soil."},
                {"title": "The chip shortage is finally ending — here's why", "publisher": "The Verge", "author": "Sean Hollister", "url": "https://www.theverge.com/tech", "source_type": "rss", "text": "TSMC's Arizona fab is now producing 4nm chips, marking a turning point in the global semiconductor shortage. With 10,000 wafers per month and a ramp to 50,000 planned, US chip production is finally becoming a reality. The facility was built with support from the CHIPS Act."},
                {"title": "TSMC Arizona production starts — implications for US chip independence", "publisher": "Reddit/r/technology", "author": "u/chip_analyst", "url": "https://www.reddit.com/r/technology/", "source_type": "social", "social_coverage": 560, "text": "TSMC Arizona is live with 4nm production. 10k wafers/month now, targeting 50k by Q4. This is huge for US chip supply chain independence. Still a fraction of TSMC's Taiwan output but it's a start. CHIPS Act subsidies were critical to making this happen."},
            ],
            "claims": [
                {"who": "TSMC", "what": "Began 4nm chip production at Arizona facility", "when": "February 2026", "where": "Phoenix, Arizona"},
                {"who": "TSMC", "what": "Initial output at 10,000 wafers/month, targeting 50,000 by Q4 2026", "when": "February 2026", "where": "Arizona fab"},
            ],
            "timeline": [
                {"timestamp": now - 172800, "text": "TSMC confirms ahead-of-schedule production start"},
                {"timestamp": now - 86400, "text": "First 4nm wafers produced"},
                {"timestamp": now - 43200, "text": "Production ramp timeline published"},
            ],
        },
        {
            "headline": "California wildfire forces evacuation of 12,000 residents near Lake Tahoe",
            "summary": "A wildfire burning in the Sierra Nevada has prompted mandatory evacuations for 12,000 residents in communities near Lake Tahoe. The fire has consumed 4,500 acres with 15% containment. Dry conditions and high winds are hampering firefighting efforts.",
            "entities": ["Lake Tahoe", "California", "CAL FIRE", "Sierra Nevada"],
            "impact": "high",
            "topic": "Environment",
            "geography": "US",
            "source_count": 8,
            "confidence": 0.93,
            "social_score": 78,
            "articles": [
                {"title": "Tahoe-area wildfire forces 12,000 to evacuate", "publisher": "AP News", "author": "Olga Rodriguez", "url": "https://apnews.com/hub/wildfires", "source_type": "rss", "text": "A wildfire burning in the Sierra Nevada has forced mandatory evacuations for about 12,000 residents in communities near Lake Tahoe. The fire has consumed 4,500 acres and is only 15% contained. CAL FIRE officials said dry conditions and high winds are complicating suppression efforts."},
                {"title": "California wildfire near Lake Tahoe grows to 4,500 acres", "publisher": "CNN", "author": "Sarah Moon", "url": "https://www.cnn.com/weather", "source_type": "rss", "text": "A fast-moving wildfire near Lake Tahoe has grown to 4,500 acres, forcing the evacuation of 12,000 residents. The fire, which started near Highway 89, is only 15 percent contained. Firefighters are battling gusty winds and dry conditions. Several structures have been reported damaged."},
                {"title": "Sierra Nevada fire threatens Tahoe communities", "publisher": "Sacramento Bee", "author": "Tony Bizjak", "url": "https://www.sacbee.com/news/california/", "source_type": "rss", "text": "The Sierra Nevada wildfire has spread to 4,500 acres near Lake Tahoe communities, triggering evacuations for 12,000 residents. CAL FIRE reports 15% containment. The fire started near Highway 89 and has been fueled by unseasonably dry conditions and wind gusts up to 40 mph."},
                {"title": "Tahoe fire evacuation live updates and resources", "publisher": "Reddit/r/news", "author": "u/tahoe_local", "url": "https://www.reddit.com/r/news/", "source_type": "social", "social_coverage": 3400, "text": "Live thread for the Lake Tahoe area wildfire. 12,000 residents under mandatory evacuation orders. Fire is at 4,500 acres, 15% contained. Highway 89 closed. Evacuation shelters open at Carson City Convention Center and Douglas County Community Center. Air quality index hazardous in the basin."},
            ],
            "claims": [
                {"who": "CAL FIRE", "what": "Ordered mandatory evacuations for 12,000 residents", "when": "February 2026", "where": "Near Lake Tahoe, California"},
                {"who": "CAL FIRE", "what": "Fire at 4,500 acres, 15% containment", "when": "February 20, 2026", "where": "Sierra Nevada"},
            ],
            "timeline": [
                {"timestamp": now - 129600, "text": "Fire reported near Highway 89"},
                {"timestamp": now - 86400, "text": "Evacuations ordered for South Lake Tahoe"},
                {"timestamp": now - 43200, "text": "Fire grows to 4,500 acres in high winds"},
                {"timestamp": now - 21600, "text": "Containment reaches 15%"},
            ],
        },
        {
            "headline": "WHO declares end of mpox global health emergency",
            "summary": "The World Health Organization officially ended its declaration of mpox as a public health emergency of international concern. Global cases dropped 94% from peak levels. Vaccination campaigns in affected regions credited with driving decline.",
            "entities": ["WHO", "mpox", "PHEIC"],
            "impact": "medium",
            "topic": "Health",
            "geography": "Global",
            "source_count": 6,
            "confidence": 0.95,
            "social_score": 45,
            "articles": [
                {"title": "WHO ends mpox emergency declaration", "publisher": "Reuters", "author": "Jennifer Rigby", "url": "https://www.reuters.com/business/healthcare-pharmaceuticals/", "source_type": "rss", "text": "The World Health Organization officially ended its declaration of mpox as a public health emergency of international concern on Monday. WHO Director-General Tedros Adhanom Ghebreyesus said global cases have dropped 94% from peak levels, crediting vaccination campaigns in affected regions."},
                {"title": "Mpox emergency over, WHO says, as cases plummet 94%", "publisher": "BBC News", "author": "Smitha Mundasad", "url": "https://www.bbc.com/news/health", "source_type": "rss", "text": "The mpox public health emergency of international concern has ended, the WHO announced. Cases fell 94% from their peak thanks to vaccination campaigns. The WHO emergency committee met last week and recommended lifting the designation. Surveillance will continue."},
            ],
            "claims": [
                {"who": "WHO", "what": "Ended mpox public health emergency of international concern", "when": "February 2026"},
                {"who": "WHO", "what": "Global mpox cases declined 94% from peak", "when": "February 2026"},
            ],
            "timeline": [
                {"timestamp": now - 259200, "text": "WHO emergency committee meets"},
                {"timestamp": now - 172800, "text": "Committee recommends ending PHEIC"},
                {"timestamp": now - 86400, "text": "Director-General formally ends emergency"},
            ],
        },
        {
            "headline": "Bitcoin surpasses $125,000 amid institutional ETF inflows",
            "summary": "Bitcoin reached a new all-time high of $125,400, driven by record inflows into spot Bitcoin ETFs totaling $2.3 billion in the past week. BlackRock's iShares Bitcoin Trust now holds over $48 billion in assets.",
            "entities": ["Bitcoin", "BlackRock", "iShares", "ETF"],
            "impact": "medium",
            "topic": "Economy",
            "geography": "Global",
            "source_count": 10,
            "confidence": 0.92,
            "social_score": 90,
            "market_odds": 0.58,
            "market_question": "Will Bitcoin reach $150,000 by June 2026?",
            "market_volume": 12000000,
            "resolution_criteria": "Resolves YES if Bitcoin price exceeds $150,000 USD on any major exchange before June 30, 2026.",
            "price_history": [
                {"timestamp": now - 604800, "probability": 0.42},
                {"timestamp": now - 518400, "probability": 0.44},
                {"timestamp": now - 432000, "probability": 0.46},
                {"timestamp": now - 345600, "probability": 0.49},
                {"timestamp": now - 259200, "probability": 0.51},
                {"timestamp": now - 172800, "probability": 0.53},
                {"timestamp": now - 86400, "probability": 0.55},
                {"timestamp": now - 43200, "probability": 0.57},
                {"timestamp": now - 21600, "probability": 0.58},
            ],
            "articles": [
                {"title": "Bitcoin hits record $125,000 on ETF demand", "publisher": "CoinDesk", "author": "Sam Reynolds", "url": "https://www.coindesk.com/markets/", "source_type": "rss", "text": "Bitcoin surged to a new all-time high of $125,400 on Wednesday, propelled by record inflows into spot Bitcoin ETFs. Institutional investors poured $2.3 billion into Bitcoin ETF products over the past week. BlackRock's iShares Bitcoin Trust now holds over $48 billion in assets under management."},
                {"title": "Institutional money keeps flowing into Bitcoin ETFs", "publisher": "Bloomberg", "author": "Katie Greifeld", "url": "https://www.bloomberg.com/crypto", "source_type": "rss", "text": "Spot Bitcoin ETFs drew a record $2.3 billion in net inflows last week as the cryptocurrency hit new all-time highs. BlackRock's IBIT leads with $48 billion AUM. Fidelity's FBTC and Ark's ARKB also saw strong inflows. The rally reflects growing institutional acceptance of Bitcoin as a portfolio asset."},
                {"title": "Bitcoin at all-time high: what's driving the rally", "publisher": "Financial Times", "author": "Scott Chipolina", "url": "https://www.ft.com/cryptocurrencies", "source_type": "rss", "text": "Bitcoin has reached $125,400, driven by institutional demand through spot ETFs, the upcoming halving cycle effects, and favorable macro conditions. Weekly ETF inflows hit a record $2.3 billion. Analysts attribute the rally to a combination of supply constraints and growing corporate treasury adoption."},
                {"title": "$BTC breaks $125k — institutional adoption accelerating", "publisher": "Reddit/r/CryptoCurrency", "author": "u/crypto_analyst", "url": "https://www.reddit.com/r/CryptoCurrency/", "source_type": "social", "social_coverage": 8500, "text": "BTC just hit $125,400 ATH. ETF inflows at $2.3B weekly record. BlackRock IBIT alone has $48B AUM. This is pure institutional demand — retail volume hasn't spiked like previous cycles. Polymarket has $150k by June at 58%. The halving supply shock is still working its way through."},
                {"title": "Bitcoin $125k. BlackRock IBIT at $48B AUM. $2.3B in ETF inflows last week alone. This isn't retail-driven — it's institutional money flowing in at unprecedented scale.", "publisher": "Bluesky", "author": "@cryptoanalyst.bsky.social", "url": "https://bsky.app/", "source_type": "social", "social_coverage": 720, "text": "Bitcoin $125k. BlackRock IBIT at $48B AUM. $2.3B in ETF inflows last week alone. This isn't retail-driven — it's institutional money flowing in at unprecedented scale. Halving supply shock still working through. Polymarket has $150k by June at 58%."},
                {"title": "BTC hits $125k — chart breakdown and what's next for crypto", "publisher": "Instagram", "author": "@cryptocharts", "url": "https://www.instagram.com/p/C3xyz123456/", "source_type": "social", "social_coverage": 28000, "text": "Bitcoin just hit $125,400 all-time high. ETF inflows record $2.3B in one week. BlackRock IBIT at $48B AUM. Key levels: support at $118k, next resistance at $130k. RSI showing overbought on daily but weekly structure bullish. Institutional demand completely different from 2021 retail cycle."},
            ],
            "claims": [
                {"who": "Bitcoin market", "what": "Price reached all-time high of $125,400", "when": "February 2026"},
                {"who": "BlackRock", "what": "iShares Bitcoin Trust holds over $48 billion in assets", "when": "February 2026"},
            ],
            "timeline": [
                {"timestamp": now - 172800, "text": "Bitcoin crosses $120,000"},
                {"timestamp": now - 86400, "text": "ETF inflows hit $2.3B weekly record"},
                {"timestamp": now - 43200, "text": "New ATH of $125,400 reached"},
            ],
        },
        {
            "headline": "India and UK sign free trade agreement after 3 years of negotiations",
            "summary": "India and the United Kingdom signed a comprehensive free trade agreement covering goods, services, and digital trade. The deal eliminates tariffs on 90% of goods over 10 years and includes provisions for professional mobility.",
            "entities": ["India", "United Kingdom", "FTA", "Narendra Modi", "Keir Starmer"],
            "impact": "medium",
            "topic": "Politics",
            "geography": "Asia",
            "source_count": 5,
            "confidence": 0.90,
            "social_score": 30,
            "articles": [
                {"title": "India-UK free trade deal signed after marathon negotiations", "publisher": "Reuters", "author": "Aftab Ahmed", "url": "https://www.reuters.com/world/india/", "source_type": "rss", "text": "India and the United Kingdom signed a comprehensive free trade agreement on Tuesday after three years of negotiations. The deal eliminates tariffs on 90% of goods over 10 years and includes provisions for professional mobility and digital trade. Prime Ministers Modi and Starmer hailed the agreement as historic."},
                {"title": "Historic India-UK trade agreement: what's in the deal", "publisher": "Financial Times", "author": "John Reed", "url": "https://www.ft.com/world/asia-pacific", "source_type": "rss", "text": "India and the UK have signed their long-awaited free trade agreement. Key provisions include elimination of tariffs on 90% of goods over a decade, mutual recognition of professional qualifications, and a digital trade chapter covering data flows. The deal is expected to boost bilateral trade by 28%."},
            ],
            "claims": [
                {"who": "India and UK", "what": "Signed comprehensive free trade agreement", "when": "February 2026", "where": "New Delhi"},
                {"who": "India and UK", "what": "Deal eliminates tariffs on 90% of goods over 10 years", "when": "February 2026"},
            ],
            "timeline": [
                {"timestamp": now - 259200, "text": "Final round of negotiations concludes"},
                {"timestamp": now - 86400, "text": "Agreement signed by trade ministers"},
            ],
        },
        {
            "headline": "Major data breach at healthcare provider exposes 8 million patient records",
            "summary": "MedFirst Health Systems disclosed a data breach affecting 8.2 million patients. Compromised data includes names, Social Security numbers, medical records, and insurance information. The breach exploited an unpatched vulnerability in a third-party billing system.",
            "entities": ["MedFirst Health Systems", "HHS", "cybersecurity"],
            "impact": "high",
            "topic": "Technology",
            "geography": "US",
            "source_count": 6,
            "confidence": 0.88,
            "social_score": 55,
            "articles": [
                {"title": "Healthcare breach exposes 8 million patient records", "publisher": "BleepingComputer", "author": "Lawrence Abrams", "url": "https://www.bleepingcomputer.com/news/security/", "source_type": "rss", "text": "MedFirst Health Systems has disclosed a massive data breach affecting 8.2 million patients. Compromised data includes names, Social Security numbers, medical records, and insurance information. The breach was traced to an unpatched vulnerability in a third-party billing system that went undetected for 47 days."},
                {"title": "MedFirst discloses massive data breach", "publisher": "Reuters", "author": "Raphael Satter", "url": "https://www.reuters.com/technology/cybersecurity/", "source_type": "rss", "text": "MedFirst Health Systems disclosed a data breach affecting 8.2 million patients on Tuesday. The breach exploited an unpatched vulnerability in a third-party billing system. Compromised data includes SSNs, medical records, and insurance details. HHS has opened an investigation."},
                {"title": "MedFirst breach discussion — 8.2M patient records exposed", "publisher": "Hacker News", "author": "infosec_watcher", "url": "https://news.ycombinator.com/news", "source_type": "social", "social_coverage": 780, "text": "MedFirst breach: 8.2M patients, 47 days undetected, third-party billing system vulnerability. The vendor was reportedly warned about the vuln 6 months ago. KrebsOnSecurity found insurance billing records in the leaked dataset despite MedFirst claiming no financial data was accessed. This is going to be a massive class action."},
            ],
            "claims": [
                {"who": "MedFirst Health Systems", "what": "Disclosed data breach affecting 8.2 million patients", "when": "February 2026"},
                {"who": "MedFirst Health Systems", "what": "Breach exploited unpatched vulnerability in third-party billing system", "when": "February 2026", "uncertainty": "According to company disclosure filing"},
            ],
            "timeline": [
                {"timestamp": now - 432000, "text": "Breach detected by security team"},
                {"timestamp": now - 259200, "text": "Investigation confirms scope of breach"},
                {"timestamp": now - 86400, "text": "Public disclosure and notification begins"},
            ],
            "novel_facts": [
                "Breach went undetected for 47 days before security team discovered it",
                "Third-party billing vendor had been warned about the vulnerability 6 months prior",
            ],
            "disputed_claims": [
                {"claim": "MedFirst claims no financial data was accessed", "contradiction": "Security researcher found insurance billing records in leaked dataset, per KrebsOnSecurity report"},
            ],
        },
        {
            "headline": "Toyota unveils solid-state battery production line, targets 2027 vehicles",
            "summary": "Toyota Motor Corporation unveiled its first solid-state battery pilot production line at its Miyoshi plant. The batteries achieve 900 Wh/L energy density with 10-minute fast charging. Mass production targets FY2027 for integration into next-generation EVs.",
            "entities": ["Toyota", "solid-state battery", "EV"],
            "impact": "medium",
            "topic": "Technology",
            "geography": "Asia",
            "source_count": 7,
            "confidence": 0.89,
            "social_score": 40,
            "articles": [
                {"title": "Toyota shows off solid-state battery production line", "publisher": "Nikkei Asia", "author": "Sean Doyle", "url": "https://asia.nikkei.com/Business/Automobiles", "source_type": "rss", "text": "Toyota Motor unveiled its first solid-state battery pilot production line at its Miyoshi plant in Aichi Prefecture. The batteries achieve 900 Wh/L energy density — roughly double current lithium-ion cells — with a 10-minute fast charging capability. Mass production is targeted for fiscal year 2027."},
                {"title": "Solid-state batteries are finally real: Toyota's production breakthrough", "publisher": "Ars Technica", "author": "Jonathan Gitlin", "url": "https://arstechnica.com/cars/", "source_type": "rss", "text": "Toyota has shown the world its solid-state battery pilot line at the Miyoshi plant. Key specs: 900 Wh/L energy density, 10-minute fast charge, and targeted mass production by FY2027. This could transform the EV market if Toyota can scale manufacturing while maintaining the performance claims."},
            ],
            "claims": [
                {"who": "Toyota", "what": "Unveiled solid-state battery pilot production line", "when": "February 2026", "where": "Miyoshi plant, Japan"},
                {"who": "Toyota", "what": "Achieved 900 Wh/L energy density with 10-minute fast charging", "when": "February 2026"},
            ],
            "timeline": [
                {"timestamp": now - 172800, "text": "Media invited to Miyoshi plant"},
                {"timestamp": now - 86400, "text": "Production line unveiled, specs announced"},
                {"timestamp": now - 43200, "text": "FY2027 mass production timeline confirmed"},
            ],
        },
        {
            "headline": "US February CPI shows inflation cooling to 2.9% year-over-year",
            "summary": "The Bureau of Labor Statistics reported CPI rose 2.9% year-over-year in February, down from 3.1% in January. Core CPI excluding food and energy rose 3.2%. Markets rallied on hopes the data supports Fed rate cuts.",
            "entities": ["BLS", "CPI", "inflation", "S&P 500"],
            "impact": "high",
            "topic": "Economy",
            "geography": "US",
            "source_count": 12,
            "confidence": 0.96,
            "social_score": 70,
            "market_odds": 0.78,
            "market_question": "Will US CPI drop below 2.5% by Q3 2026?",
            "market_volume": 5200000,
            "resolution_criteria": "Resolves YES if BLS reports CPI below 2.5% YoY for any month in Q3 2026.",
            "price_history": [
                {"timestamp": now - 604800, "probability": 0.65},
                {"timestamp": now - 432000, "probability": 0.68},
                {"timestamp": now - 259200, "probability": 0.72},
                {"timestamp": now - 86400, "probability": 0.75},
                {"timestamp": now - 21600, "probability": 0.78},
            ],
            "articles": [
                {"title": "CPI Report: Inflation Cools to 2.9%, Boosting Rate-Cut Hopes", "publisher": "Wall Street Journal", "author": "Gwynn Guilford", "url": "https://www.wsj.com/economy/", "source_type": "rss", "text": "Consumer prices rose 2.9% in February from a year earlier, the Bureau of Labor Statistics said, down from 3.1% in January. Core CPI, which excludes food and energy, increased 3.2%. Markets rallied on the data, with investors pricing in higher odds of a Fed rate cut later this year. The S&P 500 rose 1.2%."},
                {"title": "Inflation falls to 2.9%, lowest since March 2024", "publisher": "Reuters", "author": "Lucia Mutikani", "url": "https://www.reuters.com/markets/us/", "source_type": "rss", "text": "U.S. consumer prices rose 2.9% year-over-year in February, the smallest increase since March 2024. The BLS report showed broad-based cooling with core CPI at 3.2%, down from 3.4%. Bond yields fell 8 basis points. The data strengthens the case for Federal Reserve rate cuts in the second half of 2026."},
                {"title": "Markets surge as inflation data beats expectations", "publisher": "Bloomberg", "author": "Reade Pickert", "url": "https://www.bloomberg.com/markets/economics", "source_type": "rss", "text": "U.S. stocks jumped and Treasury yields fell after the February CPI report showed inflation cooling faster than expected. The S&P 500 gained 1.2% and the Nasdaq rose 1.5%. CPI came in at 2.9% vs 3.0% consensus. Traders boosted bets on a Fed rate cut by September."},
                {"title": "CPI at 2.9% — rate cut odds discussion and market reaction", "publisher": "Reddit/r/economics", "author": "u/fed_watcher", "url": "https://www.reddit.com/r/economics/", "source_type": "social", "social_coverage": 1850, "text": "February CPI at 2.9% YoY, core at 3.2%. Both below consensus. S&P up 1.2%, 10Y yield down 8bps. Fed funds futures now pricing ~75% chance of September cut. This is the fastest disinflation pace since 2023. Key question is whether shelter costs continue to moderate — they're still the biggest contributor to core."},
            ],
            "claims": [
                {"who": "BLS", "what": "CPI rose 2.9% year-over-year in February", "when": "February 2026", "where": "United States", "numbers": ["2.9%", "3.1%", "3.2%"]},
                {"who": "BLS", "what": "Core CPI at 3.2%, down from 3.4% prior month", "when": "February 2026"},
            ],
            "timeline": [
                {"timestamp": now - 43200, "text": "BLS releases February CPI data"},
                {"timestamp": now - 39600, "text": "S&P 500 rises 1.2% on report"},
                {"timestamp": now - 36000, "text": "Bond yields drop 8 basis points"},
            ],
        },
        {
            "headline": "European Central Bank cuts rates by 25bp to 3.50% citing growth concerns",
            "summary": "The ECB lowered its main refinancing rate by 25 basis points to 3.50%, citing weakening economic growth across the eurozone. President Lagarde signaled further cuts possible if inflation continues trending toward the 2% target.",
            "entities": ["ECB", "Christine Lagarde", "eurozone", "interest rates"],
            "impact": "high",
            "topic": "Economy",
            "geography": "Europe",
            "source_count": 9,
            "confidence": 0.95,
            "social_score": 55,
            "market_odds": 0.65,
            "market_question": "Will ECB cut rates below 3% by end of 2026?",
            "market_volume": 4100000,
            "resolution_criteria": "Resolves YES if ECB main refinancing rate falls below 3.00% before December 31, 2026.",
            "price_history": [
                {"timestamp": now - 604800, "probability": 0.48},
                {"timestamp": now - 432000, "probability": 0.52},
                {"timestamp": now - 259200, "probability": 0.57},
                {"timestamp": now - 86400, "probability": 0.62},
                {"timestamp": now - 21600, "probability": 0.65},
            ],
            "articles": [
                {"title": "ECB Cuts Rates to 3.50%, Signals More Easing Ahead", "publisher": "Reuters", "author": "Francesco Canepa", "url": "https://www.reuters.com/markets/europe/", "source_type": "rss", "text": "The European Central Bank cut its main refinancing rate by 25 basis points to 3.50% on Thursday, citing weakening growth across the eurozone. ECB President Christine Lagarde said risks to economic growth tilt to the downside and signaled further easing if inflation continues trending toward the 2% target."},
                {"title": "Lagarde: Growth risks tilt to the downside", "publisher": "Financial Times", "author": "Martin Arnold", "url": "https://www.ft.com/european-central-bank", "source_type": "rss", "text": "ECB President Christine Lagarde warned that eurozone growth risks are tilted to the downside after the central bank cut rates to 3.50%. The 25bp reduction was unanimous. Lagarde indicated the ECB stands ready to act further if economic conditions deteriorate, though she emphasized the decision will be data-dependent."},
            ],
            "claims": [
                {"who": "ECB", "what": "Cut main refinancing rate by 25bp to 3.50%", "when": "February 2026", "where": "Frankfurt", "numbers": ["25bp", "3.50%", "3.75%"]},
                {"who": "Christine Lagarde", "what": "Signaled further cuts possible if inflation trends toward 2% target", "when": "February 2026"},
            ],
            "timeline": [
                {"timestamp": now - 86400, "text": "ECB Governing Council meets"},
                {"timestamp": now - 79200, "text": "Rate decision announced: 25bp cut"},
                {"timestamp": now - 75600, "text": "Lagarde press conference"},
            ],
        },
        {
            "headline": "Oil prices surge 8% after OPEC+ extends production cuts through Q3 2026",
            "summary": "Brent crude rose 8.2% to $89.40/barrel after OPEC+ agreed to extend voluntary production cuts of 2.2 million barrels/day through September 2026. Saudi Arabia led the decision amid concerns about weak demand from China.",
            "entities": ["OPEC+", "Saudi Arabia", "Brent crude", "oil"],
            "impact": "high",
            "topic": "Economy",
            "geography": "Global",
            "source_count": 10,
            "confidence": 0.94,
            "social_score": 60,
            "market_odds": 0.45,
            "market_question": "Will oil prices exceed $100/barrel in 2026?",
            "market_volume": 7800000,
            "resolution_criteria": "Resolves YES if Brent crude exceeds $100/barrel on any trading day in 2026.",
            "price_history": [
                {"timestamp": now - 604800, "probability": 0.30},
                {"timestamp": now - 432000, "probability": 0.33},
                {"timestamp": now - 259200, "probability": 0.37},
                {"timestamp": now - 86400, "probability": 0.42},
                {"timestamp": now - 21600, "probability": 0.45},
            ],
            "articles": [
                {"title": "Oil Jumps 8% as OPEC+ Extends Cuts to September", "publisher": "Bloomberg", "author": "Grant Smith", "url": "https://www.bloomberg.com/energy", "source_type": "rss", "text": "Brent crude surged 8.2% to $89.40 a barrel after OPEC+ agreed to extend voluntary production cuts of 2.2 million barrels per day through September 2026. Saudi Arabia led the decision amid mounting concerns about weak demand from China. The move was more aggressive than markets expected."},
                {"title": "OPEC+ keeps supply tight, Brent hits $89", "publisher": "Reuters", "author": "Alex Lawler", "url": "https://www.reuters.com/business/energy/", "source_type": "rss", "text": "OPEC+ agreed on Monday to extend voluntary oil production cuts of 2.2 million barrels per day through Q3 2026, sending Brent crude up 8.2% to $89.40. Saudi Arabia championed the extension citing concerns about slowing Chinese demand. Russia and the UAE backed the move."},
                {"title": "Gas prices expected to rise after OPEC decision", "publisher": "CNN", "author": "Matt Egan", "url": "https://www.cnn.com/business/energy", "source_type": "rss", "text": "American drivers should brace for higher gas prices after OPEC+ extended production cuts through September. Brent crude jumped 8.2% to $89.40/barrel. Analysts expect US gas prices to rise 15-25 cents per gallon in the coming weeks. The national average is currently $3.45."},
                {"title": "OPEC+ extends cuts — oil to $100? Market implications thread", "publisher": "Reddit/r/finance", "author": "u/oil_trader", "url": "https://www.reddit.com/r/finance/", "source_type": "social", "social_coverage": 920, "text": "OPEC+ extended 2.2M bpd cuts through Q3 2026. Brent at $89.40, up 8.2%. Polymarket has $100 oil at 45%. The key question is China demand — if it recovers, $100 is realistic. If not, OPEC+ is just defending price at the cost of market share. Energy sector ETFs up 4%+ today."},
            ],
            "claims": [
                {"who": "OPEC+", "what": "Extended voluntary production cuts of 2.2M bpd through Q3 2026", "when": "February 2026", "numbers": ["2.2M bpd", "8.2%", "$89.40"]},
                {"who": "Saudi Arabia", "what": "Led decision to maintain cuts amid weak China demand concerns", "when": "February 2026"},
            ],
            "timeline": [
                {"timestamp": now - 172800, "text": "OPEC+ ministerial meeting begins"},
                {"timestamp": now - 86400, "text": "Agreement reached: cuts extended through Q3"},
                {"timestamp": now - 43200, "text": "Brent crude rises 8.2% to $89.40"},
            ],
        },
        {
            "headline": "US-China trade tensions escalate with new 25% tariffs on tech imports",
            "summary": "The US Trade Representative announced 25% tariffs on $50 billion worth of Chinese technology imports including semiconductors, batteries, and AI hardware. China vowed retaliatory measures. Markets fell 1.8% on the news.",
            "entities": ["USTR", "China", "tariffs", "trade war", "semiconductors"],
            "impact": "high",
            "topic": "Economy",
            "geography": "Global",
            "source_count": 11,
            "confidence": 0.93,
            "social_score": 75,
            "articles": [
                {"title": "US Slaps 25% Tariffs on Chinese Tech, Markets Slide", "publisher": "Wall Street Journal", "author": "Lingling Wei", "url": "https://www.wsj.com/economy/trade/", "source_type": "rss", "text": "The US Trade Representative announced 25% tariffs on $50 billion worth of Chinese technology imports including semiconductors, batteries, and AI hardware. China's Ministry of Commerce vowed swift retaliatory measures. The S&P 500 fell 1.8% and the Nasdaq dropped 2.4% on the news."},
                {"title": "New US-China tariffs threaten global supply chains", "publisher": "Reuters", "author": "David Lawder", "url": "https://www.reuters.com/world/china/", "source_type": "rss", "text": "New 25% US tariffs on $50 billion in Chinese tech imports are set to disrupt global supply chains. The tariffs target semiconductors, EV batteries, and AI training hardware. Industry groups estimate $12 billion in annual costs to US companies. China vowed retaliatory tariffs on US agricultural and energy exports."},
                {"title": "China retaliatory tariffs incoming — trade war 2.0 discussion", "publisher": "Hacker News", "author": "trade_watcher", "url": "https://news.ycombinator.com/item?id=39472801", "source_type": "social", "social_coverage": 650, "text": "New 25% tariffs on $50B of Chinese tech imports just dropped. Targets include AI training chips and EV battery components. S&P down 1.8%, Nasdaq down 2.4%. SIA estimates $12B annual cost to US companies. China promising retaliation on ag and energy. This escalation is different from 2018 — it's specifically targeting the AI supply chain."},
                {"title": "US-China tariffs megathread: tech sector impact analysis", "publisher": "Reddit/r/finance", "author": "u/global_macro", "url": "https://www.reddit.com/r/finance/", "source_type": "social", "social_coverage": 1100, "text": "25% tariffs on $50B Chinese tech imports announced. Key targets: semiconductors, EV batteries, AI hardware. Markets tanking — S&P -1.8%, Nasdaq -2.4%. NVDA down 5.2%. The tariffs specifically hit AI training chips which is a direct shot at the AI buildout. Chinese retaliation expected on agriculture. Semiconductor industry says $12B annual cost."},
            ],
            "claims": [
                {"who": "USTR", "what": "Announced 25% tariffs on $50B of Chinese tech imports", "when": "February 2026", "where": "Washington, D.C.", "numbers": ["25%", "$50B"]},
                {"who": "China", "what": "Vowed retaliatory measures on US agricultural and energy exports", "when": "February 2026"},
            ],
            "timeline": [
                {"timestamp": now - 129600, "text": "USTR announces tariff schedule"},
                {"timestamp": now - 86400, "text": "China's Ministry of Commerce responds with retaliation warning"},
                {"timestamp": now - 43200, "text": "S&P 500 falls 1.8%, Nasdaq drops 2.4%"},
            ],
            "novel_facts": [
                "Tariffs specifically target AI training chips and EV battery components",
                "Semiconductor industry estimates $12B annual cost to US companies",
            ],
        },
        {
            "headline": "Minor update to Basel III banking rules delayed to 2028",
            "summary": "The Basel Committee on Banking Supervision announced a one-year delay to the final implementation of Basel III capital requirements, moving the deadline to January 2028. Banks welcomed the extension as they continue to adjust risk models.",
            "entities": ["Basel Committee", "Basel III", "banking"],
            "impact": "low",
            "topic": "Economy",
            "geography": "Global",
            "source_count": 3,
            "confidence": 0.85,
            "social_score": 5,
            "articles": [
                {"title": "Basel III final rules delayed by one year to 2028", "publisher": "Financial Times", "author": "Banking Editor", "url": "https://www.ft.com/banking", "source_type": "rss", "text": "The Basel Committee on Banking Supervision announced a one-year delay to the final implementation of Basel III capital requirements, pushing the deadline to January 2028. The delay gives banks more time to adjust their internal risk models to the new standards."},
                {"title": "Banking regulators push back Basel III deadline", "publisher": "Reuters", "author": "Huw Jones", "url": "https://www.reuters.com/business/finance/", "source_type": "rss", "text": "Global banking regulators have delayed the final phase of Basel III rules by one year to January 2028. The decision follows lobbying by major banks who said they needed more time to implement the complex new capital requirements."},
            ],
            "claims": [
                {"who": "Basel Committee", "what": "Delayed Basel III final implementation to January 2028", "when": "February 2026", "where": "Basel, Switzerland"},
            ],
        },
    ]

    for event_data in events:
        offset = events.index(event_data)
        cluster = EventCluster(
            headline=event_data["headline"],
            summary=event_data["summary"],
            entities=event_data["entities"],
            earliest_timestamp=now - 259200 - (offset * 3600),
            latest_timestamp=now - (offset * 3600),
            source_count=event_data["source_count"],
            confidence=event_data["confidence"],
            impact=event_data["impact"],
            market_odds=event_data.get("market_odds"),
            market_question=event_data.get("market_question"),
            price_history=event_data.get("price_history", []),
            market_volume=event_data.get("market_volume"),
            resolution_criteria=event_data.get("resolution_criteria", ""),
            timeline=event_data.get("timeline", []),
            disputed_claims=event_data.get("disputed_claims", []),
            novel_facts=event_data.get("novel_facts", []),
            topic=event_data.get("topic", ""),
            geography=event_data.get("geography", ""),
            social_score=event_data.get("social_score", 0),
        )

        # Create cluster first (articles have FK to cluster)
        create_cluster(cluster, skip_llm=True)

        article_ids = []
        for a_data in event_data.get("articles", []):
            article_text = a_data.get("text", "") or event_data["summary"]
            sens_score = processing.compute_sensationalism_score(a_data["title"], article_text)
            key_sents = processing.extract_key_sentences(article_text)
            word_count = len(article_text.split())
            # Compute fact density from claims count and word count
            claims_count = len(event_data.get("claims", []))
            fact_dens = processing.compute_fact_density(claims_count, word_count)
            article = Article(
                title=a_data["title"],
                author=a_data.get("author", ""),
                publisher=a_data["publisher"],
                timestamp=now - 86400 + (events.index(event_data) * 600),
                url=a_data.get("url", ""),
                text=article_text,
                word_count=word_count,
                entities=event_data["entities"],
                key_sentences=key_sents,
                fact_density=fact_dens,
                sensationalism_score=sens_score,
                source_type=a_data.get("source_type", "rss"),
                social_coverage=a_data.get("social_coverage", 0),
                cluster_id=cluster.id,
            )
            create_article(article, skip_llm=True)
            article_ids.append(article.id)

        # Update cluster with article IDs
        conn = get_db()
        try:
            conn.execute(
                "UPDATE event_clusters SET article_ids=?, source_count=? WHERE id=?",
                (json.dumps(article_ids), len(article_ids), cluster.id)
            )
            conn.commit()
        finally:
            conn.close()

        for c_data in event_data.get("claims", []):
            claim = Claim(
                who=c_data.get("who", ""),
                what=c_data.get("what", ""),
                when=c_data.get("when", ""),
                where=c_data.get("where", ""),
                numbers=c_data.get("numbers", []),
                direct_quotes=c_data.get("direct_quotes", []),
                source_article_id=article_ids[0] if article_ids else "",
                uncertainty=c_data.get("uncertainty", ""),
            )
            create_claim(claim, cluster.id)

    # Polymarket bets come from live API ingestion — no seed data needed.
    # Real bets from the Polymarket Gamma API have valid slugs that resolve
    # to actual event pages (e.g., polymarket.com/event/{slug}).
