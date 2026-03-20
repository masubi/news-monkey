"""Text processing functions for News Monkey.

Implements: sensationalism scoring, key sentence extraction, fact density
computation, SimHash fingerprinting, embedding cosine similarity,
persistent vector store for semantic clustering, and deduplication similarity.
"""
import hashlib
import json
import logging
import math
import os
import re
import threading
import time
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


# --- Sensationalism Scoring ---

# Words/phrases that indicate sensationalized or emotional framing
EMOTIONAL_WORDS = {
    "shocking", "terrifying", "horrifying", "devastating", "explosive",
    "bombshell", "outrage", "fury", "chaos", "crisis", "panic",
    "nightmare", "catastrophe", "catastrophic", "disastrous", "dire",
    "alarming", "staggering", "jaw-dropping", "mind-blowing", "insane",
    "unbelievable", "incredible", "outrageous", "scandalous", "slam",
    "slams", "blasts", "rips", "destroys", "eviscerate", "demolish",
    "skyrocket", "plummet", "freefall", "collapse", "meltdown",
    "erupts", "rages", "firestorm", "backlash", "uproar",
}

ABSOLUTES = {
    "always", "never", "everyone", "nobody", "nothing", "everything",
    "completely", "totally", "absolutely", "entirely", "utterly",
    "all", "none", "worst", "best", "ever",
}

LOADED_ADJECTIVES = {
    "massive", "huge", "enormous", "tiny", "pathetic", "laughable",
    "brilliant", "genius", "stupid", "idiotic", "radical", "extreme",
    "unprecedented", "historic", "groundbreaking", "revolutionary",
}

QUESTION_BAIT_PATTERNS = [
    r"\?$",                          # headline ends with question mark
    r"^(will|could|should|is|are|does|did|has|can|would)\s",  # leading question word
    r"you won't believe",
    r"here's why",
    r"what you need to know",
    r"the truth about",
    r"this is (what|why|how)",
]


def compute_sensationalism_score(title: str, text: str = "") -> float:
    """Score sensationalism from 0.0 (neutral) to 1.0 (highly sensational).

    Components:
    - Emotional words density
    - Absolute language density
    - Loaded adjectives density
    - Question-bait patterns in headline
    - ALL CAPS word ratio
    - Exclamation marks
    """
    combined = (title + " " + text).lower()
    words = re.findall(r'\b\w+\b', combined)
    if not words:
        return 0.0

    word_count = len(words)
    word_set = set(words)

    # Emotional words (0-0.3)
    emotional_count = len(word_set & EMOTIONAL_WORDS)
    emotional_score = min(emotional_count / 3.0, 1.0) * 0.3

    # Absolutes (0-0.15)
    absolute_count = len(word_set & ABSOLUTES)
    absolute_score = min(absolute_count / 3.0, 1.0) * 0.15

    # Loaded adjectives (0-0.15)
    loaded_count = len(word_set & LOADED_ADJECTIVES)
    loaded_score = min(loaded_count / 3.0, 1.0) * 0.15

    # Question bait in headline (0-0.15)
    title_lower = title.lower()
    bait_count = sum(1 for p in QUESTION_BAIT_PATTERNS if re.search(p, title_lower))
    bait_score = min(bait_count / 2.0, 1.0) * 0.15

    # ALL CAPS words in original (excl. abbreviations <4 chars) (0-0.1)
    original_words = re.findall(r'\b[A-Z]{4,}\b', title + " " + text)
    caps_score = min(len(original_words) / 3.0, 1.0) * 0.1

    # Exclamation marks (0-0.15)
    exclamation_count = (title + text).count("!")
    exclamation_score = min(exclamation_count / 2.0, 1.0) * 0.15

    total = emotional_score + absolute_score + loaded_score + bait_score + caps_score + exclamation_score
    return round(min(total, 1.0), 3)


# --- Key Sentence Extraction ---

def extract_key_sentences(text: str, max_sentences: int = 5) -> list[str]:
    """Extract key sentences from article text using extractive heuristics.

    Strategy: score sentences by presence of named entities, numbers,
    quotes, and position (first/last sentences get a boost).
    """
    if not text:
        return []

    # Split into sentences (simple approach)
    sentences = re.split(r'(?<=[.!?])\s+', text.strip())
    sentences = [s.strip() for s in sentences if len(s.strip()) > 20]

    if len(sentences) <= max_sentences:
        return sentences

    scored = []
    for idx, sent in enumerate(sentences):
        score = 0.0

        # Position boost: first and last sentences
        if idx == 0:
            score += 2.0
        elif idx == len(sentences) - 1:
            score += 1.0

        # Numbers boost
        numbers = re.findall(r'\b\d[\d,.%$]+\b', sent)
        score += len(numbers) * 0.5

        # Quoted text boost
        quotes = re.findall(r'"[^"]{10,}"', sent)
        score += len(quotes) * 1.5

        # Named entity heuristic (capitalized multi-word phrases)
        entities = re.findall(r'[A-Z][a-z]+(?:\s+[A-Z][a-z]+)+', sent)
        score += len(entities) * 0.3

        # Length penalty for very short or very long sentences
        word_count = len(sent.split())
        if word_count < 5:
            score -= 1.0
        elif word_count > 50:
            score -= 0.5

        scored.append((score, idx, sent))

    scored.sort(key=lambda x: x[0], reverse=True)
    # Take top N but return in original order
    top = sorted(scored[:max_sentences], key=lambda x: x[1])
    return [s[2] for s in top]


# --- Fact Density Computation ---

def compute_fact_density(claim_count: int, word_count: int) -> float:
    """Compute fact density as distinct claims per word count.

    Returns claims per 100 words for readability.
    """
    if word_count <= 0:
        return 0.0
    return round(claim_count / word_count, 4)


# --- SimHash Fingerprinting ---

def _simhash_token_hash(token: str) -> int:
    """Hash a token to a 64-bit integer."""
    h = hashlib.md5(token.encode('utf-8')).hexdigest()
    return int(h[:16], 16)


def simhash(text: str) -> int:
    """Compute SimHash fingerprint for text deduplication.

    Returns a 64-bit integer fingerprint.
    """
    tokens = re.findall(r'\b\w+\b', text.lower())
    if not tokens:
        return 0

    v = [0] * 64
    for token in tokens:
        h = _simhash_token_hash(token)
        for i in range(64):
            if h & (1 << i):
                v[i] += 1
            else:
                v[i] -= 1

    fingerprint = 0
    for i in range(64):
        if v[i] > 0:
            fingerprint |= (1 << i)
    return fingerprint


def simhash_distance(hash1: int, hash2: int) -> int:
    """Compute Hamming distance between two SimHash fingerprints."""
    xor = hash1 ^ hash2
    return bin(xor).count('1')


def simhash_similarity(hash1: int, hash2: int) -> float:
    """Compute similarity (0-1) between two SimHash fingerprints."""
    distance = simhash_distance(hash1, hash2)
    return 1.0 - (distance / 64.0)


# --- Entity Jaccard Similarity ---

def entity_jaccard(entities1: list[str], entities2: list[str]) -> float:
    """Compute Jaccard similarity between two entity lists."""
    set1 = {e.lower() for e in entities1}
    set2 = {e.lower() for e in entities2}
    if not set1 and not set2:
        return 0.0
    intersection = set1 & set2
    union = set1 | set2
    return len(intersection) / len(union) if union else 0.0


# --- Combined Deduplication Score ---

def compute_dedup_score(
    title1: str, title2: str,
    entities1: list[str], entities2: list[str],
    text1: str = "", text2: str = "",
) -> dict:
    """Compute deduplication similarity score between two articles.

    Returns dict with component scores and overall similarity.
    Similarity > 0.85 = candidate duplicate per requirements.
    """
    # SimHash fingerprinting
    hash1 = simhash(title1 + " " + text1)
    hash2 = simhash(title2 + " " + text2)
    simhash_sim = simhash_similarity(hash1, hash2)

    # Entity Jaccard
    jaccard = entity_jaccard(entities1, entities2)

    # Title SimHash (fast pass on headlines)
    title_hash1 = simhash(title1)
    title_hash2 = simhash(title2)
    title_sim = simhash_similarity(title_hash1, title_hash2)

    # Weighted overall score
    overall = (title_sim * 0.4) + (simhash_sim * 0.3) + (jaccard * 0.3)

    return {
        "title_similarity": round(title_sim, 4),
        "content_similarity": round(simhash_sim, 4),
        "entity_jaccard": round(jaccard, 4),
        "overall_similarity": round(overall, 4),
        "is_duplicate": overall > 0.70,
    }


# --- Neutral Summary Generation ---

def generate_neutral_headline(title: str) -> str:
    """Strip sensationalized language from a headline and return neutral version.

    Rule-based approach: remove emojis, emotional words, exclamation marks,
    ALL CAPS, question-bait framing, and broadcast artifacts.
    """
    result = title

    # Strip emojis and other non-ASCII symbols (keep basic Latin + common punctuation)
    result = re.sub(
        r'[\U0001F300-\U0001F9FF\U0001FA00-\U0001FA6F\U0001FA70-\U0001FAFF'
        r'\U00002702-\U000027B0\U000024C2-\U0001F251'
        r'\U0000FE00-\U0000FE0F\U0000200D]+',
        '', result
    )

    # Remove exclamation marks
    result = result.replace("!", ".")

    # Replace ALL CAPS words (4+ chars) with title case (but not abbreviations like FBI, NATO)
    result = re.sub(
        r'\b([A-Z]{4,})\b',
        lambda m: m.group(1).title() if len(m.group(1)) > 4 else m.group(1),
        result
    )

    # Remove leading question-bait phrases and broadcast tags
    for pattern in [
        r'^(BREAKING|JUST IN|EXCLUSIVE|URGENT|ALERT|WATCH|LISTEN):\s*',
        r"^You won't believe:?\s*",
        r'^\[.*?\]\s*',  # Remove [P], [D], etc. Reddit-style tags
        r'^[\U0001F4E2\U0001F534\U0001F6A8]?\s*',  # Alarm emojis that survived first pass
    ]:
        result = re.sub(pattern, '', result, flags=re.IGNORECASE)

    # Remove broadcast show names appended after pipe or vertical bar
    # e.g. "Oil Price Shock: Who Gets Hit? | Show Name 03/09/20"
    result = re.sub(r'\s*\|\s*[\w\s]+\d{2}/\d{2}/\d{2,4}\s*$', '', result)

    # Remove trailing timestamps/dates in broadcast format
    result = re.sub(r'\s+\d{2}/\d{2}/\d{2,4}\s*$', '', result)

    # Replace emotional/loaded words in headline with neutral alternatives
    _replacements = {
        r'\bslams?\b': 'criticizes',
        r'\bblasts?\b': 'criticizes',
        r'\brips?\b': 'criticizes',
        r'\bdestroys?\b': 'challenges',
        r'\bskyrockets?\b': 'rises sharply',
        r'\bplummets?\b': 'declines sharply',
        r'\bfreefall\b': 'decline',
        r'\bchaos\b': 'disruption',
        r'\berupts?\b': 'emerges',
        r'\bmeltdown\b': 'decline',
        r'\bshock\b': 'impact',
        r'\bstunning\b': 'notable',
        r'\bbombshell\b': 'development',
    }
    for pattern, replacement in _replacements.items():
        result = re.sub(pattern, replacement, result, flags=re.IGNORECASE)

    # Collapse multiple spaces
    result = re.sub(r'\s{2,}', ' ', result)

    return result.strip()


# --- Embedding Cosine Similarity ---

def cosine_similarity(vec1: list[float], vec2: list[float]) -> float:
    """Compute cosine similarity between two embedding vectors."""
    if not vec1 or not vec2 or len(vec1) != len(vec2):
        return 0.0
    dot = sum(a * b for a, b in zip(vec1, vec2))
    mag1 = math.sqrt(sum(a * a for a in vec1))
    mag2 = math.sqrt(sum(b * b for b in vec2))
    if mag1 == 0 or mag2 == 0:
        return 0.0
    return dot / (mag1 * mag2)


def compute_dedup_score_with_embeddings(
    title1: str, title2: str,
    entities1: list[str], entities2: list[str],
    text1: str = "", text2: str = "",
    embedding1: Optional[list[float]] = None,
    embedding2: Optional[list[float]] = None,
) -> dict:
    """Compute dedup score with optional embedding cosine similarity.

    When embeddings are available, uses the full fast pass:
    headline embedding cosine similarity + entity Jaccard + SimHash fingerprinting.
    """
    # SimHash fingerprinting
    hash1 = simhash(title1 + " " + text1)
    hash2 = simhash(title2 + " " + text2)
    simhash_sim = simhash_similarity(hash1, hash2)

    # Entity Jaccard
    jaccard = entity_jaccard(entities1, entities2)

    # Embedding cosine similarity (headline)
    embedding_sim = None
    if embedding1 is not None and embedding2 is not None:
        embedding_sim = cosine_similarity(embedding1, embedding2)

    if embedding_sim is not None:
        # Full fast pass: embedding cosine (0.35) + entity Jaccard (0.30) + SimHash (0.20) + title SimHash (0.15)
        title_hash1 = simhash(title1)
        title_hash2 = simhash(title2)
        title_sim = simhash_similarity(title_hash1, title_hash2)
        overall = (embedding_sim * 0.35) + (jaccard * 0.30) + (simhash_sim * 0.20) + (title_sim * 0.15)
    else:
        # Fallback: SimHash + Jaccard + title SimHash
        title_hash1 = simhash(title1)
        title_hash2 = simhash(title2)
        title_sim = simhash_similarity(title_hash1, title_hash2)
        overall = (title_sim * 0.4) + (simhash_sim * 0.3) + (jaccard * 0.3)

    result = {
        "title_similarity": round(title_sim, 4),
        "content_similarity": round(simhash_sim, 4),
        "entity_jaccard": round(jaccard, 4),
        "overall_similarity": round(overall, 4),
        "is_duplicate": overall > 0.70,
    }
    if embedding_sim is not None:
        result["embedding_cosine"] = round(embedding_sim, 4)
    return result


# --- Precomputed Entity Hashes ---

# Cache of entity set → frozenset hash for fast Jaccard computation
# Uses OrderedDict to maintain insertion order for proper LRU eviction
from collections import OrderedDict
_entity_hash_cache: OrderedDict[str, frozenset] = OrderedDict()
_entity_hash_cache_lock = threading.Lock()
_MAX_ENTITY_HASH_CACHE = 10000


def get_entity_hash(entities: list[str]) -> frozenset:
    """Get or compute a precomputed frozenset hash for entity list.

    Caches results for fast repeated Jaccard comparisons.
    """
    key = "|".join(sorted(e.lower() for e in entities))
    with _entity_hash_cache_lock:
        if key in _entity_hash_cache:
            _entity_hash_cache.move_to_end(key)  # Mark as recently used
            return _entity_hash_cache[key]
        if len(_entity_hash_cache) >= _MAX_ENTITY_HASH_CACHE:
            # Evict oldest half (first items in OrderedDict)
            for _ in range(len(_entity_hash_cache) // 2):
                _entity_hash_cache.popitem(last=False)
        _entity_hash_cache[key] = frozenset(e.lower() for e in entities)
        return _entity_hash_cache[key]


def entity_jaccard_fast(entities1: list[str], entities2: list[str]) -> float:
    """Fast entity Jaccard using precomputed hashes."""
    s1 = get_entity_hash(entities1)
    s2 = get_entity_hash(entities2)
    if not s1 and not s2:
        return 0.0
    intersection = len(s1 & s2)
    union = len(s1 | s2)
    return intersection / union if union > 0 else 0.0


# --- In-Memory Vector Store with ANN and Cached Centroids ---

class VectorStore:
    """Persistent vector store for semantic clustering of event articles.

    Stores embeddings with metadata and supports approximate nearest neighbor (ANN)
    search using random projection LSH for fast candidate retrieval, with exact
    cosine similarity reranking. Persists vectors and centroids to disk.
    """

    ANN_NUM_TABLES = 8    # Number of hash tables for LSH
    ANN_HASH_BITS = 10    # Number of hash bits per table
    MAX_VECTORS = 50000   # Cap to prevent unbounded memory growth

    def __init__(self, persist_path: Optional[str] = None):
        self._vectors: list[dict] = []  # [{id, embedding, cluster_id, metadata}]
        self._cluster_centroids: dict[str, list[float]] = {}
        self._persist_path = persist_path
        self._dirty = False
        self._lock = threading.Lock()
        # ANN index: list of hash tables, each mapping hash -> list of vector indices
        self._ann_tables: list[dict[int, list[int]]] = []
        self._ann_projections: list[list[list[float]]] = []  # random projection vectors
        self._ann_initialized = False
        if persist_path:
            self._load()

    def _init_ann(self, dim: int):
        """Initialize random projection tables for ANN search."""
        import random
        rng = random.Random(42)  # deterministic for reproducibility
        self._ann_tables = [{} for _ in range(self.ANN_NUM_TABLES)]
        self._ann_projections = []
        for _ in range(self.ANN_NUM_TABLES):
            proj = [[rng.gauss(0, 1) for _ in range(dim)] for _ in range(self.ANN_HASH_BITS)]
            self._ann_projections.append(proj)
        self._ann_initialized = True

    def _compute_ann_hash(self, embedding: list[float], table_idx: int) -> int:
        """Compute LSH hash for a single table using random projections."""
        h = 0
        for bit_idx, proj_vec in enumerate(self._ann_projections[table_idx]):
            dot = sum(a * b for a, b in zip(embedding, proj_vec))
            if dot > 0:
                h |= (1 << bit_idx)
        return h

    def _index_vector(self, idx: int, embedding: list[float]):
        """Add a vector to all ANN hash tables."""
        if not self._ann_initialized:
            return
        for table_idx in range(self.ANN_NUM_TABLES):
            h = self._compute_ann_hash(embedding, table_idx)
            if h not in self._ann_tables[table_idx]:
                self._ann_tables[table_idx][h] = []
            self._ann_tables[table_idx][h].append(idx)

    def _rebuild_ann_index(self):
        """Rebuild ANN index from all stored vectors."""
        if not self._vectors:
            return
        dim = len(self._vectors[0]["embedding"])
        self._init_ann(dim)
        for idx, vec in enumerate(self._vectors):
            self._index_vector(idx, vec["embedding"])

    def add(self, item_id: str, embedding: list[float], cluster_id: Optional[str] = None, metadata: Optional[dict] = None):
        """Add an embedding to the store."""
        with self._lock:
            if len(self._vectors) >= self.MAX_VECTORS:
                # Prune oldest half to make room
                self._vectors = self._vectors[self.MAX_VECTORS // 2:]
                self._rebuild_ann_index()
            idx = len(self._vectors)
            self._vectors.append({
                "id": item_id,
                "embedding": embedding,
                "cluster_id": cluster_id,
                "metadata": metadata or {},
            })
            if not self._ann_initialized and embedding:
                self._init_ann(len(embedding))
            self._index_vector(idx, embedding)
            if cluster_id:
                self._update_centroid(cluster_id)
            self._dirty = True
            # Auto-save periodically (every 50 additions)
            if self._persist_path and len(self._vectors) % 50 == 0:
                self._save_unlocked()

    def search(self, query_embedding: list[float], top_k: int = 5, threshold: float = 0.0) -> list[dict]:
        """Find nearest neighbors using ANN candidate retrieval + exact reranking.

        For small stores (<200 vectors), falls back to brute-force for accuracy.
        For larger stores, uses LSH to find candidates, then exact cosine reranking.
        """
        with self._lock:
            # Brute force for small stores
            if len(self._vectors) < 200 or not self._ann_initialized:
                return self._search_brute_force(query_embedding, top_k, threshold)

            # ANN: collect candidate indices from all hash tables
            candidates = set()
            for table_idx in range(self.ANN_NUM_TABLES):
                h = self._compute_ann_hash(query_embedding, table_idx)
                if h in self._ann_tables[table_idx]:
                    candidates.update(self._ann_tables[table_idx][h])

            # If ANN found too few candidates, fall back to brute force
            if len(candidates) < top_k * 3:
                return self._search_brute_force(query_embedding, top_k, threshold)

            # Exact reranking of ANN candidates
            results = []
            for idx in candidates:
                if idx < len(self._vectors):
                    vec = self._vectors[idx]
                    sim = cosine_similarity(query_embedding, vec["embedding"])
                    if sim >= threshold:
                        results.append({
                            "id": vec["id"],
                            "cluster_id": vec["cluster_id"],
                            "similarity": round(sim, 4),
                            "metadata": vec["metadata"],
                        })
            results.sort(key=lambda x: x["similarity"], reverse=True)
            return results[:top_k]

    def _search_brute_force(self, query_embedding: list[float], top_k: int = 5, threshold: float = 0.0) -> list[dict]:
        """Brute-force nearest neighbor search."""
        results = []
        for vec in self._vectors:
            sim = cosine_similarity(query_embedding, vec["embedding"])
            if sim >= threshold:
                results.append({
                    "id": vec["id"],
                    "cluster_id": vec["cluster_id"],
                    "similarity": round(sim, 4),
                    "metadata": vec["metadata"],
                })
        results.sort(key=lambda x: x["similarity"], reverse=True)
        return results[:top_k]

    def find_cluster(self, embedding: list[float], threshold: float = 0.75) -> Optional[str]:
        """Find the best matching cluster for an embedding based on cached centroid similarity."""
        with self._lock:
            best_cluster = None
            best_sim = threshold
            for cluster_id, centroid in self._cluster_centroids.items():
                sim = cosine_similarity(embedding, centroid)
                if sim > best_sim:
                    best_sim = sim
                    best_cluster = cluster_id
            return best_cluster

    def _update_centroid(self, cluster_id: str):
        """Recompute and cache centroid for a cluster."""
        cluster_vecs = [v["embedding"] for v in self._vectors if v["cluster_id"] == cluster_id]
        if not cluster_vecs:
            self._cluster_centroids.pop(cluster_id, None)
            return
        dim = len(cluster_vecs[0])
        centroid = [0.0] * dim
        for vec in cluster_vecs:
            for i in range(dim):
                centroid[i] += vec[i]
        n = len(cluster_vecs)
        centroid = [c / n for c in centroid]
        self._cluster_centroids[cluster_id] = centroid

    def consolidate(self):
        """Periodic consolidation: remove orphaned vectors, rebuild ANN index, recompute all centroids."""
        with self._lock:
            # Recompute all centroids
            cluster_ids = set(v["cluster_id"] for v in self._vectors if v.get("cluster_id"))
            for cid in cluster_ids:
                self._update_centroid(cid)
            # Remove stale centroid entries
            stale = set(self._cluster_centroids.keys()) - cluster_ids
            for cid in stale:
                del self._cluster_centroids[cid]
            # Rebuild ANN index
            self._rebuild_ann_index()
            self._dirty = True
            logger.info("Vector store consolidated: %d vectors, %d clusters", len(self._vectors), len(cluster_ids))

    def get_centroid(self, cluster_id: str) -> Optional[list[float]]:
        """Return cached centroid embedding for a cluster, or None if not found."""
        with self._lock:
            return self._cluster_centroids.get(cluster_id)

    def get_cluster_ids(self) -> list[str]:
        """Return all known cluster IDs."""
        with self._lock:
            return list(self._cluster_centroids.keys())

    @property
    def size(self) -> int:
        with self._lock:
            return len(self._vectors)

    def _save_unlocked(self):
        """Persist vector store to disk. Caller must hold _lock."""
        if not self._persist_path or not self._dirty:
            return
        try:
            data = {
                "vectors": self._vectors,
                "centroids": self._cluster_centroids,
            }
            path = Path(self._persist_path)
            path.parent.mkdir(parents=True, exist_ok=True)
            tmp_path = str(path) + ".tmp"
            with open(tmp_path, "w") as f:
                json.dump(data, f)
            os.replace(tmp_path, str(path))
            self._dirty = False
            logger.debug("Vector store saved (%d vectors, %d centroids)", len(self._vectors), len(self._cluster_centroids))
        except Exception as e:
            logger.error("Failed to save vector store: %s", e)

    def save(self):
        """Persist vector store (vectors + cached centroids) to disk."""
        if not self._persist_path or not self._dirty:
            return
        with self._lock:
            self._save_unlocked()

    def _load(self):
        """Load vector store from disk and rebuild ANN index."""
        if not self._persist_path:
            return
        path = Path(self._persist_path)
        if not path.exists():
            return
        try:
            with open(path) as f:
                data = json.load(f)
            self._vectors = data.get("vectors", [])
            self._cluster_centroids = data.get("centroids", {})
            # Rebuild ANN index from loaded data
            if self._vectors:
                self._rebuild_ann_index()
            logger.info("Vector store loaded (%d vectors, %d centroids)",
                       len(self._vectors), len(self._cluster_centroids))
        except Exception as e:
            logger.error("Failed to load vector store: %s", e)


# Global vector store instance with persistence
_data_dir = Path(os.environ.get("NEWS_MONKEY_DATA_DIR", Path(__file__).parent / "data"))
_vs_path = str(_data_dir / "vector_store.json")
vector_store = VectorStore(persist_path=_vs_path)
