"""Ollama integration for News Monkey.

Provides LLM-based fact extraction, neutral headline generation,
and text embeddings for semantic similarity and clustering.
Falls back to rule-based processing when Ollama is unavailable.
"""
import json
import logging
import os
import threading
from typing import Optional

import httpx

logger = logging.getLogger(__name__)

# Semaphore to limit concurrent Ollama generate requests (prevents congestion timeouts)
_generate_semaphore = threading.Semaphore(2)

OLLAMA_BASE_URL = os.environ.get("OLLAMA_BASE_URL", "http://localhost:11434")
OLLAMA_MODEL = os.environ.get("OLLAMA_MODEL", "qwen3:4b")
OLLAMA_EMBED_MODEL = os.environ.get("OLLAMA_EMBED_MODEL", "nomic-embed-text")
OLLAMA_TIMEOUT = float(os.environ.get("OLLAMA_TIMEOUT", "30"))

_ollama_available: Optional[bool] = None
_ollama_checked_at: float = 0.0
_OLLAMA_RECHECK_INTERVAL = 300.0  # Re-check every 5 minutes if unavailable

# Circuit breaker: after N consecutive timeouts, temporarily disable Ollama
_consecutive_timeouts: int = 0
_CIRCUIT_BREAKER_THRESHOLD = 3  # Trips after this many consecutive timeouts
_circuit_open_until: float = 0.0  # Timestamp when circuit breaker resets
_CIRCUIT_BREAKER_COOLDOWN = 60.0  # Seconds to wait before retrying after trip


def _check_circuit_breaker() -> bool:
    """Return True if circuit breaker is open (Ollama calls should be skipped)."""
    import time
    if _consecutive_timeouts >= _CIRCUIT_BREAKER_THRESHOLD:
        if time.time() < _circuit_open_until:
            return True  # Circuit is open — skip calls
    return False


def _record_timeout():
    """Record a timeout and potentially trip the circuit breaker."""
    import time
    global _consecutive_timeouts, _circuit_open_until
    _consecutive_timeouts += 1
    if _consecutive_timeouts >= _CIRCUIT_BREAKER_THRESHOLD:
        _circuit_open_until = time.time() + _CIRCUIT_BREAKER_COOLDOWN
        logger.warning(
            "Ollama circuit breaker tripped after %d consecutive timeouts — pausing for %ds",
            _consecutive_timeouts, _CIRCUIT_BREAKER_COOLDOWN
        )


def _record_success():
    """Reset consecutive timeout counter on successful call."""
    global _consecutive_timeouts
    _consecutive_timeouts = 0


def is_available() -> bool:
    """Check if Ollama is reachable. Re-checks periodically if previously unavailable."""
    import time
    global _ollama_available, _ollama_checked_at
    if _check_circuit_breaker():
        return False
    now = time.time()
    # Re-check even after success every 30 minutes (circuit breaker for Ollama crash)
    if _ollama_available is True and (now - _ollama_checked_at) < _OLLAMA_RECHECK_INTERVAL * 6:
        return True
    if _ollama_available is False and (now - _ollama_checked_at) < _OLLAMA_RECHECK_INTERVAL:
        return False
    try:
        resp = httpx.get(f"{OLLAMA_BASE_URL}/api/tags", timeout=5)
        _ollama_available = resp.status_code == 200
    except Exception:
        _ollama_available = False
    _ollama_checked_at = now
    if not _ollama_available:
        logger.warning("Ollama not available at %s — using rule-based fallbacks", OLLAMA_BASE_URL)
    return _ollama_available


def reset_availability():
    """Reset cached availability check (for testing or reconnection)."""
    global _ollama_available, _consecutive_timeouts, _circuit_open_until
    _ollama_available = None
    _consecutive_timeouts = 0
    _circuit_open_until = 0.0


def generate(prompt: str, system: str = "", model: str = "") -> Optional[str]:
    """Generate text using Ollama LLM.

    Returns None if Ollama is unavailable.
    """
    if not is_available():
        return None
    acquired = _generate_semaphore.acquire(timeout=OLLAMA_TIMEOUT)
    if not acquired:
        logger.warning("Ollama generate semaphore timeout — too many queued requests")
        return None
    try:
        payload = {
            "model": model or OLLAMA_MODEL,
            "prompt": prompt,
            "stream": False,
        }
        if system:
            payload["system"] = system
        resp = httpx.post(
            f"{OLLAMA_BASE_URL}/api/generate",
            json=payload,
            timeout=OLLAMA_TIMEOUT,
        )
        if resp.status_code == 200:
            _record_success()
            return resp.json().get("response", "").strip()
    except (httpx.TimeoutException, TimeoutError):
        _record_timeout()
        logger.debug("Ollama generate timed out (timeout=%ds)", OLLAMA_TIMEOUT)
    except Exception as e:
        if "timed out" in str(e).lower() or "timeout" in str(e).lower():
            _record_timeout()
        logger.error("Ollama generate error: %s", e)
    finally:
        _generate_semaphore.release()
    return None


def embed(text: str, model: str = "") -> Optional[list[float]]:
    """Get embedding vector for text using Ollama.

    Returns None if Ollama is unavailable.
    """
    if not is_available():
        return None
    try:
        resp = httpx.post(
            f"{OLLAMA_BASE_URL}/api/embed",
            json={"model": model or OLLAMA_EMBED_MODEL, "input": text},
            timeout=OLLAMA_TIMEOUT,
        )
        if resp.status_code == 200:
            _record_success()
            data = resp.json()
            embeddings = data.get("embeddings")
            if embeddings and len(embeddings) > 0:
                return embeddings[0]
    except (httpx.TimeoutException, TimeoutError):
        _record_timeout()
        logger.debug("Ollama embed timed out")
    except Exception as e:
        if "timed out" in str(e).lower() or "timeout" in str(e).lower():
            _record_timeout()
        logger.error("Ollama embed error: %s", e)
    return None


def embed_batch(texts: list[str], model: str = "") -> Optional[list[list[float]]]:
    """Get embedding vectors for multiple texts.

    Returns None if Ollama is unavailable.
    """
    if not is_available():
        return None
    try:
        resp = httpx.post(
            f"{OLLAMA_BASE_URL}/api/embed",
            json={"model": model or OLLAMA_EMBED_MODEL, "input": texts},
            timeout=OLLAMA_TIMEOUT * 2,
        )
        if resp.status_code == 200:
            _record_success()
            return resp.json().get("embeddings")
    except (httpx.TimeoutException, TimeoutError):
        _record_timeout()
        logger.debug("Ollama batch embed timed out")
    except Exception as e:
        if "timed out" in str(e).lower() or "timeout" in str(e).lower():
            _record_timeout()
        logger.error("Ollama batch embed error: %s", e)
    return None


def extract_claims(article_text: str, title: str = "") -> Optional[list[dict]]:
    """Extract verifiable factual claims from article text using LLM.

    Returns list of claim dicts with keys: who, what, when, where, numbers,
    direct_quotes, uncertainty. Returns None if Ollama unavailable.
    """
    if not article_text:
        return None

    system = (
        "You are a fact extraction system. Extract ONLY verifiable factual claims "
        "from the article text. No opinions, predictions, or emotional framing. "
        "Preserve uncertainty markers (e.g., 'according to', 'reportedly'). "
        "Return a JSON array of objects with keys: who, what, when, where, "
        "numbers (array of strings), direct_quotes (array of strings), "
        "uncertainty (string, empty if certain)."
    )
    # Truncate very long articles to avoid exceeding LLM context window
    max_chars = 8000
    if len(article_text) > max_chars:
        article_text = article_text[:max_chars] + "\n[truncated]"
    prompt = f"/no_think\nTitle: {title}\n\nArticle:\n{article_text}\n\nExtract all verifiable factual claims as JSON array:"

    result = generate(prompt, system=system)
    if not result:
        return None

    try:
        # Try to parse the JSON from the response
        # Strip thinking tags from qwen3
        cleaned = result.strip()
        if "</think>" in cleaned:
            cleaned = cleaned.split("</think>")[-1].strip()
        # LLMs sometimes wrap in markdown code blocks
        if cleaned.startswith("```"):
            lines = cleaned.split("\n")
            cleaned = "\n".join(lines[1:-1] if lines[-1].strip() == "```" else lines[1:])
        claims = json.loads(cleaned)
        if isinstance(claims, list):
            return claims
    except (json.JSONDecodeError, ValueError):
        logger.warning("Failed to parse LLM claim extraction output")
    return None


def _classify_ai_heuristic(headline: str, summary: str = "") -> bool:
    """Determine if an article is genuinely about AI using fast heuristic classification.

    Returns True if the article is substantively about AI/ML, False if not.
    """
    import re

    text = f"{headline} {summary}".lower()

    # Strong AI indicators — if headline contains these, it's almost certainly AI
    strong_ai_terms = [
        "artificial intelligence", "machine learning", "deep learning", "neural network",
        "large language model", "generative ai", "gen ai", "ai model", "ai system",
        "ai safety", "ai regulation", "ai policy", "ai governance", "ai chip",
        "chatgpt", "gpt-4", "gpt-5", "llama", "ai startup", "ai company",
        "ai training", "ai inference", "ai agent", "ai assistant",
        "text-to-image", "text-to-video", "ai-generated", "ai-powered",
    ]

    # AI company names — substantive if in the headline
    ai_companies = [
        "openai", "anthropic", "deepmind", "hugging face", "huggingface",
        "stability ai", "cohere", "mistral ai",
    ]

    # AI product names — substantive if in the headline
    ai_products = ["chatgpt", "gpt-4", "gpt-5", "github copilot"]

    headline_lower = headline.lower()

    # Check strong AI terms in headline — high confidence
    for term in strong_ai_terms:
        if term in headline_lower:
            return True

    # Check AI companies in headline — usually substantive
    for company in ai_companies:
        if company in headline_lower:
            return True

    # Check AI products in headline
    for product in ai_products:
        if product in headline_lower:
            return True

    # Check for " ai " as a standalone word in headline (not part of another word)
    if re.search(r'\bai\b', headline_lower) and len(headline_lower) < 200:
        # Count AI context signals in the full text
        ai_context_words = [
            "model", "training", "data", "algorithm", "robot", "automat",
            "compute", "gpu", "inference", "deploy", "tech", "startup",
            "regulation", "bias", "ethics", "research", "lab",
        ]
        context_count = sum(1 for w in ai_context_words if w in text)
        if context_count >= 1:
            return True

    # Broad match in summary only — lower confidence, require more AI signals
    ai_signal_count = sum(1 for t in strong_ai_terms if t in text)
    company_in_text = any(c in text for c in ai_companies)
    if ai_signal_count >= 2 or (ai_signal_count >= 1 and company_in_text):
        return True

    return False


def _classify_ai_llm(headline: str, summary: str = "") -> Optional[bool]:
    """Use LLM to determine if an article is genuinely about AI.

    Returns True/False if LLM gives a clear answer, None if unavailable or ambiguous.
    Uses a longer timeout since this is typically called from background tasks.
    """
    if not is_available():
        return None
    system = (
        "You are a strict AI news classifier. Determine if the given article is "
        "SUBSTANTIVELY about artificial intelligence, machine learning, AI companies, "
        "AI products, AI infrastructure (chips, data centers for AI), or AI policy/regulation. "
        "Articles that merely MENTION AI in passing but are primarily about something else "
        "(e.g., an oil price article that mentions AI demand) should be classified as NOT AI. "
        "Respond with ONLY 'YES' or 'NO'. No explanation."
    )
    context = f"/no_think\nHeadline: {headline}"
    if summary:
        context += f"\nSummary: {summary[:300]}"

    try:
        payload = {
            "model": OLLAMA_MODEL,
            "prompt": context,
            "system": system,
            "stream": False,
            "options": {"num_predict": 20, "temperature": 0},
        }
        resp = httpx.post(
            f"{OLLAMA_BASE_URL}/api/generate",
            json=payload,
            timeout=max(OLLAMA_TIMEOUT, 120),  # Allow longer for classification
        )
        if resp.status_code != 200:
            return None
        result = resp.json().get("response", "").strip()
    except Exception as e:
        logger.debug("AI classification LLM error: %s", e)
        return None

    if not result:
        return None
    answer = result.upper()
    # Handle thinking tags from qwen3
    if "</think>" in answer:
        answer = answer.split("</think>")[-1].strip()
    answer = answer.strip()
    if answer.startswith("YES"):
        return True
    if answer.startswith("NO"):
        return False
    return None


def classify_ai_relevance(headline: str, summary: str = "") -> Optional[bool]:
    """Determine if an article is genuinely about AI.

    Uses fast heuristic for real-time classification. LLM-based classification
    is available via _classify_ai_llm() for background re-classification tasks
    that can tolerate higher latency.
    """
    return _classify_ai_heuristic(headline, summary)


def classify_ai_relevance_llm(headline: str, summary: str = "") -> Optional[bool]:
    """LLM-based AI classification for background re-classification.

    Tries LLM first for higher accuracy, falls back to heuristic.
    Suitable for batch/background processing, not real-time requests.
    """
    llm_result = _classify_ai_llm(headline, summary)
    if llm_result is not None:
        return llm_result
    return _classify_ai_heuristic(headline, summary)


def generate_neutral_headline(title: str, summary: str = "") -> Optional[str]:
    """Generate a neutral, factual headline using LLM.

    Returns None if Ollama unavailable (caller should fall back to rule-based).
    """
    system = (
        "You are a neutral news headline writer. Rewrite the given headline to be "
        "purely factual, removing all sensationalism, emotional language, loaded "
        "adjectives, and opinion. Keep it concise (under 15 words). "
        "Return ONLY the headline, no quotes or explanation."
    )
    context = f"/no_think\nOriginal headline: {title}"
    if summary:
        context += f"\nContext: {summary}"
    return generate(context, system=system)
