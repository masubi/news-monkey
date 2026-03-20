# News Monkey

A news aggregation and analysis platform that ingests from multiple sources, deduplicates content, scores sensationalism, and surfaces gaps between social and traditional media coverage.

## Features

- **Multi-source ingestion** — RSS feeds, NewsAPI, Hacker News, Reddit, Bluesky, Mastodon, Twitter/X, TikTok, Instagram
- **Market data** — Polymarket, Kalshi, and Callsheet integration with unusual bet detection
- **Deduplication** — SimHash fingerprinting, entity Jaccard similarity, and vector cosine similarity
- **Sensationalism scoring** — Detects emotional language, clickbait patterns, and generates neutral headlines
- **Semantic search** — Vector store with approximate nearest neighbor (LSH) for clustering related stories
- **Fact extraction** — LLM-powered claim extraction via local Ollama models
- **Dashboard** — News feed, market anomalies, source status, social vs traditional gaps, AI insights

## Quick Start

```bash
# Clone and setup
git clone https://github.com/masubi/news-monkey.git
cd news-monkey
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# Configure
cp .env.example .env
# Edit .env with your API keys (NEWSAPI_KEY, KALSHI_API_KEY, etc.)

# Start Ollama (required for embeddings and fact extraction)
ollama pull qwen3:4b
ollama pull nomic-embed-text

# Run
./start.sh
```

The dashboard will be available at `http://localhost:8001`.

## Docker

```bash
docker-compose up -d
```

This starts separate services for ingestion, embedding, fact extraction, API, frontend, and Ollama.

## Configuration

Copy `.env.example` to `.env` and customize. Environment variables override `.env` values. See `.env.example` for all available options including:

- API keys (NewsAPI, Kalshi)
- Ollama model and endpoint settings
- Poll intervals for each data source
- Custom RSS feeds and subreddits

## Project Structure

```
news-monkey/
├── app.py              # FastAPI app, routes, WebSocket
├── config.py           # Loads .env into os.environ
├── database.py         # SQLite schema and queries
├── ingestion.py        # Data source polling and scraping
├── processing.py       # Dedup, scoring, vectors, clustering
├── ollama_client.py    # Ollama LLM client
├── models.py           # Pydantic models
├── static/             # Dashboard frontend (HTML/JS/CSS)
├── tests/              # Unit and browser tests
├── data/               # SQLite DB, vector store, logs (gitignored)
├── .env.example        # Configuration template
├── docker-compose.yml  # Multi-service Docker setup
├── start.sh            # Start the app
└── stop.sh             # Stop the app
```

## License

MIT
