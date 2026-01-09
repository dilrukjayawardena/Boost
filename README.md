# Boost

Lightweight, modular ETL (Extract → Transform → Load) utilities and examples built with Polars, adapters, and a small orchestrator.

**Key points**
- Extractors for CSV and JSON sources
- Transformers with pl/Polars helpers
- Loaders supporting database target
- Example tasks and a simple orchestrator in `app/main.py`

**Features**
- Pluggable adapters (extractors/loaders/transformers)
- Example configurations for moving JSON/CSV into a database
- Utilities for encryption, timestamp conversion, and value handling

**Repository structure (high-level)**
- [app/main.py](app/main.py) — example entrypoint that runs sample ETL jobs
- [app/src/etl/orchestrator.py](app/src/etl/orchestrator.py) — central orchestrator that executes ETL flows
- [app/src/adapters/extractors](app/src/adapters/extractors) — CSV/JSON extractor implementations
- [app/src/adapters/loaders](app/src/adapters/loaders) — Database and file loader implementations
- [app/src/domain/entities.py](app/src/domain/entities.py) — `ETLConfig`, enums and domain models
- [app/src/data](app/src/data) — sample `test.csv` and `test.json`
- [app/src/tests](app/src/tests) — unit tests
- `requirements.txt` — Python dependencies

Prerequisites
- Python 3.9+ (or your preferred Python 3.x)
- Docker & Docker Compose (optional, for running the project inside containers)
- Recommended: create a virtual environment for local runs
- Copy two data files to app/src/data location before running
Quickstart — Docker (recommended for reproducible environment)
1. From the repository root, start services:

```bash
docker compose up -d
```

2. To find the Jupyter Notebook URL/token produced by the API container, check the API service logs:

```bash
docker compose logs api --follow
# look for the notebook URL and token in the output
```

3. Stop and remove containers and volumes:

```bash
docker compose down -v
```


