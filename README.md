# Enron Email Ingest Pipeline

This project ingests the **Enron email dataset** into MongoDB using an agent-based pipeline orchestrated with **Celery**.
The pipeline parses raw email files (`eml`, `msg`, or Enron-style `1_`, `98_`, etc.), normalizes them, and stores structured data in MongoDB for downstream analysis and graph visualization.

---

## Features
- **Parsing Agent**: Extracts headers, body, attachments, and metadata from raw emails.
- **Orchestrator**: Manages ingestion jobs, batches large datasets, and spawns parsing agents.
- **MongoDB Storage**: Stores normalized emails with deduplication (via `dedupe_key`).
- **Celery + Redis**: Asynchronous distributed task processing.

---

## Project Structure
```
enron_ingest/
├── app/
│   ├── agents/              # Parsing agent and (future) NER/Validation agents
│   ├── common/              # Shared utilities (db, parsing helpers)
│   ├── orchestrator.py      # Orchestrator tasks
│   └── main.py              # 
├── worker.py                # Celery worker bootstrap
├── manual_ingest.py         # Manual ingestion script for local testing
├── requirements.txt         # Python dependencies
├── .env                     # Environment variables (not pushed to GitHub)
└── README.md                # Project documentation
```

---

## Setup

### 1. Clone the repository
```bash
git clone https://github.com/<your-username>/Hierarchical-Graph-DB.git
cd enron_ingest
```

### 2. Create and activate virtual environment
```bash
python -m venv .venv
.venv\Scripts\activate   # Windows
```

### 3. Install dependencies
```bash
pip install -r requirements.txt
```

### 4. Configure `.env`
Create a `.env` file in the project root:

```
MONGODB_URI=mongodb+srv://<user>:<pass>@cluster.mongodb.net
DB_NAME=Project_Demo
COLLECTION_NAME=Emails
REDIS_URL=redis://localhost:6379/0
```

### 5. Start Redis (Docker)
```bash
docker run -d --name redis -p 6379:6379 redis
```

### 6. Run Celery Worker
```bash
celery -A worker.celery_app worker -Q orchestrator,agents -l INFO --concurrency 1 --pool=solo
```



## Example Ingestion
1. Run the manual script:

```bash
python manual_ingest.py
```

2. Check MongoDB Compass → `Project_Demo > Emails`.

---

## Next Steps
- Add NERAgent for entity extraction (spaCy/transformers).
- Add ValidationAgent for disambiguation rules.
- Build relationship/graph agent for network visualization.
- Integrate with Cytoscape.js/D3.js for UI.

---

