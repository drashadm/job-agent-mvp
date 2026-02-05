# job-agent-mvp

Minimal MVP for automating job applications.

This repository ingests job descriptions, uses OpenAI to parse and score jobs, reads/writes records to Airtable, and logs events for auditability.

Setup

1. Copy `.env.example` to `.env` and populate values.
2. Install dependencies:

```
pip install -r requirements.txt
```

Run

From the repository root run the CLI help:

```
python src/main.py --help
```

To run the pipeline:

```
python src/main.py run --url https://example.com/job --file job_description.txt
```
