# Compliant API Research Data Pipeline

**Production-grade ETL from official APIs — no scraping, fully auditable.**

## What This Proves
- Uses only official REST + GraphQL endpoints (GitLab API)
- Handles pagination, rate limiting, and transient failures with exponential backoff
- Loads data via idempotent upserts (safe to rerun)
- Maps to a clean dimensional model ready for analytics

## Data Model
| Table | Rows | Description |
|-------|------|-------------|
| freelancer_profiles | 567 | Freelancer characteristics (username, skills) |
| job_listings | 1,013 | Job attributes (title, status, categories) |
| work_diary | 1,000 | Aggregated hours logged per job |
| projects | 2 | Data sources (multi-project capable) |

## Tech Stack
- Python 3.11 · Pandas · SQLAlchemy · PostgreSQL
- tenacity (retry logic) · python-dotenv (secrets)
- GitLab REST API v4 + GraphQL API

## Compliance & Ethics
- All data accessed via **official GitLab REST API v4 and GraphQL API**
- Authenticated using a **personal access token** with `rread_api` scope only
- Only **publicly available project data** is collected
- No scraping, no browser automation, no terms-of-service violations
- Full audit trail: every record traces back to a specific API endpoint
- Token stored in `.env`, never committed to version control

## Setup
1. Clone repo
2. Create conda env: `conda create -n compliant-pipeline python=3.11 -y`
3. Activate: `conda activate compliant-pipeline`
4. Install: `pip install -r requirements.txt`
5. Install PostgreSQL, create database `research_db` and user `pipeline_user`
6. Copy `.env.example` to `.env` and fill in your GitLab token + DB URL
7. Run: `python src/pipeline.py`
