# Compliant API Research Pipeline — Project Summary

## Dataset Overview
| Table | Rows | Description |
|-------|------|-------------|
| freelancer_profiles | 567 | Freelancer characteristics (username, skills) |
| job_listings | 1,013 | Job attributes (title, status, categories) |
| work_diary | 1,000 | Aggregated hours logged per job |
| projects | 2 | Data sources (multi-project capable) |

## Technical Approach
- **APIs Used:** GitLab REST v4 + GraphQL (official, documented endpoints)
- **Language:** Python 3.11 with pandas, SQLAlchemy, tenacity
- **Database:** PostgreSQL with idempotent upsert logic
- **Compliance:** Public data only, authenticated access, no scraping

## Key Design Decisions
- Idempotent loads (safe to rerun without duplicates)
- Exponential backoff retry logic for API resilience
- Full logging and error handling
- Secrets management via environment variables
- Clean dimensional model ready for analysis

## Deliverables
- Complete Python pipeline (src/pipeline.py)
- Full dataset in PostgreSQL + Excel export
- Professional README with setup instructions
- EDA summary with data quality checks
