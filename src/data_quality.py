import pandas as pd
from sqlalchemy import create_engine, text

DB_URL = "postgresql+psycopg2://pipeline_user:secure123@localhost:5433/research_db"
engine = create_engine(DB_URL)

tests = {
    "projects has rows": "SELECT COUNT(*) FROM projects",
    "freelancers has rows": "SELECT COUNT(*) FROM freelancers",
    "jobs has rows": "SELECT COUNT(*) FROM jobs",
    "work_logs has rows": "SELECT COUNT(*) FROM work_logs",
    "no null job titles": "SELECT COUNT(*) FROM jobs WHERE title IS NULL",
    "no null freelancer usernames": "SELECT COUNT(*) FROM freelancers WHERE username IS NULL",
    "work_logs reference valid jobs": """
        SELECT COUNT(*) FROM work_logs w 
        LEFT JOIN jobs j ON w.source_job_iid = j.source_job_iid 
        AND w.source_project_full_path = j.source_project_full_path 
        WHERE j.source_job_iid IS NULL
    """,
}

passed = 0
failed = 0

with engine.connect() as conn:
    for name, sql in tests.items():
        result = conn.execute(text(sql)).scalar()
        if name.startswith("no null") or "reference" in name:
            ok = result == 0
        else:
            ok = result > 0
        status = "PASS" if ok else "FAIL"
        if ok:
            passed += 1
        else:
            failed += 1
        print(f"[{status}] {name} (result: {result})")

print(f"\n{passed} passed, {failed} failed")
