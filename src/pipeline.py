import os
import json
import logging
from urllib.parse import quote

import pandas as pd
import requests
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)
logger = logging.getLogger(__name__)

GITLAB_TOKEN = os.getenv("GITLAB_TOKEN")
GITLAB_GRAPHQL_URL = os.getenv("GITLAB_GRAPHQL_URL", "https://gitlab.com/api/graphql")
GITLAB_REST_URL = os.getenv("GITLAB_REST_URL", "https://gitlab.com/api/v4")
DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL is missing, check .env")
if not GITLAB_TOKEN:
    logger.warning("No GITLAB_TOKEN, rate limits may apply")

SOURCE = "gitlab"
MAX_ISSUES_PER_PROJECT = 500
PROJECTS = ["gitlab-org/gitlab-runner", "gitlab-org/gitlab"]

session = requests.Session()
session.headers.update({
    "User-Agent": "compliant-api-research-pipeline/1.0",
    "Content-Type": "application/json",
})
if GITLAB_TOKEN:
    session.headers["Authorization"] = f"Bearer {GITLAB_TOKEN}"

retry_strategy = Retry(
    total=3,
    backoff_factor=1,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET", "POST"]
)
adapter = HTTPAdapter(max_retries=retry_strategy)
session.mount("https://", adapter)


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type((requests.RequestException, RuntimeError)),
    reraise=True
)
def gql(query: str, variables: dict | None = None) -> dict:
    resp = session.post(
        GITLAB_GRAPHQL_URL,
        json={"query": query, "variables": variables or {}},
        timeout=30,
    )
    resp.raise_for_status()
    payload = resp.json()
    if payload.get("errors"):
        raise RuntimeError(payload["errors"])
    return payload["data"]


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type(requests.RequestException),
    reraise=True
)
def rest_get(path: str, params: dict | None = None):
    resp = session.get(f"{GITLAB_REST_URL}{path}", params=params, timeout=30)
    resp.raise_for_status()
    return resp.json(), resp.headers


def ensure_schema(engine):
    ddl = """
    CREATE TABLE IF NOT EXISTS projects (
        source TEXT NOT NULL,
        project_id BIGINT NOT NULL,
        full_path TEXT NOT NULL,
        name TEXT,
        description TEXT,
        web_url TEXT,
        created_at TIMESTAMP NULL,
        updated_at TIMESTAMP NULL,
        PRIMARY KEY (source, project_id)
    );
    CREATE TABLE IF NOT EXISTS freelancers (
        source TEXT NOT NULL,
        source_user_id BIGINT NOT NULL,
        username TEXT,
        name TEXT,
        profile_url TEXT,
        skills_summary TEXT,
        first_seen_at TIMESTAMP NULL,
        updated_at TIMESTAMP NULL,
        PRIMARY KEY (source, source_user_id)
    );
    CREATE TABLE IF NOT EXISTS jobs (
        source TEXT NOT NULL,
        source_project_full_path TEXT NOT NULL,
        source_job_iid INTEGER NOT NULL,
        source_job_id BIGINT NOT NULL,
        title TEXT,
        state TEXT,
        labels TEXT,
        web_url TEXT,
        author_username TEXT,
        created_at TIMESTAMP NULL,
        updated_at TIMESTAMP NULL,
        PRIMARY KEY (source, source_project_full_path, source_job_iid)
    );
    CREATE TABLE IF NOT EXISTS work_logs (
        source TEXT NOT NULL,
        source_project_full_path TEXT NOT NULL,
        source_job_iid INTEGER NOT NULL,
        total_time_spent_seconds BIGINT,
        total_time_spent_hours DOUBLE PRECISION,
        time_stats_json TEXT,
        collected_at TIMESTAMP NULL,
        PRIMARY KEY (source, source_project_full_path, source_job_iid)
    );
    """
    with engine.begin() as conn:
        for stmt in ddl.split(";"):
            sql = stmt.strip()
            if sql:
                conn.execute(text(sql))


def to_ts(val):
    """Convert GitLab date string to pandas Timestamp or None."""
    if val is None:
        return None
    if isinstance(val, pd.Timestamp):
        return val
    try:
        return pd.to_datetime(val, errors="coerce")
    except Exception:
        return None


def fetch_project_meta(full_path: str) -> dict:
    data = gql(
        """query($fullPath: ID!) { project(fullPath: $fullPath) { id fullPath } }""",
        {"fullPath": full_path},
    )
    if not data.get("project"):
        raise ValueError(f"Project not found: {full_path}")
    encoded = quote(full_path, safe="")
    project, _ = rest_get(f"/projects/{encoded}")
    return project


def fetch_issues(project_id: int, max_issues: int = None) -> list[dict]:
    all_rows = []
    page = 1
    while True:
        logger.info(f"  Fetching issues page {page}...")
        rows, headers = rest_get(
            f"/projects/{project_id}/issues",
            params={"per_page": 100, "page": page, "state": "all"},
        )
        if not rows:
            break
        all_rows.extend(rows)
        logger.info(f"  Page {page}: {len(rows)} issues (total so far: {len(all_rows)})")
        if max_issues and len(all_rows) >= max_issues:
            logger.info(f"  Reached cap of {max_issues}, stopping pagination")
            break
        if headers.get("X-Next-Page"):
            page += 1
        else:
            break
    logger.info(f"  Total issues fetched: {len(all_rows)}")
    return all_rows


def fetch_time_stats(project_id: int, issue_iid: int) -> dict:
    data, _ = rest_get(f"/projects/{project_id}/issues/{issue_iid}/time_stats")
    return data


def build_frames(projects: list[str], max_issues_per_project: int = None):
    project_rows = []
    freelancer_rows = []
    job_rows = []
    work_rows = []
    seen_freelancers = set()

    for full_path in projects:
        logger.info(f"Processing project: {full_path}")
        try:
            project = fetch_project_meta(full_path)
        except Exception as e:
            logger.error(f"Failed to fetch project {full_path}: {e}")
            continue

        project_id = int(project["id"])
        project_rows.append({
            "source": SOURCE,
            "project_id": project_id,
            "full_path": project.get("path_with_namespace") or project.get("fullPath") or full_path,
            "name": project.get("name"),
            "description": project.get("description"),
            "web_url": project.get("web_url"),
            "created_at": to_ts(project.get("created_at")),
            "updated_at": to_ts(project.get("last_activity_at") or project.get("updated_at")),
        })

        try:
            issues = fetch_issues(project_id, max_issues=max_issues_per_project)
        except Exception as e:
            logger.error(f"Failed to fetch issues for {full_path}: {e}")
            continue

        logger.info(f"Processing {len(issues)} issues for time stats...")
        for i, issue in enumerate(issues):
            if i % 100 == 0:
                logger.info(f"  Time stats progress: {i}/{len(issues)}")

            author = issue.get("author") or {}
            labels = issue.get("labels") or []
            labels_text = ", ".join(sorted({x.strip() for x in labels if x and x.strip()})) or None

            job_rows.append({
                "source": SOURCE,
                "source_project_full_path": full_path,
                "source_job_iid": int(issue["iid"]),
                "source_job_id": int(issue["id"]),
                "title": issue.get("title"),
                "state": issue.get("state"),
                "labels": labels_text,
                "web_url": issue.get("web_url"),
                "author_username": author.get("username"),
                "created_at": to_ts(issue.get("created_at")),
                "updated_at": to_ts(issue.get("updated_at")),
            })

            if author.get("id") and author["id"] not in seen_freelancers:
                seen_freelancers.add(author["id"])
                freelancer_rows.append({
                    "source": SOURCE,
                    "source_user_id": int(author["id"]),
                    "username": author.get("username"),
                    "name": author.get("name"),
                    "profile_url": author.get("web_url"),
                    "skills_summary": labels_text,
                    "first_seen_at": to_ts(issue.get("created_at")),
                    "updated_at": to_ts(issue.get("updated_at")),
                })

            try:
                time_stats = fetch_time_stats(project_id, int(issue["iid"]))
            except Exception as e:
                logger.warning(f"Missing time stats for issue {issue['iid']}: {e}")
                continue

            seconds = time_stats.get("total_time_spent_seconds") or time_stats.get("total_time_spent") or 0
            work_rows.append({
                "source": SOURCE,
                "source_project_full_path": full_path,
                "source_job_iid": int(issue["iid"]),
                "total_time_spent_seconds": int(seconds),
                "total_time_spent_hours": round(int(seconds) / 3600, 4),
                "time_stats_json": json.dumps(time_stats),
                "collected_at": pd.Timestamp.utcnow(),
            })

    projects_df = pd.DataFrame(project_rows).drop_duplicates(subset=["source", "project_id"])
    freelancers_df = pd.DataFrame(freelancer_rows).drop_duplicates(subset=["source", "source_user_id"])
    jobs_df = pd.DataFrame(job_rows).drop_duplicates(subset=["source", "source_project_full_path", "source_job_iid"])
    work_df = pd.DataFrame(work_rows).drop_duplicates(subset=["source", "source_project_full_path", "source_job_iid"])

    return projects_df, freelancers_df, jobs_df, work_df


def upsert_df(engine, df: pd.DataFrame, table: str, conflict_cols: list[str]):
    if df.empty:
        return
    stage = f"stg_{table}"
    df.to_sql(stage, engine, if_exists="replace", index=False)
    cols = list(df.columns)
    insert_cols = ", ".join(cols)
    select_cols = ", ".join(cols)
    conflict = ", ".join(conflict_cols)
    update_cols = [c for c in cols if c not in conflict_cols]
    if update_cols:
        set_clause = ", ".join([f"{c}=EXCLUDED.{c}" for c in update_cols])
        sql = f"INSERT INTO {table} ({insert_cols}) SELECT {select_cols} FROM {stage} ON CONFLICT ({conflict}) DO UPDATE SET {set_clause};"
    else:
        sql = f"INSERT INTO {table} ({insert_cols}) SELECT {select_cols} FROM {stage} ON CONFLICT ({conflict}) DO NOTHING;"
    with engine.begin() as conn:
        conn.execute(text(sql))
        conn.execute(text(f"DROP TABLE IF EXISTS {stage}"))


def main():
    engine = create_engine(DATABASE_URL, future=True)
    ensure_schema(engine)
    projects_df, freelancers_df, jobs_df, work_df = build_frames(PROJECTS, MAX_ISSUES_PER_PROJECT)
    upsert_df(engine, projects_df, "projects", ["source", "project_id"])
    upsert_df(engine, freelancers_df, "freelancers", ["source", "source_user_id"])
    upsert_df(engine, jobs_df, "jobs", ["source", "source_project_full_path", "source_job_iid"])
    upsert_df(engine, work_df, "work_logs", ["source", "source_project_full_path", "source_job_iid"])
    logger.info(f"Done. projects={len(projects_df)} freelancers={len(freelancers_df)} jobs={len(jobs_df)} work_logs={len(work_df)}")


if __name__ == "__main__":
    main()
