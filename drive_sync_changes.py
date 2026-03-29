import os
import json
import tempfile
import psycopg
from googleapiclient.discovery import build
from google.oauth2 import service_account


DB_URL = os.getenv("SUPABASE_DB_URL")
SERVICE_ACCOUNT_JSON = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")


def require_env(name: str, value: str | None) -> str:
    if not value:
        raise RuntimeError(f"Missing required env var: {name}")
    return value


def build_drive_client():
    service_account_json = require_env("GOOGLE_SERVICE_ACCOUNT_JSON", SERVICE_ACCOUNT_JSON)

    try:
        json.loads(service_account_json)
    except Exception as e:
        raise RuntimeError(f"GOOGLE_SERVICE_ACCOUNT_JSON is not valid JSON: {e}")

    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        f.write(service_account_json)
        creds_path = f.name

    creds = service_account.Credentials.from_service_account_file(
        creds_path,
        scopes=["https://www.googleapis.com/auth/drive.readonly"]
    )
    return build("drive", "v3", credentials=creds)


def main():
    db_url = require_env("SUPABASE_DB_URL", DB_URL)
    drive = build_drive_client()

    conn = psycopg.connect(db_url)
    cur = conn.cursor()

    try:
        cur.execute("select page_token from drive_sync_state where id = 1")
        row = cur.fetchone()

        if row and row[0]:
            token = row[0]
        else:
            token = drive.changes().getStartPageToken().execute()["startPageToken"]

        changes = drive.changes().list(pageToken=token).execute()
        new_token = changes.get("newStartPageToken") or token

        cur.execute("""
            insert into drive_sync_state (id, page_token, last_sync_at)
            values (1, %s, now())
            on conflict (id)
            do update set page_token = excluded.page_token, last_sync_at = now()
        """, (new_token,))

        conn.commit()
        print("Drive sync state updated successfully.")

    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()