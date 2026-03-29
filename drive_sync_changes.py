import os
import tempfile
from googleapiclient.discovery import build
from google.oauth2 import service_account
import psycopg

DB_URL = os.getenv("SUPABASE_DB_URL")
CREDS_PATH = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

SERVICE_ACCOUNT_JSON = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")

with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
    f.write(SERVICE_ACCOUNT_JSON)
    creds_path = f.name

creds = service_account.Credentials.from_service_account_file(
    creds_path,
    scopes=["https://www.googleapis.com/auth/drive.readonly"]
)

drive = build("drive", "v3", credentials=creds)

def main():
    conn = psycopg.connect(DB_URL)
    cur = conn.cursor()

    cur.execute("select page_token from drive_sync_state where id = 1")
    row = cur.fetchone()

    if row and row[0]:
        token = row[0]
    else:
        token = drive.changes().getStartPageToken().execute()["startPageToken"]

    changes = drive.changes().list(pageToken=token).execute()

    new_token = changes.get("newStartPageToken")

    cur.execute("""
        insert into drive_sync_state (id, page_token, last_sync_at)
        values (1, %s, now())
        on conflict (id)
        do update set page_token = excluded.page_token, last_sync_at = now()
    """, (new_token,))

    conn.commit()
    cur.close()
    conn.close()

if __name__ == "__main__":
    main()