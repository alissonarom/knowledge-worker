import os
import json
import base64
import tempfile
import psycopg
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from google.oauth2 import service_account
from pypdf import PdfReader


DB_URL = os.getenv("SUPABASE_DB_URL")
FOLDER_ID = os.getenv("GOOGLE_DRIVE_FOLDER_ID")
SERVICE_ACCOUNT_JSON_BASE64 = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON_BASE64")


def require_env(name: str, value: str | None) -> str:
    if not value:
        raise RuntimeError(f"Missing required env var: {name}")
    return value


def build_drive_client():
    b64 = require_env("GOOGLE_SERVICE_ACCOUNT_JSON_BASE64", SERVICE_ACCOUNT_JSON_BASE64)

    try:
        decoded = base64.b64decode(b64).decode("utf-8")
        json.loads(decoded)
    except Exception as e:
        raise RuntimeError(f"GOOGLE_SERVICE_ACCOUNT_JSON_BASE64 is invalid: {e}")

    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        f.write(decoded)
        creds_path = f.name

    creds = service_account.Credentials.from_service_account_file(
        creds_path,
        scopes=["https://www.googleapis.com/auth/drive.readonly"]
    )
    return build("drive", "v3", credentials=creds)


def extract_pdf_text(file_path: str) -> str:
    reader = PdfReader(file_path)
    text = ""
    for page in reader.pages:
        text += page.extract_text() or ""
        text += "\n"
    return text.strip()


def chunk_text(text: str, size: int = 500) -> list[str]:
    words = text.split()
    chunks = []
    for i in range(0, len(words), size):
        chunk = " ".join(words[i:i + size]).strip()
        if chunk:
            chunks.append(chunk)
    return chunks


def download_drive_file(drive, file_id: str, target_path: str):
    request = drive.files().get_media(fileId=file_id)
    with open(target_path, "wb") as fh:
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            _, done = downloader.next_chunk()


def upsert_knowledge_file(cur, file_id: str, file_name: str, extracted_text: str):
    cur.execute("""
        insert into knowledge_files (
            file_id,
            file_name,
            source,
            doc_kind,
            trail,
            extracted_text,
            metadata,
            created_at
        )
        values (%s, %s, %s, %s, %s, %s, %s::jsonb, now())
        on conflict (file_id) do update set
            file_name = excluded.file_name,
            source = excluded.source,
            extracted_text = excluded.extracted_text,
            metadata = excluded.metadata
        returning id
    """, (
        file_id,
        file_name,
        "google_drive",
        "pdf",
        "vectorized",
        extracted_text,
        json.dumps({"file_name": file_name})
    ))
    return cur.fetchone()[0]


def replace_document_and_chunks(cur, file_id: str, file_name: str, extracted_text: str):
    cur.execute("""
        delete from knowledge_chunks
        where document_id in (
            select id from knowledge_documents where file_id = %s
        )
    """, (file_id,))

    cur.execute("delete from knowledge_documents where file_id = %s", (file_id,))

    cur.execute("""
        insert into knowledge_documents (
            file_id,
            title,
            content,
            metadata,
            created_at
        )
        values (%s, %s, %s, %s::jsonb, now())
        returning id
    """, (
        file_id,
        file_name,
        extracted_text,
        json.dumps({
            "source": "google_drive",
            "file_name": file_name,
            "trail": "vectorized"
        })
    ))
    doc_id = cur.fetchone()[0]

    chunks = chunk_text(extracted_text, size=500)

    for idx, chunk in enumerate(chunks):
        cur.execute("""
            insert into knowledge_chunks (
                document_id,
                chunk_index,
                content,
                metadata,
                created_at
            )
            values (%s, %s, %s, %s::jsonb, now())
        """, (
            doc_id,
            idx,
            chunk,
            json.dumps({
                "file_id": file_id,
                "file_name": file_name,
                "chunk_index": idx
            })
        ))

    return doc_id, len(chunks)


def main():
    db_url = require_env("SUPABASE_DB_URL", DB_URL)
    folder_id = require_env("GOOGLE_DRIVE_FOLDER_ID", FOLDER_ID)

    drive = build_drive_client()

    results = drive.files().list(
        q=f"'{folder_id}' in parents and mimeType='application/pdf' and trashed=false",
        fields="files(id, name, modifiedTime)"
    ).execute()

    files = results.get("files", [])

    if not files:
        print("No PDF files found in the folder.")
        return

    conn = psycopg.connect(db_url)
    cur = conn.cursor()

    processed = 0

    try:
        for f in files:
            file_id = f["id"]
            file_name = f["name"]

            safe_name = file_name.replace("/", "_")
            file_path = f"/tmp/{safe_name}"

            print(f"Processing: {file_name} ({file_id})")

            download_drive_file(drive, file_id, file_path)
            text = extract_pdf_text(file_path)

            if not text.strip():
                print(f"Skipping empty PDF text: {file_name}")
                continue

            upsert_knowledge_file(cur, file_id, file_name, text)
            _, chunk_count = replace_document_and_chunks(cur, file_id, file_name, text)

            conn.commit()
            processed += 1
            print(f"Done: {file_name} | chunks={chunk_count}")

        print(f"Processed files: {processed}")

    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()