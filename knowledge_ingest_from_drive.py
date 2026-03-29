import json
import tempfile
import os
import psycopg
from googleapiclient.discovery import build
from google.oauth2 import service_account
from pypdf import PdfReader

DB_URL = os.getenv("SUPABASE_DB_URL")
FOLDER_ID = os.getenv("GOOGLE_DRIVE_FOLDER_ID")
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

def extract_pdf_text(file_path):
    reader = PdfReader(file_path)
    text = ""
    for page in reader.pages:
        text += page.extract_text() or ""
    return text

def chunk_text(text, size=500):
    words = text.split()
    chunks = []
    for i in range(0, len(words), size):
        chunks.append(" ".join(words[i:i+size]))
    return chunks

def main():
    conn = psycopg.connect(DB_URL)
    cur = conn.cursor()

    results = drive.files().list(
        q=f"'{FOLDER_ID}' in parents and mimeType='application/pdf'",
        fields="files(id, name)"
    ).execute()

    files = results.get("files", [])

    for f in files:
        file_id = f["id"]
        file_name = f["name"]

        request = drive.files().get_media(fileId=file_id)
        file_path = f"/tmp/{file_name}"

        with open(file_path, "wb") as fh:
            fh.write(request.execute())

        text = extract_pdf_text(file_path)

        cur.execute("""
            insert into knowledge_files (file_id, file_name, source, extracted_text)
            values (%s, %s, %s, %s)
            returning id
        """, (file_id, file_name, "drive", text))

        file_db_id = cur.fetchone()[0]

        cur.execute("""
            insert into knowledge_documents (file_id, title, content)
            values (%s, %s, %s)
            returning id
        """, (file_id, file_name, text))

        doc_id = cur.fetchone()[0]

        chunks = chunk_text(text)

        for i, chunk in enumerate(chunks):
            cur.execute("""
                insert into knowledge_chunks (document_id, chunk_index, content)
                values (%s, %s, %s)
            """, (doc_id, i, chunk))

        conn.commit()

    cur.close()
    conn.close()

if __name__ == "__main__":
    main()