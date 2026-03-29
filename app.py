from fastapi import FastAPI
import subprocess

app = FastAPI()

@app.get("/health")
def health():
    return {"ok": True}

@app.post("/ingest")
def ingest():
    result = subprocess.run(
        ["python", "knowledge_ingest_from_drive.py"],
        capture_output=True,
        text=True
    )
    return {
        "returncode": result.returncode,
        "stdout": result.stdout,
        "stderr": result.stderr
    }

@app.post("/sync")
def sync():
    result = subprocess.run(
        ["python", "drive_sync_changes.py"],
        capture_output=True,
        text=True
    )
    return {
        "returncode": result.returncode,
        "stdout": result.stdout,
        "stderr": result.stderr
    }