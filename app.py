import os
import subprocess
from fastapi import FastAPI

app = FastAPI(title="Knowledge Worker", version="1.0.0")


@app.get("/")
def root():
    return {"ok": True, "service": "knowledge-worker"}


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