from fastapi import FastAPI, BackgroundTasks
import subprocess
import os

app = FastAPI(title="Knowledge Worker", version="1.0.0")

last_runs = {
    "ingest": {"status": "idle", "stdout": "", "stderr": "", "returncode": None},
    "sync": {"status": "idle", "stdout": "", "stderr": "", "returncode": None},
}

def run_ingest():
    global last_runs
    last_runs["ingest"] = {"status": "running", "stdout": "", "stderr": "", "returncode": None}
    result = subprocess.run(
        ["python", "knowledge_ingest_from_drive.py"],
        capture_output=True,
        text=True
    )
    last_runs["ingest"] = {
        "status": "finished" if result.returncode == 0 else "failed",
        "stdout": result.stdout,
        "stderr": result.stderr,
        "returncode": result.returncode
    }

def run_sync():
    global last_runs
    last_runs["sync"] = {"status": "running", "stdout": "", "stderr": "", "returncode": None}
    result = subprocess.run(
        ["python", "drive_sync_changes.py"],
        capture_output=True,
        text=True
    )
    last_runs["sync"] = {
        "status": "finished" if result.returncode == 0 else "failed",
        "stdout": result.stdout,
        "stderr": result.stderr,
        "returncode": result.returncode
    }

@app.get("/")
def root():
    return {"ok": True, "service": "knowledge-worker"}

@app.get("/health")
def health():
    return {
        "ok": True,
        "env_check": {
            "SUPABASE_DB_URL": bool(os.getenv("SUPABASE_DB_URL")),
            "GOOGLE_DRIVE_FOLDER_ID": bool(os.getenv("GOOGLE_DRIVE_FOLDER_ID")),
            "GOOGLE_SERVICE_ACCOUNT_JSON_BASE64": bool(os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON_BASE64")),
        }
    }

@app.post("/ingest")
def ingest(background_tasks: BackgroundTasks):
    if last_runs["ingest"]["status"] == "running":
        return {"started": False, "message": "ingest already running"}
    background_tasks.add_task(run_ingest)
    return {"started": True, "message": "ingest started"}

@app.get("/ingest/status")
def ingest_status():
    return last_runs["ingest"]

@app.post("/sync")
def sync(background_tasks: BackgroundTasks):
    if last_runs["sync"]["status"] == "running":
        return {"started": False, "message": "sync already running"}
    background_tasks.add_task(run_sync)
    return {"started": True, "message": "sync started"}

@app.get("/sync/status")
def sync_status():
    return last_runs["sync"]