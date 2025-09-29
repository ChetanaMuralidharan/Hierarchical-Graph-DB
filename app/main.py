import uuid, os, datetime as dt
from fastapi import FastAPI, UploadFile, File
from pydantic import BaseModel
from app.common.db import db
from app.orchestrator import t_start_job, stage_zip_to_tmp

app = FastAPI(title="CoD Orchestrator")

class IngestResponse(BaseModel):
    job_id: str
    status: str

@app.post("/ingest", response_model=IngestResponse)
async def ingest_zip(file: UploadFile = File(...)):
    # persist upload to disk (simple MVP)
    out_path = f"/tmp/{uuid.uuid4()}_{file.filename}"
    with open(out_path, "wb") as f:
        f.write(await file.read())

    input_dir = stage_zip_to_tmp(out_path)
    job_id = str(uuid.uuid4())
    db().jobs.insert_one({
        "_id": job_id,
        "created_at": dt.datetime.utcnow(),
        "status": "QUEUED",
        "source": file.filename,
        "input_dir": input_dir,
    })
    t_start_job.delay(job_id, input_dir)
    return {"job_id": job_id, "status": "QUEUED"}

@app.get("/jobs/{job_id}")
def job_status(job_id: str):
    job = db().jobs.find_one({"_id": job_id}, {"_id": 0})
    return job or {"error": "not_found"}
