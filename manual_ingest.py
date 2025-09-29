import uuid
from app.orchestrator import t_start_job
from app.common.db import db

# path to your dataset folder (allen-p with inbox/sent/etc.)
input_dir = r"C:\Users\mchet\Downloads\maildir-20250919T181827Z-1-001\maildir"


job_id = str(uuid.uuid4())
db().jobs.insert_one({
    "_id": job_id,
    "status": "QUEUED",
    "source": "manual_trigger",
    "input_dir": input_dir,
})

# directly trigger orchestrator via Celery
t_start_job.delay(job_id, input_dir)

print(f"Triggered job {job_id} for folder {input_dir}")
