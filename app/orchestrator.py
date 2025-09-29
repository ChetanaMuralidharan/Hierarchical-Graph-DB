import glob, os, zipfile, tempfile, shutil, uuid, datetime as dt
from celery import group, chord
from worker import celery_app
from app.common.db import db
from app.agents.parsing_agent import parse_and_ingest_file

# ---- Celery task wrappers ----

@celery_app.task(name="app.agents.parsing.parse_file")
def t_parse_file(path: str, src_meta: dict):
    return parse_and_ingest_file(path, src_meta)

@celery_app.task(name="app.orchestrator.after_parse")
def t_after_parse(job_id: str, _results):
    # TODO: enqueue NER group here; for MVP we mark parsed
    db().jobs.update_one({"_id": job_id}, {"$set": {"status": "PARSED"}})
    # fan-out NER next (stub):
    # ner_tasks = group(t_ner_extract.s(email_id) for email_id in email_ids_for_job)
    # return chord(ner_tasks)(t_after_ner.s(job_id))
    return True

@celery_app.task(name="app.orchestrator.start_job")
def t_start_job(job_id: str, input_dir: str):
    job = db().jobs.find_one({"_id": job_id})
    if not job:
        return False

    tasks = []

    # mimic old ingest_tree traversal
    for user in sorted(os.listdir(input_dir)):
        user_dir = os.path.join(input_dir, user)
        if not os.path.isdir(user_dir):
            continue

        for folder in sorted(os.listdir(user_dir)):
            folder_dir = os.path.join(user_dir, folder)
            if not os.path.isdir(folder_dir):
                continue

            for filename in sorted(os.listdir(folder_dir)):
                fpath = os.path.join(folder_dir, filename)
                if not os.path.isfile(fpath):
                    continue

                src_meta = {
                    "user": user,
                    "folder": folder,
                    "filename": filename
                }
                tasks.append(t_parse_file.s(fpath, src_meta))

    if not tasks:
        db().jobs.update_one({"_id": job_id}, {"$set": {"status": "EMPTY"}})
        print(f"[ORCH] No files found in {input_dir}")
        return True

    db().jobs.update_one({"_id": job_id}, {
        "$set": {"status": "PARSING", "file_count": len(tasks)}
    })
    print(f"[ORCH] Queued {len(tasks)} files for parsing")

    return chord(group(tasks))(t_after_parse.s(job_id))

# ---- helpers to stage uploads ----

def stage_zip_to_tmp(zip_path: str) -> str:
    tmpdir = tempfile.mkdtemp(prefix="cod_ingest_")
    with zipfile.ZipFile(zip_path) as zf:
        zf.extractall(tmpdir)
    return tmpdir

def cleanup_tmp(path: str):
    shutil.rmtree(path, ignore_errors=True)
