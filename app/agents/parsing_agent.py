import os
from email import message_from_bytes
from app.common.db import db
from app.common.utils import smart_decode, parse_addresses, to_iso_date, content_hash
from dateutil.tz import tzutc

def parse_email_bytes(raw: bytes, src_meta: dict):
    msg = message_from_bytes(raw)
    message_id = (msg.get("Message-ID") or "").strip()
    subject = (msg.get("Subject") or "").strip()
    from_addr = parse_addresses(msg.get("From"))
    to_list   = parse_addresses(msg.get_all("To"))
    cc_list   = parse_addresses(msg.get_all("Cc"))
    bcc_list  = parse_addresses(msg.get_all("Bcc"))
    date_iso  = to_iso_date(msg.get("Date"))

    body_text = ""
    if msg.is_multipart():
        for part in msg.walk():
            if part.get_content_type() == "text/plain":
                body_text += smart_decode(part.get_payload(decode=True))
    else:
        body_text = smart_decode(msg.get_payload(decode=True))

    attachments = []
    if msg.is_multipart():
        for part in msg.walk():
            fn = part.get_filename()
            if fn:
                attachments.append({
                    "filename": fn,
                    "content_type": part.get_content_type(),
                    "size": len(part.get_payload(decode=True) or b"")
                })

    headers = {}
    for k, v in msg.items():
        key = k.lower()
        headers[key] = (headers.get(key, "") + "\n" + v).strip() if key in headers else v

    dedupe_key = message_id or content_hash({
        "from": from_addr[0] if from_addr else "",
        "to": to_list,
        "date": date_iso.isoformat() if date_iso else "",
        "subject": subject,
        "body_preview": body_text[:2000],
    })

    doc = {
        "dedupe_key": dedupe_key,
        "message_id": message_id or None,
        "date": date_iso,
        "from": from_addr[0] if from_addr else "",
        "to": to_list,
        "cc": cc_list,
        "bcc": bcc_list,
        "subject": subject,
        "body": body_text,
        "attachments": attachments,
        "mailboxes": [src_meta],  # {"user","folder","filename"} if you have it
        "headers": headers,
        "entities": [],
        "thread_id": None,
    }
    return doc

def upsert_email(doc: dict):
    col = db()["Emails"]
    # ensure indexes once (safe to call repeatedly)
    col.create_index({"dedupe_key": 1}, unique=True)
    col.create_index({"date": 1})
    col.create_index({"mailboxes.user": 1, "mailboxes.folder": 1})

    # avoid $setOnInsert + $addToSet conflict by excluding mailboxes on insert
    insert_doc = {k:v for k,v in doc.items() if k != "mailboxes"}
    res = col.update_one(
        {"dedupe_key": doc["dedupe_key"]},
        {
            "$setOnInsert": insert_doc,
            "$addToSet": {"mailboxes": doc["mailboxes"][0]},
        },
        upsert=True
    )
    created = int(bool(res.upserted_id))
    modified = 1 if (res.matched_count and res.modified_count) else 0
    return {"created": created, "modified": modified}

def parse_and_ingest_file(path: str, src_meta: dict):
    with open(path, "rb") as f:
        raw = f.read()
    doc = parse_email_bytes(raw, src_meta)
    return upsert_email(doc)
