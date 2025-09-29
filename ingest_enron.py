import os
import hashlib
import chardet
from email import message_from_bytes
from email.utils import getaddresses, parsedate_to_datetime
from datetime import datetime
from pymongo import MongoClient, UpdateOne
from pymongo.errors import DuplicateKeyError
from dateutil.tz import tzutc
from dotenv import load_dotenv
import argparse
import json

# ---------- Helpers ----------

def read_bytes(path):
    with open(path, "rb") as f:
        return f.read()

def smart_decode(b: bytes) -> str:
    """Decode bytes to str; try detected encoding, then fallbacks."""
    if not b:
        return ""
    guess = chardet.detect(b) or {}
    enc = guess.get("encoding") or "latin-1"
    try:
        return b.decode(enc, errors="ignore")
    except Exception:
        try:
            return b.decode("utf-8", errors="ignore")
        except Exception:
            return b.decode("latin-1", errors="ignore")

def parse_addresses(headers_value) -> list:
    """Normalize a header (which can be list/str/None) into a list of emails."""
    items = []
    if headers_value is None:
        return items
    if isinstance(headers_value, list):
        flat = ", ".join(headers_value)
    else:
        flat = headers_value
    for _, addr in getaddresses([flat]):
        a = addr.strip().lower()
        if a:
            items.append(a)
    return items

def to_iso_date(date_header: str):
    if not date_header:
        return None
    try:
        dt = parsedate_to_datetime(date_header)
        if dt is None:
            return None
        # normalize naive to UTC
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=tzutc())
        return dt.astimezone(tzutc())
    except Exception:
        return None

def content_hash(fields: dict) -> str:
    """Fallback dedupe key if Message-ID is missing: hash stable parts."""
    payload = json.dumps(fields, sort_keys=True, ensure_ascii=False).encode("utf-8")
    return "hash_" + hashlib.sha256(payload).hexdigest()

# ---------- Core parsing ----------

def parse_email_file(filepath: str, user: str, folder: str, filename: str) -> dict:
    raw = read_bytes(filepath)
    # Parse as RFC822 using bytes (more robust than text mode)
    msg = message_from_bytes(raw)

    message_id = (msg.get("Message-ID") or "").strip()
    subject = (msg.get("Subject") or "").strip()
    from_addr = parse_addresses(msg.get("From"))
    to_list = parse_addresses(msg.get_all("To"))
    cc_list = parse_addresses(msg.get_all("Cc"))
    bcc_list = parse_addresses(msg.get_all("Bcc"))
    date_iso = to_iso_date(msg.get("Date"))

    # Extract plain text body (prefer text/plain)
    body_text = ""
    if msg.is_multipart():
        for part in msg.walk():
            if part.get_content_type() == "text/plain":
                payload = part.get_payload(decode=True)
                body_text += smart_decode(payload)
    else:
        payload = msg.get_payload(decode=True)
        body_text = smart_decode(payload)

    # Attachments (metadata only; payload handling optional)
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

    # Keep ALL original headers as a dict (lower-cased keys for consistency)
    headers = {}
    for k, v in msg.items():
        # If duplicate header names exist, join them
        key = k.lower()
        headers[key] = (headers.get(key, "") + "\n" + v).strip() if key in headers else v

    # Canonical dedupe key
    dedupe_key = message_id or content_hash({
        "from": from_addr[0] if from_addr else "",
        "to": to_list,
        "date": date_iso.isoformat() if date_iso else "",
        "subject": subject,
        "body_preview": body_text[:2000],  # cap to keep the hash stable
    })

    doc = {
        "dedupe_key": dedupe_key,          # <-- unique
        "message_id": message_id or None,  # original id if present
        "date": date_iso,
        "from": from_addr[0] if from_addr else "",
        "to": to_list,
        "cc": cc_list,
        "bcc": bcc_list,
        "subject": subject,
        "body": body_text,
        "attachments": attachments,
        "mailboxes": [
            {"user": user, "folder": folder, "filename": filename}
        ],
        "headers": headers,
        "entities": [],
        "thread_id": None
    }
    return doc

# ---------- Ingest runner ----------

def ensure_indexes(col):
    # Unique on dedupe_key (covers Message-ID and no-ID cases)
    col.create_index({"dedupe_key": 1}, unique=True)
    # Useful query indexes
    col.create_index({"from": 1})
    col.create_index({"to": 1})
    col.create_index({"date": 1})
    col.create_index({"mailboxes.user": 1, "mailboxes.folder": 1})
    col.create_index({"entities.text": 1, "entities.type": 1})

def ingest_tree(base_dir: str, mongo_uri: str, db_name: str, coll_name: str, dry_run: bool=False, batch_size: int=100):
    client = MongoClient(mongo_uri)
    col = client[db_name][coll_name]
    ensure_indexes(col)

    ops = []
    total_seen = 0
    total_upserts = 0
    total_mailbox_merges = 0

    # Walk: /base_dir/<user>/<folder>/<files>
    for user in sorted(os.listdir(base_dir)):
        user_dir = os.path.join(base_dir, user)
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

                total_seen += 1
                try:
                    doc = parse_email_file(fpath, user, folder, filename)

                    # exclude mailboxes from setOnInsert to avoid conflict
                    doc_for_insert = {k: v for k, v in doc.items() if k != "mailboxes"}

                    ops.append(
                        UpdateOne(
                            {"dedupe_key": doc["dedupe_key"]},
                            {
                                "$setOnInsert": doc_for_insert,
                                "$addToSet": {"mailboxes": doc["mailboxes"][0]}
                            },
                            upsert=True
                        )
                    )

                    if len(ops) >= batch_size:
                        if not dry_run:
                            res = col.bulk_write(ops, ordered=False)
                            total_upserts += (res.upserted_count or 0)
                            total_mailbox_merges += (res.modified_count or 0)
                        ops = []

                except Exception as e:
                    print(f"[WARN] Failed {fpath}: {e}")

    # Flush remainder
    if ops and not dry_run:
        res = col.bulk_write(ops, ordered=False)
        total_upserts += (res.upserted_count or 0)
        total_mailbox_merges += (res.modified_count or 0)

    print(f"Seen files: {total_seen}")
    print(f"New emails inserted: {total_upserts}")
    print(f"Existing emails that got extra mailbox locations: {total_mailbox_merges}")


if __name__ == "__main__":
    load_dotenv()
    parser = argparse.ArgumentParser(description="Ingest Enron-like email tree into MongoDB.")
    parser.add_argument("--base_dir", required=True, help="Path to dataset root (contains user folders).")
    parser.add_argument("--uri", default=os.getenv("MONGODB_URI"), help="MongoDB connection string.")
    parser.add_argument("--db", default=os.getenv("DB_NAME", "project_demo"))
    parser.add_argument("--coll", default=os.getenv("COLLECTION_NAME", "emails"))
    parser.add_argument("--dry_run", action="store_true", help="Parse but do not write to MongoDB.")
    parser.add_argument("--batch_size", type=int, default=200, help="Bulk write batch size.")
    args = parser.parse_args()

    if not args.uri:
        raise SystemExit("Missing MongoDB URI. Set --uri or MONGODB_URI in .env")

    ingest_tree(args.base_dir, args.uri, args.db, args.coll, dry_run=args.dry_run, batch_size=args.batch_size)
