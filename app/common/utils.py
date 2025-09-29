import hashlib, json, chardet
from email import message_from_bytes
from email.utils import getaddresses, parsedate_to_datetime
from dateutil.tz import tzutc

def smart_decode(b: bytes) -> str:
    if not b: return ""
    guess = chardet.detect(b) or {}
    enc = guess.get("encoding") or "latin-1"
    try:
        return b.decode(enc, errors="ignore")
    except Exception:
        try: return b.decode("utf-8", errors="ignore")
        except Exception: return b.decode("latin-1", errors="ignore")

def parse_addresses(value) -> list[str]:
    items = []
    if not value: return items
    flat = ", ".join(value) if isinstance(value, list) else value
    for _, addr in getaddresses([flat]):
        a = addr.strip().lower()
        if a: items.append(a)
    return items

def to_iso_date(date_header: str):
    if not date_header: return None
    try:
        dt = parsedate_to_datetime(date_header)
        if not dt: return None
        if dt.tzinfo is None: dt = dt.replace(tzinfo=tzutc())
        return dt.astimezone(tzutc())
    except Exception:
        return None

def content_hash(fields: dict) -> str:
    payload = json.dumps(fields, sort_keys=True, ensure_ascii=False).encode("utf-8")
    return "hash_" + hashlib.sha256(payload).hexdigest()
