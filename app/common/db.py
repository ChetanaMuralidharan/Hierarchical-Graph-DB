import os
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()
_client = None

def mongo_client():
    global _client
    if _client is None:
        _client = MongoClient(os.getenv("MONGODB_URI"))
    return _client

def db():
    name = os.getenv("DB_NAME", "Project_Demo")   # <-- use DB_NAME
    print(f"[DB] Using database: {name}")         # debug log
    return mongo_client()[name]

def emails_collection():
    dbname = os.getenv("DB_NAME", "Project_Demo")
    collname = os.getenv("COLLECTION_NAME", "emails")
    print(f"[DB] Using collection: {dbname}.{collname}")
    return mongo_client()[dbname][collname]
