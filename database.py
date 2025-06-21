from pymongo import MongoClient
import os

# Use env variable or fallback to local MongoDB
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
client = MongoClient(MONGO_URI)
mongo_db = client["sentiment_db"]
results_collection = mongo_db["results"]

def insert_results(batch):
    if batch:
        results_collection.insert_many(batch)

def fetch_results_from_db():
    cursor = results_collection.find({}, {"_id": 0})  # exclude MongoDB ID
    return list(cursor)

def clear_results_collection():
    result = results_collection.delete_many({})
    return result.deleted_count
