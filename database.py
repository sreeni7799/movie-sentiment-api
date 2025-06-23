from pymongo import MongoClient
import re

try:
    client = MongoClient("mongodb://localhost:27017/")

    client.admin.command('ping')
    print("connected to MongoDB")
    
    mongo_db = client["sentiment_db"]
    results_collection = mongo_db["results"]
    
    existing_count = results_collection.count_documents({})
    if existing_count > 0:
        print(f"Found {existing_count} existing records in database")
    else:
        print(" Local database is empty")
    
except Exception as e:
    print(f" Failed to connect to MongoDB!")
    print(f"Error: {e}")
    
    client = None
    mongo_db = None
    results_collection = None

def insert_results(batch):
    if results_collection is None: 
        raise Exception(" MongoDB not connected")
    for collection_name in mongo_db.list_collection_names():
        mongo_db[collection_name].delete_many({})

    if not batch:
        print("No data to insert")
        return
    
    try:
        result = results_collection.insert_many(batch)
        count = len(result.inserted_ids)
        print(f"Inserted {count} results into MongoDB")
    
        total = results_collection.count_documents({})
        print(f" DB now has {total} total records")
        return count
        
    except Exception as e:
        print(f"Failed to insert results: {e}")
        raise e

def fetch_results_from_db():
    if results_collection is None: 
        print(" Local MongoDB not connected")
        return []
    
    try:
        cursor = results_collection.find({}, {"_id": 0})
        results = list(cursor)
        print(f" Retrieved {len(results)} results from local MongoDB")
        return results
        
    except Exception as e:
        print(f" Failed to fetch results: {e}")
        return []

def clear_results_collection():
    if results_collection is None: 
        print("Local MongoDB not connected")
        return 0
    
    try:
        result = results_collection.delete_many({})
        count = result.deleted_count
        print(f"Cleared {count} results from local MongoDB")
        return count
        
    except Exception as e:
        print(f" Failed to clear results: {e}")
        return 0

def search_movies_by_sentiment(movie_name=None, sentiment=None):
    if results_collection is None: 
        print("Local MongoDB not connected")
        return []
    
    try:
        query = {}
        search_terms = []
        
        if movie_name and movie_name.strip():
            query["movie_name"] = {
                "$regex": re.escape(movie_name.strip()), 
                "$options": "i"  # case-insensitive
            }
            search_terms.append(f"movie name containing '{movie_name.strip()}'")
        
        if sentiment and sentiment.strip():
            query["sentiment"] = sentiment.strip().lower()
            search_terms.append(f"sentiment: {sentiment.strip().lower()}")
        
        cursor = results_collection.find(query, {"_id": 0})
        results = list(cursor)
        
        if search_terms:
            search_description = " AND ".join(search_terms)
            print(f"Searched for {search_description}")
            print(f"Found {len(results)} matching results")
        else:
            print(f"Retrieved all {len(results)} results (no search filters)")
        
        return results
        
    except Exception as e:
        print(f"Search failed: {e}")
        return []

def get_unique_movies():
    if results_collection is None: 
        print("Local MongoDB not connected")
        return []
    
    try:
        unique_movies = results_collection.distinct("movie_name")
        movies = sorted([movie for movie in unique_movies if movie and movie.strip()])
        
        print(f"Found {len(movies)} unique movies in local database")
        if movies:
            print(f"Sample movies: {', '.join(movies[:3])}{'...' if len(movies) > 3 else ''}")
        
        return movies
        
    except Exception as e:
        print(f"Failed to get unique movies: {e}")
        return []

def get_sentiment_summary(movie_name=None):
    if results_collection is None: 
        return []
    
    try:
        pipeline = []
        
        if movie_name and movie_name.strip():
            pipeline.append({
                "$match": {
                    "movie_name": {
                        "$regex": re.escape(movie_name.strip()), 
                        "$options": "i"
                    }
                }
            })
        else:
            print("Generating summary for all movies")
        
        pipeline.extend([
            {
                "$group": {
                    "_id": {
                        "movie_name": "$movie_name",
                        "sentiment": "$sentiment"
                    },
                    "count": {"$sum": 1},
                    "avg_confidence": {"$avg": "$confidence"}
                }
            },
            {
                "$group": {
                    "_id": "$_id.movie_name",
                    "sentiments": {
                        "$push": {
                            "sentiment": "$_id.sentiment",
                            "count": "$count",
                            "avg_confidence": "$avg_confidence"
                        }
                    },
                    "total_reviews": {"$sum": "$count"}
                }
            },
            {"$sort": {"_id": 1}}  # Sort by movie name
        ])
        
        cursor = results_collection.aggregate(pipeline)
        summary = list(cursor)
        
        print(f"Generated sentiment summary for {len(summary)} movies")
        return summary
        
    except Exception as e:
        print(f"Failed to generate sentiment summary: {e}")
        return []

def get_database_stats():
    if results_collection is None:
        return {"status": "disconnected", "error": "MongoDB not connected"}
    
    try:
        total_docs = results_collection.count_documents({})
        unique_movies = len(results_collection.distinct("movie_name"))
        
        positive_count = results_collection.count_documents({"sentiment": "positive"})
        negative_count = results_collection.count_documents({"sentiment": "negative"})
        
        stats = {
            "status": "connected",
            "connection_type": "local",
            "total_documents": total_docs,
            "unique_movies": unique_movies,
            "positive_reviews": positive_count,
            "negative_reviews": negative_count,
            "database_name": "sentiment_db",
            "collection_name": "results"
        }
        
        return stats
        
    except Exception as e:
        return {"status": "error", "error": str(e)}

if results_collection is not None:
    print("Local MongoDB database ready!")
    stats = get_database_stats()
    if stats["status"] == "connected":
        print(f"Database stats: {stats['total_documents']} docs, {stats['unique_movies']} movies")
else:
   print("failed")