from pymongo import MongoClient
import re

print("🏠 Setting up local MongoDB connection...")

try:
    # Connect to local MongoDB
    client = MongoClient("mongodb://localhost:27017/")
    
    # Test the connection
    client.admin.command('ping')
    print("✅ Successfully connected to local MongoDB!")
    
    # Set up database and collection
    mongo_db = client["sentiment_db"]
    results_collection = mongo_db["results"]
    
    # Check existing data
    existing_count = results_collection.count_documents({})
    if existing_count > 0:
        print(f"📊 Found {existing_count} existing records in local database")
    else:
        print("📝 Local database is empty and ready for new data")
    
except Exception as e:
    print(f"❌ Failed to connect to local MongoDB!")
    print(f"Error: {e}")
    print("\n🔧 Troubleshooting:")
    print("1. Make sure MongoDB is installed and running")
    print("2. Windows: Check if 'MongoDB Server' service is running")
    print("3. Mac: Run 'brew services start mongodb/brew/mongodb-community'")
    print("4. Linux: Run 'sudo systemctl start mongod'")
    print("5. Try restarting your computer")
    
    # Set to None so app knows there's no connection
    client = None
    mongo_db = None
    results_collection = None

def insert_results(batch):
    """Insert sentiment analysis results into local database"""
    if results_collection is None:  # FIXED: Use 'is None' instead of 'not'
        raise Exception("❌ Local MongoDB not connected! Please check your MongoDB installation.")
    
    if not batch:
        print("⚠️ No data to insert")
        return
    
    try:
        result = results_collection.insert_many(batch)
        count = len(result.inserted_ids)
        print(f"✅ Inserted {count} results into local MongoDB")
        
        # Show total count
        total = results_collection.count_documents({})
        print(f"📊 Local database now has {total} total records")
        return count
        
    except Exception as e:
        print(f"❌ Failed to insert results: {e}")
        raise e

def fetch_results_from_db():
    """Get all results from local database"""
    if results_collection is None:  # FIXED: Use 'is None' instead of 'not'
        print("❌ Local MongoDB not connected")
        return []
    
    try:
        cursor = results_collection.find({}, {"_id": 0})
        results = list(cursor)
        print(f"📊 Retrieved {len(results)} results from local MongoDB")
        return results
        
    except Exception as e:
        print(f"❌ Failed to fetch results: {e}")
        return []

def clear_results_collection():
    """Delete all results from local database"""
    if results_collection is None:  # FIXED: Use 'is None' instead of 'not'
        print("❌ Local MongoDB not connected")
        return 0
    
    try:
        result = results_collection.delete_many({})
        count = result.deleted_count
        print(f"🗑️ Cleared {count} results from local MongoDB")
        return count
        
    except Exception as e:
        print(f"❌ Failed to clear results: {e}")
        return 0

def search_movies_by_sentiment(movie_name=None, sentiment=None):
    """
    Search for movies based on movie name and/or sentiment
    
    Args:
        movie_name (str): Movie name to search (case-insensitive, partial match)
        sentiment (str): 'positive' or 'negative'
    
    Returns:
        list: Matching results
    """
    if results_collection is None:  # FIXED: Use 'is None' instead of 'not'
        print("❌ Local MongoDB not connected")
        return []
    
    try:
        # Build search query
        query = {}
        search_terms = []
        
        # Add movie name search (case-insensitive, partial match)
        if movie_name and movie_name.strip():
            query["movie_name"] = {
                "$regex": re.escape(movie_name.strip()), 
                "$options": "i"  # case-insensitive
            }
            search_terms.append(f"movie name containing '{movie_name.strip()}'")
        
        # Add sentiment filter
        if sentiment and sentiment.strip():
            query["sentiment"] = sentiment.strip().lower()
            search_terms.append(f"sentiment: {sentiment.strip().lower()}")
        
        # Execute search
        cursor = results_collection.find(query, {"_id": 0})
        results = list(cursor)
        
        # Log search details
        if search_terms:
            search_description = " AND ".join(search_terms)
            print(f"🔍 Searched for {search_description}")
            print(f"📊 Found {len(results)} matching results")
        else:
            print(f"📊 Retrieved all {len(results)} results (no search filters)")
        
        return results
        
    except Exception as e:
        print(f"❌ Search failed: {e}")
        return []

def get_unique_movies():
    """Get list of all unique movie names in local database"""
    if results_collection is None:  # FIXED: Use 'is None' instead of 'not'
        print("❌ Local MongoDB not connected")
        return []
    
    try:
        unique_movies = results_collection.distinct("movie_name")
        # Filter out empty/None values and sort
        movies = sorted([movie for movie in unique_movies if movie and movie.strip()])
        
        print(f"🎬 Found {len(movies)} unique movies in local database")
        if movies:
            print(f"📝 Sample movies: {', '.join(movies[:3])}{'...' if len(movies) > 3 else ''}")
        
        return movies
        
    except Exception as e:
        print(f"❌ Failed to get unique movies: {e}")
        return []

def get_sentiment_summary(movie_name=None):
    """
    Get sentiment summary statistics
    
    Args:
        movie_name (str): Specific movie name (optional)
    
    Returns:
        list: Summary data with sentiment counts and averages
    """
    if results_collection is None:  # FIXED: Use 'is None' instead of 'not'
        print("❌ Local MongoDB not connected")
        return []
    
    try:
        pipeline = []
        
        # Filter by movie name if specified
        if movie_name and movie_name.strip():
            pipeline.append({
                "$match": {
                    "movie_name": {
                        "$regex": re.escape(movie_name.strip()), 
                        "$options": "i"
                    }
                }
            })
            print(f"📈 Generating summary for movies matching '{movie_name.strip()}'")
        else:
            print("📈 Generating summary for all movies")
        
        # Aggregation pipeline
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
        
        print(f"📊 Generated sentiment summary for {len(summary)} movies")
        return summary
        
    except Exception as e:
        print(f"❌ Failed to generate sentiment summary: {e}")
        return []

def get_database_stats():
    """Get statistics about the local database"""
    if results_collection is None:  # FIXED: Use 'is None' instead of 'not'
        return {"status": "disconnected", "error": "MongoDB not connected"}
    
    try:
        total_docs = results_collection.count_documents({})
        unique_movies = len(results_collection.distinct("movie_name"))
        
        # Count by sentiment
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

# Test connection when module loads
if results_collection is not None:  # FIXED: Use 'is not None' instead of truth testing
    print("🎉 Local MongoDB database module ready!")
    stats = get_database_stats()
    if stats["status"] == "connected":
        print(f"📊 Database stats: {stats['total_documents']} docs, {stats['unique_movies']} movies")
else:
    print("⚠️ Database module loaded but MongoDB connection failed")
    print("Please check MongoDB installation and try again")