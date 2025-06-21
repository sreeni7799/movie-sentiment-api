# test_database.py - Run this to test your MongoDB connection

from database import insert_results, fetch_results_from_db, clear_results_collection
from pymongo import MongoClient
import os
from datetime import datetime

def test_connection():
    """Test if MongoDB connection is working"""
    try:
        # Test connection
        MONGO_URI = os.getenv("MONGO_URI", "mongodb+srv://sreeni7799:D1cHsheQG01wcnzn@cluster0.yubwquq.mongodb.net/")
        client = MongoClient(MONGO_URI)
        
        # Test if we can connect
        client.admin.command('ping')
        print("✅ MongoDB connection successful!")
        
        # Test database access
        db = client["sentiment_db"]
        collection = db["results"]
        print(f"✅ Database 'sentiment_db' accessible")
        
        return True
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        return False

def test_database_operations():
    """Test insert, fetch, and clear operations"""
    print("\n--- Testing Database Operations ---")
    
    # Test data
    test_data = [
        {
            "text": "This is a test review",
            "movie_name": "Test Movie",
            "sentiment": "positive",
            "confidence": 0.85,
            "timestamp": datetime.now().isoformat()
        },
        {
            "text": "Another test review",
            "movie_name": "Test Movie 2", 
            "sentiment": "negative",
            "confidence": 0.72,
            "timestamp": datetime.now().isoformat()
        }
    ]
    
    try:
        # Test insert
        print("1. Testing insert...")
        insert_results(test_data)
        print("✅ Insert successful!")
        
        # Test fetch
        print("2. Testing fetch...")
        results = fetch_results_from_db()
        print(f"✅ Fetch successful! Found {len(results)} records")
        
        # Print first few results
        for i, result in enumerate(results[-2:]):  # Show last 2 results
            print(f"   Record {i+1}: {result['movie_name']} - {result['sentiment']}")
        
        # Test clear (optional - uncomment if you want to clear test data)
        # print("3. Testing clear...")
        # count = clear_results_collection()
        # print(f"✅ Clear successful! Removed {count} records")
        
        return True
        
    except Exception as e:
        print(f"❌ Database operation failed: {e}")
        return False

if __name__ == "__main__":
    print("=== MongoDB Connection Test ===")
    
    if test_connection():
        test_database_operations()
    else:
        print("\nPlease check your MongoDB connection string and internet connectivity.")
        print("\nCommon issues:")
        print("1. Check if MongoDB Atlas cluster is running")
        print("2. Verify connection string is correct")
        print("3. Check IP whitelist in MongoDB Atlas")
        print("4. Ensure internet connectivity")