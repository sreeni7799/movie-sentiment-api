from flask import Flask, request, jsonify
from flask_cors import CORS
import pandas as pd
import requests
import os
import logging
from datetime import datetime
from database import (
    insert_results, 
    fetch_results_from_db, 
    clear_results_collection,
    search_movies_by_sentiment,
    get_unique_movies,
    get_sentiment_summary,
    get_database_stats
)

app = Flask(__name__)
CORS(app)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

ML_SERVICE_URL = os.getenv('ML_SERVICE_URL', 'http://localhost:8000')
UPLOAD_FOLDER = 'uploads'
MAX_FILE_SIZE = 100 * 1024 * 1024   # Allow 100 mb csv files

os.makedirs(UPLOAD_FOLDER, exist_ok=True)

@app.route('/api/test', methods=['GET'])
def health_check():
    try:
        ml_response = requests.get(f"{ML_SERVICE_URL}/health", timeout=5)
        ml_status = "connected" if ml_response.status_code == 200 else "disconnected"
    except:
        ml_status = "disconnected"
    
    db_stats = get_database_stats()
    
    return jsonify({
        "status": "API service running",
        "timestamp": datetime.now().isoformat(),
        "ml_service_status": ml_status,
        "ml_service_url": ML_SERVICE_URL,
        "database_stats": db_stats,
        "environment": "local_development",
        "upload_folder": UPLOAD_FOLDER,
        "max_file_size_mb": MAX_FILE_SIZE / (1024 * 1024)
    })

@app.route('/api/database/stats', methods=['GET'])
def database_stats():
    try:
        stats = get_database_stats()
        return jsonify({
            "database_stats": stats,
            "success": True
        })
    except Exception as e:
        return jsonify({
            "error": f"Failed to get database stats: {str(e)}",
            "success": False
        }), 500
    
  #Process CSV file for sentiment analysis
@app.route('/api/analyze-csv', methods=['POST'])
def analyze_csv():
    try:
        # Check if file was uploaded
        if 'csv_file' not in request.files:
            return jsonify({"error": "No CSV file provided"}), 400
        
        file = request.files['csv_file']
        if file.filename == '' or not file.filename.lower().endswith('.csv'):
            return jsonify({"error": "Please select a valid CSV file"}), 400


        file.seek(0, 2)  # Seek to end of file
        file_size = file.tell()
        file.seek(0)  # Reset to beginning
        
        if file_size > MAX_FILE_SIZE:
            return jsonify({
                "error": f"File too large. Maximum size: {MAX_FILE_SIZE/1024/1024:.1f}MB. Your file: {file_size/1024/1024:.1f}MB"
            }), 400

        try:
            df = pd.read_csv(file)
        except Exception as e:
            return jsonify({"error": f"Failed to read CSV file: {str(e)}"}), 400
            
        logger.info(f"CSV loaded with {len(df)} rows and columns: {list(df.columns)}")

        required_columns = ['title', 'review']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            return jsonify({
                "error": f"Missing required columns: {missing_columns}",
                "found_columns": list(df.columns),
                "required_columns": required_columns,
                "help": "Please ensure your CSV has 'title' and 'review' columns"
            }), 400

        original_count = len(df)
        df = df.dropna(subset=required_columns)
        cleaned_count = len(df)
        
        if cleaned_count == 0:
            return jsonify({
                "error": "No valid data found after removing empty rows",
                "help": "Please check that your CSV has data in both 'title' and 'review' columns"
            }), 400
        
        if cleaned_count < original_count:
            logger.info(f"Removed {original_count - cleaned_count} rows with missing data")
        
        # Prepare batch-ML service
        reviews_batch = []
        for _, row in df.iterrows():
            reviews_batch.append({
                "text": str(row['review']), 
                "movie_name": str(row['title'])
            })

        logger.info(f"Sending {len(reviews_batch)} reviews to ML service at {ML_SERVICE_URL}")

        # Call ML service
        try:
            ml_response = requests.post(
                f"{ML_SERVICE_URL}/process-batch", 
                json={"reviews": reviews_batch}, 
                timeout=300  # 5 minutes timeout
            )
        except requests.exceptions.Timeout:
            return jsonify({
                "error": "ML service timeout. Please try with a smaller file or check if ML service is running."
            }), 504
        except requests.exceptions.ConnectionError:
            return jsonify({
                "error": f"Cannot connect to ML service at {ML_SERVICE_URL}. Please check if it's running."
            }), 503

        if ml_response.status_code == 200:
            batch_results = ml_response.json().get('results', [])
            
            if not batch_results:
                return jsonify({
                    "error": "ML service returned no results",
                    "ml_response": ml_response.json()
                }), 500
            
            # Add metadata to each result
            timestamp = datetime.now().isoformat()
            for result in batch_results:
                result['timestamp'] = timestamp
                result['processed_locally'] = True
            
            # Store in local MongoDB
            try:
                insert_count = insert_results(batch_results)
                
                return jsonify({
                    "message": "CSV processed successfully!",
                    "processed_count": len(batch_results),
                    "total_rows": original_count,
                    "cleaned_rows": cleaned_count,
                    "stored_count": insert_count,
                    "success": True
                })
                
            except Exception as db_error:
                logger.error(f"Database error: {db_error}")
                return jsonify({
                    "error": "Results processed but failed to save to database",
                    "details": str(db_error),
                    "help": "Please check your MongoDB connection"
                }), 500
                
        else:
            logger.error(f"ML service error: {ml_response.status_code} - {ml_response.text}")
            return jsonify({
                "error": f"ML service returned error: {ml_response.status_code}",
                "details": ml_response.text,
                "help": "Please check if your ML service is running and accessible"
            }), 500

    except Exception as e:
        logger.error(f"Error in analyze_csv: {str(e)}")
        return jsonify({
            "error": f"Internal server error: {str(e)}",
            "help": "Please check the server logs for more details"
        }), 500

@app.route('/api/search', methods=['GET'])
def search_movies():
    """Search for movies based on movie name and/or sentiment"""
    try:
        movie_name = request.args.get('movie_name', '').strip()
        sentiment = request.args.get('sentiment', '').strip().lower()
        
        # Validate sentiment parameter
        if sentiment and sentiment not in ['positive', 'negative']:
            return jsonify({
                "error": "Invalid sentiment. Must be 'positive' or 'negative'",
                "received": sentiment
            }), 400
        
        # Perform search
        results = search_movies_by_sentiment(
            movie_name=movie_name if movie_name else None,
            sentiment=sentiment if sentiment else None
        )
        
        return jsonify({
            "results": results,
            "total_count": len(results),
            "search_criteria": {
                "movie_name": movie_name if movie_name else "Any",
                "sentiment": sentiment if sentiment else "Any"
            },
            "success": True
        })
        
    except Exception as e:
        logger.error(f"Error in search_movies: {str(e)}")
        return jsonify({"error": f"Search failed: {str(e)}"}), 500

@app.route('/api/movies', methods=['GET'])
def get_movies_list():
    """Get list of all unique movie names in local database"""
    try:
        movies = get_unique_movies()
        return jsonify({
            "movies": movies,
            "count": len(movies),
            "success": True
        })
    except Exception as e:
        logger.error(f"Error getting movies list: {str(e)}")
        return jsonify({"error": f"Failed to get movies: {str(e)}"}), 500

@app.route('/api/summary', methods=['GET'])
def get_summary():
    """Get sentiment summary for movies"""
    try:
        movie_name = request.args.get('movie_name', '').strip()
        
        summary = get_sentiment_summary(
            movie_name=movie_name if movie_name else None
        )
        
        return jsonify({
            "summary": summary,
            "movie_name": movie_name if movie_name else "All movies",
            "success": True
        })
        
    except Exception as e:
        logger.error(f"Error getting summary: {str(e)}")
        return jsonify({"error": f"Failed to get summary: {str(e)}"}), 500

@app.route('/api/results', methods=['GET'])
def get_results():
    """Get all results from local database"""
    try:
        results = fetch_results_from_db()
        return jsonify({
            "results": results,
            "total_count": len(results),
            "success": True
        })
    except Exception as e:
        logger.error(f"Error retrieving results: {str(e)}")
        return jsonify({"error": f"Failed to retrieve results: {str(e)}"}), 500

@app.route('/api/results/clear', methods=['DELETE'])
def clear_results():
    """Clear all results from local database"""
    try:
        count = clear_results_collection()
        return jsonify({
            "message": f"Cleared {count} results from local database",
            "success": True
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# Error handlers
@app.errorhandler(404)
def not_found(error):
    return jsonify({
        "error": "Endpoint not found", 
        "available_endpoints": [
            "GET /api/test",
            "POST /api/analyze-csv", 
            "GET /api/search",
            "GET /api/movies",
            "GET /api/summary",
            "GET /api/results",
            "DELETE /api/results/clear"
        ]
    }), 404

@app.errorhandler(500)
def internal_error(error):
    return jsonify({"error": "Internal server error"}), 500

if __name__ == '__main__':
    print("🚀 Starting Sentiment Analysis API (Local Development)")
    print("=" * 50)
    print(f"🌐 ML Service URL: {ML_SERVICE_URL}")
    print(f"📁 Upload Folder: {UPLOAD_FOLDER}")
    print(f"📊 Max File Size: {MAX_FILE_SIZE/1024/1024:.1f}MB")
    print("🏠 Database: Local MongoDB")
    print("=" * 50)
    
    # Check database connection on startup
    db_stats = get_database_stats()
    if db_stats["status"] == "connected":
        print(f"✅ Database connected: {db_stats['total_documents']} documents, {db_stats['unique_movies']} movies")
    else:
        print("⚠️ Database connection issue - check MongoDB installation")
    
    print("🎯 Starting server on http://localhost:5000")
    print("📚 API Documentation: http://localhost:5000/api/test")
    
    app.run(host='0.0.0.0', port=5000, debug=True)