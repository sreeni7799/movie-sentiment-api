from flask import Flask, request, jsonify
from flask_cors import CORS
import pandas as pd
import requests
import os
import logging
from datetime import datetime
from rq import Queue
import redis
import sys
sys.path.append('../shared') 
from shared.database import (
    insert_results, 
    fetch_results_from_db, 
    clear_results_collection,
    search_movies_by_sentiment,
    get_unique_movies,
    get_sentiment_summary,
    get_database_stats 
)

try:
    redis_conn = redis.Redis(
        host=os.getenv('REDIS_HOST', 'localhost'), 
        port=int(os.getenv('REDIS_PORT', 6379)), 
        db=0
    )
    redis_conn.ping()
    sentiment_queue = Queue('sentiment_analysis', connection=redis_conn)
    print("Redis connected - Queue ready for worker service")
except Exception as e:
    print("Background processing disabled")
    sentiment_queue = None

app = Flask(__name__)
CORS(app)

ML_SERVICE_URL = os.getenv('ML_SERVICE_URL', 'http://localhost:8000')
UPLOAD_FOLDER = 'uploads'
MAX_FILE_SIZE = 100 * 1024 * 1024  # 100MB 

os.makedirs(UPLOAD_FOLDER, exist_ok=True)

@app.route('/api/test', methods=['GET'])
def health_check():
    try:
        ml_response = requests.get(f"{ML_SERVICE_URL}/health", timeout=5)
        ml_status = "connected" if ml_response.status_code == 200 else "disconnected"
    except:
        ml_status = "disconnected"
    
    db_stats = get_database_stats()
    
    redis_status = "connected" if sentiment_queue is not None else "disconnected"
    
    return jsonify({
        "status": "API service is running ",
        "ml_service_status": ml_status,
        "database_stats": db_stats,
        "redis_status": redis_status,
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

@app.route('/api/redis/status', methods=['GET'])
def redis_status():
    try:
        if sentiment_queue is None:
            return jsonify({
                "redis_status": "disconnected"
            })
        
        queue_info = {
            "redis_status": "connected"
        }
        
        return jsonify({
            "redis_info": queue_info,
            "success": True
        })
        
    except Exception as e:
        return jsonify({"error": "Redis not available"}), 500

@app.route('/api/analyze-csv', methods=['POST'])
def analyze_csv():
    try:
        if 'csv_file' not in request.files:
            return jsonify({"error": "No CSV file provided"}), 400
        
        file = request.files['csv_file']
        if file.filename == '' or not file.filename.lower().endswith('.csv'):
            return jsonify({"error": "Please select a valid CSV file"}), 400

        file.seek(0, 2) 
        file_size = file.tell()
        file.seek(0)
        
        if file_size > MAX_FILE_SIZE:
            return jsonify({
                "error": f"File too large. Maximum size: {MAX_FILE_SIZE/1024/1024:.1f}MB. Your file: {file_size/1024/1024:.1f}MB"
            }), 400

        try:
            df = pd.read_csv(file)
        except Exception as e:
            return jsonify({"error": "Invalid CSV format"}), 400
            

        required_columns = ['title', 'review']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            return jsonify({
                "error": "Missing required columns"}), 400


        original_count = len(df)
        df = df.dropna(subset=required_columns)
        cleaned_count = len(df)
        
        
        if cleaned_count == 0:
            return jsonify({
                "error": "No valid data found after removing empty rows",}), 400
        
        if cleaned_count < original_count:
            print(f"Removed {original_count - cleaned_count} rows with missing data")

        reviews_batch = []
        for _, row in df.iterrows():
            reviews_batch.append({
                "text": str(row['review']), 
                "movie_name": str(row['title'])
            })

        use_background_processing = sentiment_queue is not None and len(reviews_batch) > 1000  
        if use_background_processing:
            try:
                job = sentiment_queue.enqueue(
                    'worker_tasks.process_sentiment_batch',
                    reviews_batch,
                    job_timeout='30m'
                )
                
                return jsonify({
                    "message": "Large CSV queued for background processing",
                    "job_id": job.id,
                    "total_rows": original_count,
                    "cleaned_rows": cleaned_count,
                    "queued_for_processing": len(reviews_batch),
                    "processing_mode": "background",
                    "success": True
                })
                
            except Exception as queue_error:
                print(f"Background processing failed, falling back to synchronous: {queue_error}")
        
    
        try:
            ml_response = requests.post(
                f"{ML_SERVICE_URL}/process-batch", 
                json={"reviews": reviews_batch}, 
                timeout=300  # 5 minutes timeout
            )
        except requests.exceptions.Timeout:
            return jsonify({
                "error": "ML service timeout"}), 504
        except requests.exceptions.ConnectionError:
            return jsonify({
                "error": f"Cannot connect to ML service at {ML_SERVICE_URL}"
            }), 503

        if ml_response.status_code == 200:
            batch_results = ml_response.json().get('results', [])
            
            if not batch_results:
                return jsonify({
                    "error": "ML service returned no results"
                    }), 500
            
            # Add metadata to results
            timestamp = datetime.now().isoformat()
            for result in batch_results:
                result['timestamp'] = timestamp
                result['processed_by'] = 'api_service_sync'
                result['processing_mode'] = 'synchronous'

            # Store results in database
            try:
                insert_count = insert_results(batch_results)
                
                return jsonify({
                    "message": "CSV processed successfully!",
                    "processed_count": len(batch_results),
                    "total_rows": original_count,
                    "cleaned_rows": cleaned_count,
                    "stored_count": insert_count,
                    "processing_mode": "synchronous",
                    "success": True
                })
                
            except Exception as db_error:
                print(f"Database error: {str(db_error)}")
                return jsonify({
                    "error": "Results processed but failed to save to database"
                }), 500
                
        else:
            print(f"ML service error: {ml_response.status_code} - {ml_response.text}")
            return jsonify({
                "error": f"ML service returned error: {ml_response.status_code}"
            }), 500

    except Exception as e:
        print(f"Error in analyze_csv: {str(e)}")
        return jsonify({
            "error": "Internal server error}"
        }), 500

@app.route('/api/search', methods=['GET'])
def search_movies():
    try:
        movie_name = request.args.get('movie_name', '').strip()
        sentiment = request.args.get('sentiment', '').strip().lower()
    
        if sentiment and sentiment not in ['positive', 'negative']:
            return jsonify({
                "error": "Invalid sentiment"}), 400
        
        use_background_search = sentiment_queue is not None and request.args.get('background') == 'true'
        
        if use_background_search:
            try:
                job = sentiment_queue.enqueue(
                    'background_search', 
                    movie_name,
                    sentiment,
                    job_timeout='5m'
                )
                
                return jsonify({
                    "message": "Search queued for background processing",
                    "job_id": job.id,
                    "search_criteria": {
                        "movie_name": movie_name if movie_name else "Any",
                        "sentiment": sentiment if sentiment else "Any"
                    },
                    "processing_mode": "background",
                    "success": True
                })
                
            except Exception as queue_error:
                print(f"Background search failed, falling back to immediate search: {queue_error}")
        
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
            "processing_mode": "immediate",
            "success": True
        })
        
    except Exception as e:
        print(f"Error in search_movies: {str(e)}")
        return jsonify({"error": f"Search failed: {str(e)}"}), 500

@app.route('/api/movies', methods=['GET'])
def get_movies_list():
    try:
        movies = get_unique_movies()
        return jsonify({
            "movies": movies,
            "count": len(movies),
            "success": True
        })
    except Exception as e:
        return jsonify({"error": f"Failed to get movies: {str(e)}"}), 500

@app.route('/api/summary', methods=['GET'])
def get_summary():
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
        print(f"Error getting summary: {str(e)}")
        return jsonify({"error": f"Failed to get summary: {str(e)}"}), 500

@app.route('/api/results', methods=['GET'])
def get_results():
    try:
        results = fetch_results_from_db()
        return jsonify({
            "results": results,
            "total_count": len(results),
            "success": True
        })
    except Exception as e:
        print(f"Error retrieving results: {str(e)}")
        return jsonify({"error": f"Failed to retrieve results: {str(e)}"}), 500

@app.route('/api/results/clear', methods=['DELETE'])
def clear_results():
    try:
        count = clear_results_collection()
        return jsonify({
            "message": f"Cleared {count} results from local database",
            "success": True
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/job/<job_id>', methods=['GET'])
def get_job_status(job_id):
    if sentiment_queue is None:
        return jsonify({"error": "Background processing not available"}), 503
    
    try:
        from rq.job import Job
        job = Job.fetch(job_id, connection=redis_conn)
        
        return jsonify({
            "job_id": job_id,
            "status": job.status,
            "created_at": job.created_at.isoformat() if job.created_at else None,
            "started_at": job.started_at.isoformat() if job.started_at else None,
            "ended_at": job.ended_at.isoformat() if job.ended_at else None,
            "result": job.result,
            "meta": job.meta,
            "success": True
        })
        
    except Exception as e:
        return jsonify({"error": f"Failed to get job status: {str(e)}"}), 500
    
@app.route('/api/worker/status', methods=['GET'])
def worker_service_status():
    if sentiment_queue is None:
        return jsonify({
            "worker_service": "unavailable",
            "redis_status": "disconnected",
            "message": "Redis not available"
        })
    
    try:
        queue_length = len(sentiment_queue)
        failed_jobs = len(sentiment_queue.failed_job_registry)
        
        workers = sentiment_queue.workers
        active_workers = len(workers)
        
        return jsonify({
            "worker_service": "available" if active_workers > 0 else "no_workers",
            "redis_status": "connected",
            "queue_stats": {
                "pending_jobs": queue_length,
                "failed_jobs": failed_jobs,
                "active_workers": active_workers
            },
            "message": f"{active_workers} worker(s) active, {queue_length} jobs pending",
            "success": True
        })
        
    except Exception as e:
        return jsonify({
            "error": f"Failed to get worker status: {str(e)}",
            "success": False
        }), 500

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
            "DELETE /api/results/clear",
            "GET /api/database/stats",
            "GET /api/redis/status",
            "GET /api/job/<job_id>"
        ]
    }), 404

@app.errorhandler(500)
def internal_error(error):
    return jsonify({"error": "Internal server error"}), 500

if __name__ == '__main__':
    print("Starting API server")
    db_stats = get_database_stats()
    if db_stats["status"] == "connected":
        print(f"Database connected: {db_stats['total_documents']} documents, {db_stats['unique_movies']} movies")
    else:
        print("Database connection issue")

    if sentiment_queue is not None:
        print("Redis connected")
    else:
        print("Redis not available")
    
    app.run(host='0.0.0.0', port=5000, debug=True)