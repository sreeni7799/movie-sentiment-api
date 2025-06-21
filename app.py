from flask import Flask, request, jsonify
from flask_cors import CORS
import pandas as pd
import requests
import os
import logging
from datetime import datetime
from rq import Queue
import redis

try:
    redis_conn = redis.Redis(host='localhost', port=6379, decode_responses=True)
    redis_conn.ping()
    sentiment_queue = Queue('sentiment_analysis', connection=redis_conn)
except Exception as e:
    print(f"Failed to connect to Redis: {e}")
    sentiment_queue = None

app = Flask(__name__)
CORS(app) 

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

ML_SERVICE_URL = os.getenv('ML_SERVICE_URL', 'http://localhost:8000')
UPLOAD_FOLDER = 'uploads'
MAX_FILE_SIZE = 5 * 1024 * 1024  # 5MB limit

# Create upload folder if it doesn't exist
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

# In-memory storage for results (in production, use a database)
results_storage = []

@app.route('/api/test', methods=['GET'])
def health_check():
    try:
        # Test connection to ML service
        ml_response = requests.get(f"{ML_SERVICE_URL}/health", timeout=5)
        ml_status = "connected" if ml_response.status_code == 200 else "disconnected"
    except:
        ml_status = "disconnected"
    
    return jsonify({
        "status": "API service is running",
        "timestamp": datetime.now().isoformat(),
        "ml_service_status": ml_status
    })

@app.route('/api/analyze-csv', methods=['POST'])
def analyze_csv():
    try:
        if 'csv_file' not in request.files:
            return jsonify({"error": "No CSV file provided"}), 400
        
        file = request.files['csv_file']
        
        if file.filename == '':
            return jsonify({"error": "No file selected"}), 400
        
        if not file.filename.lower().endswith('.csv'):
            return jsonify({"error": "File must be a CSV"}), 400
        
        #file.seek(0, os.SEEK_END)
        file_size = file.tell()
        #file.seek(0)
        
        if file_size > MAX_FILE_SIZE:
            return jsonify({"error": "File size exceeds 5MB limit"}), 400
        
        try:
            df = pd.read_csv(file)
            logger.info(f"CSV loaded with {len(df)} rows and columns: {list(df.columns)}")
        except Exception as e:
            return jsonify({"error": f"Invalid CSV format: {str(e)}"}), 400
        required_columns = ['title', 'review']
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            return jsonify({
                "error": f"Missing required columns: {missing_columns}",
                "found_columns": list(df.columns),
                "required_columns": required_columns
            }), 400
        
        # Clean and prepare data
        df = df.dropna(subset=required_columns)
        processed_count = 0
        
        # Process each review (batch processing for efficiency)
        for index, row in df.iterrows():
            try:
                # Prepare data for ML service
                review_data = {
                    "text": str(row['review']),
                    "movie_name": str(row['title'])
                }
                
               # Queue the job for ML processing (asynchronous)
                if sentiment_queue:
                    try:
                        job = sentiment_queue.enqueue(
                            'ml_worker.process_sentiment',
                            review_data, 
                            job_timeout='5m',  # 5 minute timeout
                            job_id=f"review_{index}_{datetime.now().timestamp()}"
                        )
                        job_info = {
                        "job_id": job.id,
                        "movie_name": review_data["movie_name"],
                        "original_text": review_data["text"],
                        "status": "queued",
                        "timestamp": datetime.now().isoformat()
                        }
        
                        results_storage.append(job_info)
                        processed_count += 1
                        
                        logger.info(f"Queued job {job.id} for review: {review_data['movie_name']}")
                        
                    except Exception as e:
                        logger.error(f"Failed to queue job for row {index}: {e}")
                else:
                    logger.error("Queue not available, skipping row")                    
            except Exception as e:
                logger.error(f"Error processing row {index}: {str(e)}")
                continue
        
        return jsonify({
    "message": "CSV processed and jobs queued successfully",
    "queued_count": processed_count,
    "total_rows": len(df),
    "note": "Processing is now asynchronous. Check /api/results for updates.",
    "success": True
})
        
    except Exception as e:
        logger.error(f"Error in analyze_csv: {str(e)}")
        return jsonify({"error": f"Internal server error: {str(e)}"}), 500

@app.route('/api/results', methods=['GET'])
def get_results():
    try:
        # Update job statuses before returning results
        if sentiment_queue:
            for result in results_storage:
                if 'job_id' in result:
                    job = sentiment_queue.fetch_job(result['job_id'])
                    if job:
                        result['status'] = job.get_status()
                        if job.is_finished and job.result:
                            # Update with ML results
                            result.update(job.result)
        
        sorted_results = sorted(
            results_storage, 
            key=lambda x: x.get('timestamp', ''), 
            reverse=True
        )
        
        return jsonify({
            "results": sorted_results,
            "total_count": len(sorted_results),
            "success": True
        })
        
    except Exception as e:
        logger.error(f"Error retrieving results: {str(e)}")
        return jsonify({"error": f"Failed to retrieve results: {str(e)}"}), 500

@app.route('/api/results/clear', methods=['DELETE'])
def clear_results():
    global results_storage
    count = len(results_storage)
    results_storage = []
    
    return jsonify({
        "message": f"Cleared {count} results",
        "success": True
    })

@app.errorhandler(404)
def not_found(error):
    return jsonify({"error": "Endpoint not found"}), 404

@app.errorhandler(500)
def internal_error(error):
    return jsonify({"error": "Internal server error"}), 500

@app.route('/api/job-status/<job_id>', methods=['GET'])
def get_job_status(job_id):
    """Check the status of a specific queued job"""
    try:
        if not sentiment_queue:
            return jsonify({"error": "Queue not available"}), 503
            
        job = sentiment_queue.fetch_job(job_id)
        
        if not job:
            return jsonify({"error": "Job not found"}), 404
            
        return jsonify({
            "job_id": job_id,
            "status": job.get_status(),
            "result": job.result if job.is_finished else None,
            "started_at": job.started_at.isoformat() if job.started_at else None,
            "ended_at": job.ended_at.isoformat() if job.ended_at else None,
            "success": True
        })
        
    except Exception as e:
        logger.error(f"Error checking job status: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/queue-info', methods=['GET'])
def get_queue_info():
    """Get information about the queue"""
    try:
        if not sentiment_queue:
            return jsonify({"error": "Queue not available"}), 503
            
        return jsonify({
            "queue_length": len(sentiment_queue),
            "failed_jobs": len(sentiment_queue.failed_job_registry),
            "started_jobs": len(sentiment_queue.started_job_registry),
            "deferred_jobs": len(sentiment_queue.deferred_job_registry),
            "success": True
        })
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    logger.info("Starting API Service...")
    logger.info(f"ML Service URL: {ML_SERVICE_URL}")
    app.run(host='0.0.0.0', port=5000, debug=True)