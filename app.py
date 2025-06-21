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
    redis_conn = redis.Redis(
    host='localhost', 
    port=6379, 
    decode_responses=True,
    encoding='utf-8',
    encoding_errors='strict'
)
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
        
        # Prepare batch data for ML service
        reviews_batch = []
        for index, row in df.iterrows():
            review_data = {
                "text": str(row['review']),
                "movie_name": str(row['title'])
            }
            reviews_batch.append(review_data)
        
        logger.info(f"Sending {len(reviews_batch)} reviews to ML service for batch processing")
        
        # Send batch to ML service
        try:
            ml_response = requests.post(
                f"{ML_SERVICE_URL}/process-batch",
                json={"reviews": reviews_batch},
                timeout=300  # 5 minutes timeout for batch processing
            )
            
            if ml_response.status_code == 200:
                batch_results = ml_response.json()
                
                # Store results
                for result in batch_results.get('results', []):
                    result['timestamp'] = datetime.now().isoformat()
                    results_storage.append(result)
                
                return jsonify({
                    "message": "CSV processed successfully",
                    "processed_count": len(batch_results.get('results', [])),
                    "total_rows": len(df),
                    "success": True
                })
            else:
                return jsonify({
                    "error": f"ML service returned error: {ml_response.status_code}",
                    "details": ml_response.text
                }), 500
                
        except requests.exceptions.Timeout:
            return jsonify({
                "error": "Processing timeout. Try with a smaller file.",
                "suggestion": "Split your CSV into smaller batches"
            }), 408
            
        except Exception as e:
            logger.error(f"Error calling ML service: {str(e)}")
            return jsonify({"error": f"Failed to process with ML service: {str(e)}"}), 500
        
    except Exception as e:
        logger.error(f"Error in analyze_csv: {str(e)}")
        return jsonify({"error": f"Internal server error: {str(e)}"}), 500
    
@app.route('/api/results', methods=['GET'])
def get_results():
    try:
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