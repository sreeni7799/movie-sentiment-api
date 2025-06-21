from flask import Flask, request, jsonify
from flask_cors import CORS
import pandas as pd
import requests
import os
import logging
from datetime import datetime
from rq import Queue
import redis
from database import insert_results, fetch_results_from_db, clear_results_collection

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

@app.route('/api/test', methods=['GET'])
def health_check():
    try:
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
        if file.filename == '' or not file.filename.lower().endswith('.csv'):
            return jsonify({"error": "Invalid file provided"}), 400

        df = pd.read_csv(file)
        logger.info(f"CSV loaded with {len(df)} rows and columns: {list(df.columns)}")

        required_columns = ['title', 'review']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            return jsonify({
                "error": f"Missing required columns: {missing_columns}",
                "found_columns": list(df.columns),
                "required_columns": required_columns
            }), 400

        df = df.dropna(subset=required_columns)

        reviews_batch = [{"text": str(row['review']), "movie_name": str(row['title'])} for _, row in df.iterrows()]

        ml_response = requests.post(f"{ML_SERVICE_URL}/process-batch", json={"reviews": reviews_batch}, timeout=300)

        if ml_response.status_code == 200:
            batch_results = ml_response.json().get('results', [])
            timestamp = datetime.now().isoformat()
            for result in batch_results:
                result['timestamp'] = timestamp
            insert_results(batch_results)
            return jsonify({
                "message": "CSV processed successfully",
                "processed_count": len(batch_results),
                "total_rows": len(df),
                "success": True
            })
        else:
            return jsonify({
                "error": f"ML service returned error: {ml_response.status_code}",
                "details": ml_response.text
            }), 500

    except Exception as e:
        logger.error(f"Error in analyze_csv: {str(e)}")
        return jsonify({"error": f"Internal server error: {str(e)}"}), 500

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
        logger.error(f"Error retrieving results: {str(e)}")
        return jsonify({"error": f"Failed to retrieve results: {str(e)}"}), 500

@app.route('/api/results/clear', methods=['DELETE'])
def clear_results():
    try:
        count = clear_results_collection()
        return jsonify({
            "message": f"Cleared {count} results",
            "success": True
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.errorhandler(404)
def not_found(error):
    return jsonify({"error": "Endpoint not found"}), 404

@app.errorhandler(500)
def internal_error(error):
    return jsonify({"error": "Internal server error"}), 500

if __name__ == '__main__':
    logger.info("Starting API Service...")
    logger.info(f"ML Service URL: {ML_SERVICE_URL}")
    app.run(host='0.0.0.0', port=5000, debug=True)
