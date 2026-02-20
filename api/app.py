"""
Marketing Data Engine - Flask API
Main application with all API endpoints
"""

from flask import Flask, request, jsonify, send_file, send_from_directory
from flask_cors import CORS
import pandas as pd
import numpy as np
import json
import io
import os
from datetime import datetime
from werkzeug.utils import secure_filename

# Import services
import sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from services.data_ingestion import DataIngestionService
from services.data_normalization import DataNormalizationService
from services.data_quality import DataQualityService
from services.data_merger import DataMergerService
from services.ai_insights import AIInsightsService
from services.report_generator import ReportGeneratorService

# Initialize Flask app
app = Flask(__name__)
CORS(app)

# Configuration - Use relative paths for portability
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
UPLOAD_FOLDER = os.path.join(BASE_DIR, 'data', 'uploads')
REPORT_FOLDER = os.path.join(BASE_DIR, 'data', 'reports')

os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(REPORT_FOLDER, exist_ok=True)

app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['MAX_CONTENT_LENGTH'] = 50 * 1024 * 1024  # 50MB max

# Allowed file extensions
ALLOWED_EXTENSIONS = {'csv', 'xlsx', 'xls', 'json'}

# Initialize services
ingestion_service = DataIngestionService(UPLOAD_FOLDER)
normalization_service = DataNormalizationService()
quality_service = DataQualityService()
merger_service = DataMergerService()
insights_service = AIInsightsService()
report_service = ReportGeneratorService(REPORT_FOLDER)

# Store processed data in memory (in production, use a database)
data_store = {}


def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


# ============== HEALTH CHECK ==============

@app.route('/api/health', methods=['GET'])
def health_check():
    """Health check endpoint."""
    return jsonify({
        'status': 'healthy',
        'timestamp': datetime.now().isoformat(),
        'version': '1.0.0',
        'service': 'Marketing Data Engine API'
    })


# ============== DATA INGESTION ENDPOINTS ==============

@app.route('/api/upload', methods=['POST'])
def upload_file():
    """
    Upload a data file (CSV, Excel, JSON).
    Returns data preview and metadata.
    """
    if 'file' not in request.files:
        return jsonify({'success': False, 'error': 'No file provided'}), 400

    file = request.files['file']
    if file.filename == '':
        return jsonify({'success': False, 'error': 'No file selected'}), 400

    if not allowed_file(file.filename):
        return jsonify({'success': False, 'error': 'File type not allowed. Use CSV, Excel, or JSON.'}), 400

    try:
        # Read file content
        file_content = file.read()
        filename = secure_filename(file.filename)

        # Use the unified buffer ingestion method
        result = ingestion_service.ingest_from_buffer(io.BytesIO(file_content), filename)

        if result['success']:
            # Store data with a unique ID
            data_id = f"data_{datetime.now().strftime('%Y%m%d%H%M%S')}"
            data_store[data_id] = result['data']

            # Prepare response (without full dataframe)
            response = {
                'success': True,
                'data_id': data_id,
                'metadata': result['metadata'],
                'preview': result['data'].head(20).to_dict('records'),
                'columns': list(result['data'].columns)
            }
            return jsonify(response)
        else:
            return jsonify(result), 400

    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/upload-multiple', methods=['POST'])
def upload_multiple_files():
    """
    Upload multiple data files and merge them.
    """
    if 'files' not in request.files:
        return jsonify({'success': False, 'error': 'No files provided'}), 400

    files = request.files.getlist('files')
    if not files or all(f.filename == '' for f in files):
        return jsonify({'success': False, 'error': 'No files selected'}), 400

    datasets = []
    platforms = []

    for file in files:
        if file.filename == '' or not allowed_file(file.filename):
            continue

        try:
            file_content = file.read()
            filename = secure_filename(file.filename)

            # Use the unified buffer ingestion method
            result = ingestion_service.ingest_from_buffer(io.BytesIO(file_content), filename)

            if result['success']:
                datasets.append(result['data'])
                platforms.append(result['metadata']['platform'])

        except Exception as e:
            continue

    if not datasets:
        return jsonify({'success': False, 'error': 'No valid files processed'}), 400

    # Merge datasets
    merge_result = merger_service.merge_datasets(datasets, platforms)

    if merge_result['success']:
        data_id = f"merged_{datetime.now().strftime('%Y%m%d%H%M%S')}"
        data_store[data_id] = merge_result['data']

        return jsonify({
            'success': True,
            'data_id': data_id,
            'files_processed': len(datasets),
            'platforms': platforms,
            'metadata': merge_result['metadata'],
            'preview': merge_result['data'].head(20).to_dict('records')
        })

    return jsonify(merge_result), 400


# ============== DATA NORMALIZATION ENDPOINTS ==============

@app.route('/api/normalize/<data_id>', methods=['POST'])
def normalize_data(data_id):
    """
    Normalize data with standard column mapping and format conversion.
    """
    if data_id not in data_store:
        return jsonify({'success': False, 'error': 'Data not found. Upload data first.'}), 404

    try:
        df = data_store[data_id]

        # Get options from request
        options = request.get_json() or {}
        currency = options.get('currency', 'USD')
        custom_mappings = options.get('custom_mappings')
        platform = options.get('platform', 'unknown')

        # Normalize data
        result = normalization_service.normalize(df, platform, currency, custom_mappings)

        if result['success']:
            # Update stored data
            data_store[data_id] = result['data']

            return jsonify({
                'success': True,
                'data_id': data_id,
                'normalization_report': result['normalization_report'],
                'preview': result['data'].head(20).to_dict('records')
            })

        return jsonify(result), 400

    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# ============== DATA QUALITY ENDPOINTS ==============

@app.route('/api/quality/check/<data_id>', methods=['GET'])
def check_data_quality(data_id):
    """
    Perform data quality checks.
    """
    if data_id not in data_store:
        return jsonify({'success': False, 'error': 'Data not found'}), 404

    df = data_store[data_id]
    report = quality_service.check_quality(df)

    return jsonify({
        'success': True,
        'quality_report': report
    })


@app.route('/api/quality/anomalies/<data_id>', methods=['GET'])
def detect_anomalies(data_id):
    """
    Detect anomalies in the data.
    """
    if data_id not in data_store:
        return jsonify({'success': False, 'error': 'Data not found'}), 404

    df = data_store[data_id]
    anomalies = quality_service.detect_anomalies(df)
    performance_issues = quality_service.detect_performance_anomalies(df)

    return jsonify({
        'success': True,
        'anomalies': anomalies,
        'performance_issues': performance_issues
    })


# ============== DATA MERGING ENDPOINTS ==============

@app.route('/api/aggregate/date/<data_id>', methods=['GET'])
def aggregate_by_date(data_id):
    """
    Aggregate data by date.
    """
    if data_id not in data_store:
        return jsonify({'success': False, 'error': 'Data not found'}), 404

    granularity = request.args.get('granularity', 'daily')
    df = data_store[data_id]

    result = merger_service.aggregate_by_date(df, granularity)

    if result['success']:
        return jsonify({
            'success': True,
            'data': result['data'].to_dict('records'),
            'granularity': result['granularity'],
            'periods': result['periods']
        })

    return jsonify(result), 400


@app.route('/api/aggregate/campaign/<data_id>', methods=['GET'])
def aggregate_by_campaign(data_id):
    """
    Aggregate data by campaign.
    """
    if data_id not in data_store:
        return jsonify({'success': False, 'error': 'Data not found'}), 404

    include_platform = request.args.get('include_platform', 'true').lower() == 'true'
    df = data_store[data_id]

    result = merger_service.aggregate_by_campaign(df, include_platform)

    if result['success']:
        return jsonify({
            'success': True,
            'data': result['data'].to_dict('records'),
            'total_campaigns': result['total_campaigns']
        })

    return jsonify(result), 400


@app.route('/api/compare/platforms/<data_id>', methods=['GET'])
def compare_platforms(data_id):
    """
    Compare performance across platforms.
    """
    if data_id not in data_store:
        return jsonify({'success': False, 'error': 'Data not found'}), 404

    df = data_store[data_id]
    result = merger_service.compare_platforms(df)

    return jsonify(result)


# ============== AI INSIGHTS ENDPOINTS ==============

@app.route('/api/insights/<data_id>', methods=['POST'])
def generate_insights(data_id):
    """
    Generate AI-powered insights from the data.
    """
    if data_id not in data_store:
        return jsonify({'success': False, 'error': 'Data not found'}), 404

    df = data_store[data_id]
    options = request.get_json() or {}

    question = options.get('question')
    focus_area = options.get('focus_area')

    result = insights_service.generate_insights(df, question, focus_area)

    return jsonify({
        'success': True,
        'insights': result
    })


# ============== REPORT GENERATION ENDPOINTS ==============

@app.route('/api/report/excel/<data_id>', methods=['POST'])
def generate_excel_report(data_id):
    """
    Generate an Excel report.
    """
    if data_id not in data_store:
        return jsonify({'success': False, 'error': 'Data not found'}), 404

    df = data_store[data_id]
    options = request.get_json() or {}

    report_name = options.get('report_name', 'marketing_report')
    include_charts = options.get('include_charts', True)
    insights = options.get('insights')

    result = report_service.generate_excel_report(df, report_name, include_charts, insights)

    return jsonify(result)


@app.route('/api/report/pdf/<data_id>', methods=['POST'])
def generate_pdf_report(data_id):
    """
    Generate a PDF report.
    """
    if data_id not in data_store:
        return jsonify({'success': False, 'error': 'Data not found'}), 404

    df = data_store[data_id]
    options = request.get_json() or {}

    report_name = options.get('report_name', 'marketing_report')
    client_name = options.get('client_name', 'Client')
    insights = options.get('insights')
    include_charts = options.get('include_charts', True)

    result = report_service.generate_pdf_report(df, report_name, client_name, insights, include_charts)

    return jsonify(result)


@app.route('/api/export/csv/<data_id>', methods=['GET'])
def export_csv(data_id):
    """
    Export data as CSV.
    """
    if data_id not in data_store:
        return jsonify({'success': False, 'error': 'Data not found'}), 404

    df = data_store[data_id]
    result = report_service.generate_csv_export(df)

    return jsonify(result)


# ============== FILE DOWNLOAD ENDPOINTS ==============

@app.route('/api/download/<filename>', methods=['GET'])
def download_file(filename):
    """
    Download a generated report file.
    """
    try:
        return send_from_directory(REPORT_FOLDER, filename, as_attachment=True)
    except FileNotFoundError:
        return jsonify({'success': False, 'error': 'File not found'}), 404


# ============== DATA RETRIEVAL ENDPOINTS ==============

@app.route('/api/data/<data_id>', methods=['GET'])
def get_data(data_id):
    """
    Get stored data.
    """
    if data_id not in data_store:
        return jsonify({'success': False, 'error': 'Data not found'}), 404

    df = data_store[data_id]

    # Pagination
    page = int(request.args.get('page', 1))
    per_page = int(request.args.get('per_page', 100))

    start_idx = (page - 1) * per_page
    end_idx = start_idx + per_page

    return jsonify({
        'success': True,
        'data_id': data_id,
        'total_rows': len(df),
        'page': page,
        'per_page': per_page,
        'data': df.iloc[start_idx:end_idx].to_dict('records'),
        'columns': list(df.columns)
    })


@app.route('/api/data/<data_id>/stats', methods=['GET'])
def get_data_stats(data_id):
    """
    Get statistical summary of stored data.
    """
    if data_id not in data_store:
        return jsonify({'success': False, 'error': 'Data not found'}), 404

    df = data_store[data_id]

    # Numeric columns stats
    numeric_df = df.select_dtypes(include=[np.number])
    stats = {
        'count': len(df),
        'columns': len(df.columns),
        'numeric_columns': list(numeric_df.columns),
        'statistics': numeric_df.describe().to_dict()
    }

    return jsonify({
        'success': True,
        'stats': stats
    })


# ============== SAMPLE DATA ENDPOINTS ==============

@app.route('/api/sample-data', methods=['GET'])
def get_sample_data():
    """
    Generate sample marketing data for testing.
    """
    from datetime import timedelta
    import random

    platforms = ['Google Ads', 'Meta Ads', 'TikTok Ads']
    campaigns = {
        'Google Ads': ['Brand_Search', 'Competitor_Search', 'Display_Retargeting', 'YouTube_Awareness'],
        'Meta Ads': ['Lookalike_1%', 'Interest_Targeting', 'Retargeting', 'Broad_Audience'],
        'TikTok Ads': ['Trend_Jacker', 'Creator_Partnership', 'Spark_Ads', 'In_Feed_Ads']
    }

    data = []
    base_date = datetime.now() - timedelta(days=30)

    for day in range(30):
        date = base_date + timedelta(days=day)

        for platform in platforms:
            for campaign in campaigns[platform]:
                # Generate realistic metrics
                impressions = random.randint(5000, 50000)
                clicks = int(impressions * random.uniform(0.005, 0.05))
                spend = round(clicks * random.uniform(0.5, 3), 2)
                conversions = int(clicks * random.uniform(0.01, 0.1))
                conversion_value = round(conversions * random.uniform(20, 100), 2)

                data.append({
                    'date': date.strftime('%Y-%m-%d'),
                    'platform': platform,
                    'campaign_name': campaign,
                    'impressions': impressions,
                    'clicks': clicks,
                    'spend': spend,
                    'conversions': conversions,
                    'conversion_value': conversion_value
                })

    df = pd.DataFrame(data)
    data_id = f"sample_{datetime.now().strftime('%Y%m%d%H%M%S')}"
    data_store[data_id] = df

    return jsonify({
        'success': True,
        'data_id': data_id,
        'message': 'Generated 30 days of sample marketing data',
        'platforms': platforms,
        'total_rows': len(df),
        'preview': df.head(20).to_dict('records')
    })


# Run the app
if __name__ == '__main__':
    print("🚀 Marketing Data Engine API starting...")
    print("📍 API running on http://localhost:5001")
    print("📚 Available endpoints:")
    print("   POST /api/upload - Upload a data file")
    print("   POST /api/upload-multiple - Upload multiple files")
    print("   POST /api/normalize/<data_id> - Normalize data")
    print("   GET  /api/quality/check/<data_id> - Check data quality")
    print("   GET  /api/quality/anomalies/<data_id> - Detect anomalies")
    print("   GET  /api/aggregate/date/<data_id> - Aggregate by date")
    print("   GET  /api/aggregate/campaign/<data_id> - Aggregate by campaign")
    print("   POST /api/insights/<data_id> - Generate AI insights")
    print("   POST /api/report/excel/<data_id> - Generate Excel report")
    print("   GET  /api/sample-data - Generate sample data")

    app.run(host='0.0.0.0', port=5001, debug=True)