#!/bin/bash
# Marketing Data Engine Startup Script

echo "🚀 Starting Marketing Data Engine..."

# Change to project directory
cd /home/z/my-project/marketing-data-engine

# Activate virtual environment
source venv/bin/activate

# Create necessary directories
mkdir -p data/uploads data/reports

echo "Starting Flask API on port 5001..."
python api/app.py &
API_PID=$!

echo "Waiting for API to start..."
sleep 3

echo "Starting Streamlit on port 8501..."
streamlit run frontend/app.py --server.port 8501 --server.address 0.0.0.0 &
STREAMLIT_PID=$!

echo ""
echo "✅ Marketing Data Engine is running!"
echo ""
echo "📍 API URL: http://localhost:5001"
echo "📍 Streamlit URL: http://localhost:8501"
echo ""
echo "Press Ctrl+C to stop all services"

# Wait for processes
wait $API_PID $STREAMLIT_PID
