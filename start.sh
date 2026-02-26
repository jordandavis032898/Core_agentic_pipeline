#!/bin/bash
# Unified startup script for Agentic Router

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "=========================================="
echo "  Starting Agentic Router"
echo "=========================================="

# 1. Start Qdrant
echo ""
echo "1. Starting Qdrant..."
if docker ps | grep -q qdrant; then
    echo "   ✓ Qdrant already running"
elif docker ps -a | grep -q qdrant; then
    echo "   Starting existing Qdrant container..."
    docker start qdrant
    sleep 2
else
    echo "   Creating Qdrant container..."
    docker run -d --name qdrant -p 6333:6333 -p 6334:6334 qdrant/qdrant:latest
    sleep 3
fi

# Check Qdrant health
if curl -s http://localhost:6333/health > /dev/null 2>&1; then
    echo "   ✓ Qdrant is healthy"
else
    echo "   ⚠️  Qdrant may still be starting..."
fi

# 2. Check .env file
echo ""
echo "2. Checking configuration..."
if [ ! -f ".env" ]; then
    echo "   ⚠️  .env file not found!"
    echo "   Creating from env.example..."
    cp env.example .env
    echo "   ⚠️  Please edit .env and add your API keys:"
    echo "      - OPENAI_API_KEY"
    echo "      - LLAMA_CLOUD_API_KEY"
    echo ""
    read -p "   Press Enter after adding API keys, or Ctrl+C to exit..."
fi

# 3. Setup UI .env
echo ""
echo "3. Setting up UI..."
cd agentic-router-ui
if [ ! -f ".env" ]; then
    echo "VITE_API_URL=http://localhost:8000" > .env
    echo "   ✓ Created UI .env file"
fi

# Check if node_modules exists
if [ ! -d "node_modules" ]; then
    echo "   Installing UI dependencies..."
    npm install
fi
cd ..

# 4. Check virtual environment
echo ""
echo "4. Checking Python environment..."
if [ ! -d "venv" ]; then
    echo "   ✗ Virtual environment not found!"
    echo "   Run: python3 -m venv venv && source venv/bin/activate && pip install -r requirements.txt"
    exit 1
fi

# 5. Start backend in background
echo ""
echo "5. Starting Backend API..."
# Use venv Python directly to ensure correct environment
nohup venv/bin/python -m uvicorn api:app --host 0.0.0.0 --port 8000 > backend.log 2>&1 &
BACKEND_PID=$!
echo "   Backend started (PID: $BACKEND_PID)"
echo "   Using Python: $(venv/bin/python --version)"
echo "   Logs: tail -f backend.log"

# 6. Start UI in background
echo ""
echo "6. Starting UI..."
cd agentic-router-ui
nohup npm run dev > ../ui.log 2>&1 &
UI_PID=$!
cd ..
echo "   UI started (PID: $UI_PID)"
echo "   Logs: tail -f ui.log"

# Wait for services to start
echo ""
echo "Waiting for services to start..."
sleep 5

# Check services
echo ""
echo "=========================================="
echo "  Service Status"
echo "=========================================="

if curl -s http://localhost:6333/health > /dev/null 2>&1; then
    echo "✓ Qdrant: http://localhost:6333"
else
    echo "⚠ Qdrant: Not responding"
fi

if curl -s http://localhost:8000/health > /dev/null 2>&1; then
    echo "✓ Backend: http://localhost:8000"
    echo "  API Docs: http://localhost:8000/docs"
else
    echo "⚠ Backend: Starting... (check backend.log)"
fi

if curl -s http://localhost:5173 > /dev/null 2>&1; then
    echo "✓ UI: http://localhost:5173"
else
    echo "⚠ UI: Starting... (check ui.log)"
fi

echo ""
echo "=========================================="
echo "  Services Started!"
echo "=========================================="
echo ""
echo "Access the UI at: http://localhost:5173"
echo ""
echo "To view logs:"
echo "  tail -f backend.log"
echo "  tail -f ui.log"
echo ""
echo "To stop services:"
echo "  kill $BACKEND_PID $UI_PID"
echo "  docker stop qdrant"
echo ""
