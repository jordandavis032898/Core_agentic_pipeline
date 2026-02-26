# Services Status

## Services Started

### 1. Qdrant Vector Database
- **Status**: Starting/Started
- **URL**: http://localhost:6333
- **Health Check**: http://localhost:6333/health
- **Container**: `qdrant`

### 2. Backend API Server
- **Status**: Starting/Started
- **URL**: http://localhost:8000
- **API Docs**: http://localhost:8000/docs
- **Health Check**: http://localhost:8000/health
- **Process**: Running in background

### 3. UI Development Server
- **Status**: Starting/Started
- **URL**: http://localhost:5173
- **Process**: Running in background

## Quick Access

- **UI**: http://localhost:5173
- **API Docs**: http://localhost:8000/docs
- **Health Check**: http://localhost:8000/health

## To Check Status

```bash
# Check Qdrant
curl http://localhost:6333/health

# Check Backend
curl http://localhost:8000/health

# Check if services are running
ps aux | grep uvicorn
ps aux | grep "npm run dev"
docker ps | grep qdrant
```

## To Stop Services

```bash
# Stop backend (find and kill process)
pkill -f "uvicorn api:app"

# Stop UI (find and kill process)
pkill -f "npm run dev"

# Stop Qdrant
docker stop qdrant
```

## Testing

1. Open browser: http://localhost:5173
2. Upload a PDF document
3. Wait for processing
4. Go to Chat tab and ask questions
