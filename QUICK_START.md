# Quick Start Guide - Using Python 3.12.8

## All Services Startup

Use the provided script to start everything:

```bash
cd "/Users/harshagrawal/vivek's_workspace/agentic_router_v2/agentic_router_integrated/agentic-router"
./start_services.sh
```

This will:
1. Start Qdrant container
2. Check/configure .env files
3. Start Backend API (using Python 3.12.8)
4. Start UI development server
5. Show service status

## Manual Start (3 Terminals)

### Terminal 1 - Qdrant
```bash
docker start qdrant
# Or if not exists:
docker run -d --name qdrant -p 6333:6333 -p 6334:6334 qdrant/qdrant:latest
```

### Terminal 2 - Backend API
```bash
cd "/Users/harshagrawal/vivek's_workspace/agentic_router_v2/agentic_router_integrated/agentic-router"
/Users/harshagrawal/.pyenv/versions/3.12.8/bin/python -m uvicorn api:app --reload --host 0.0.0.0 --port 8000
```

### Terminal 3 - UI
```bash
cd "/Users/harshagrawal/vivek's_workspace/agentic_router_v2/agentic_router_integrated/agentic-router/agentic-router-ui"
npm run dev
```

## Access Points

- **UI**: http://localhost:5173
- **Backend API**: http://localhost:8000
- **API Docs**: http://localhost:8000/docs
- **Qdrant**: http://localhost:6333

## Verify Services

```bash
# Check Qdrant
curl http://localhost:6333/health

# Check Backend
curl http://localhost:8000/health

# Check UI (should return HTML)
curl http://localhost:5173
```

## Testing

1. Open http://localhost:5173 in browser
2. Upload a PDF document
3. Wait for processing (check Documents tab)
4. Go to Chat tab and ask questions

## Troubleshooting

### Backend won't start
- Check `.env` file has API keys
- Check logs: `tail -f backend.log` (if using script) or check terminal output
- Verify Python 3.12.8 has packages: `/Users/harshagrawal/.pyenv/versions/3.12.8/bin/python -c "from chatbot.chatbot_adapter import ChatbotAdapter"`

### UI won't start
- Check if port 5173 is available
- Check logs: `tail -f ui.log` (if using script)
- Verify `.env` in `agentic-router-ui/` has `VITE_API_URL=http://localhost:8000`

### Import errors
- Make sure you're using Python 3.12.8: `/Users/harshagrawal/.pyenv/versions/3.12.8/bin/python`
- All packages should be installed in that Python environment
