# Testing Guide - New RAG Integration


## Prerequisites

1. **Python 3.11+** installed
2. **Qdrant** running (local or remote)
3. **API Keys** configured:
   - `OPENAI_API_KEY`
   - `LLAMA_CLOUD_API_KEY`

## Setup Steps

### 1. Activate Virtual Environment

```bash
cd "/Users/harshagrawal/vivek's_workspace/agentic_router_v2/agentic_router_integrated/agentic-router"
source venv/bin/activate
```

### 2. Install/Update Dependencies

```bash
pip install --upgrade pip
pip install -r requirements.txt
```

### 3. Configure Environment Variables

```bash
# Copy env.example to .env if it doesn't exist
cp env.example .env

# Edit .env and add your API keys
# Required:
# - OPENAI_API_KEY=your_key_here
# - LLAMA_CLOUD_API_KEY=your_key_here
# - QDRANT_URL=http://localhost:6333 (or your Qdrant URL)
```

### 4. Start Qdrant (if using local)

```bash
# Using docker-compose (if available)
docker-compose up -d

# Or start Qdrant manually
# Make sure Qdrant is accessible at the URL specified in .env
```

### 5. Start the API Server

```bash
# In the activated venv
uvicorn api:app --reload --host 0.0.0.0 --port 8000
```

The server should start and be accessible at `http://localhost:8000`

## Testing the APIs

### Option 1: Using the Test Script

```bash
# Make sure server is running first
# Then in another terminal:

source venv/bin/activate
python test_api_integration.py [path_to_pdf_file]
```

The test script will:
1. Check server health
2. Test route endpoint
3. Upload a PDF (if provided)
4. Check upload status
5. Query the document (with user_id)

### Option 2: Manual Testing with curl

#### 1. Health Check
```bash
curl http://localhost:8000/health
```

#### 2. Route a Query
```bash
curl -X POST http://localhost:8000/route \
  -H "Content-Type: application/json" \
  -d '{
    "query": "What is Apple revenue?",
    "pdf_uploaded": false
  }'
```

#### 3. Upload a PDF (with user_id)
```bash
curl -X POST http://localhost:8000/upload \
  -F "file=@/path/to/your/document.pdf" \
  -F "user_id=test_user_123"
```

Save the `file_id` from the response.

#### 4. Check Status
```bash
curl http://localhost:8000/status/{file_id}
```

Wait until `chatbot_ready` is `true`.

#### 5. Query the Document (with user_id)
```bash
curl -X POST http://localhost:8000/query \
  -H "Content-Type: application/json" \
  -d '{
    "file_id": "your_file_id_here",
    "user_id": "test_user_123",
    "question": "What is this document about?"
  }'
```

### Option 3: Using the UI

1. Start the UI (if available):
```bash
cd agentic-router-ui
npm install
npm run dev
```

2. Open the UI in browser (usually `http://localhost:5173`)

3. The UI will automatically:
   - Generate a user_id and store it in localStorage
   - Include user_id in all upload and query requests
   - Display documents and allow querying

## Expected Behavior

### Upload Endpoint
- **Request**: Must include `user_id` as form field
- **Response**: Returns `file_id`, `status`, `parsed`, `pages_count`, `chatbot_status`
- **Processing**: PDF is parsed, nodes are generated, query engines are created in background

### Query Endpoint
- **Request**: Must include `user_id` in JSON body
- **Response**: Returns structured answer with:
  - `answer`: Professional formatted response
  - `chunks`: Source chunks used
  - `filters_applied`: Applied filters (includes user_id)
- **Isolation**: Only queries documents belonging to the specified user_id

### Key Features to Test

1. **User Isolation**: 
   - Upload same document with different user_ids
   - Query with different user_ids - should only see own documents

2. **Multi-Document Support**:
   - Upload multiple PDFs with same user_id
   - Query should search across all user's documents

3. **Dual Retrievers**:
   - Ask numerical questions → should use table retriever
   - Ask narrative questions → should use combined retriever

4. **Response Quality**:
   - Answers should be professionally formatted
   - Should include Executive Summary, Key Metrics, etc.

## Troubleshooting

### Server won't start
- Check if port 8000 is available
- Verify all dependencies are installed
- Check .env file has required API keys

### Upload fails
- Verify PDF file is valid
- Check LLAMA_CLOUD_API_KEY is set correctly
- Check file size limits

### Query returns "No documents found"
- Verify user_id matches the one used during upload
- Check chatbot_ready status in /status endpoint
- Wait a bit longer for ingestion to complete

### Qdrant connection errors
- Verify Qdrant is running: `curl http://localhost:6333/health`
- Check QDRANT_URL in .env matches your Qdrant instance
- For Qdrant Cloud, ensure QDRANT_API_KEY is set

### Import errors
- Make sure all requirements are installed: `pip install -r requirements.txt`
- Verify Python version is 3.11+
- Check that pipeline_v1_final/helper.py is accessible

## Test Checklist

- [ ] Server starts without errors
- [ ] Health check returns 200
- [ ] Route endpoint works
- [ ] Upload endpoint accepts user_id
- [ ] Status endpoint shows chatbot processing/ready
- [ ] Query endpoint works with user_id
- [ ] User isolation works (different user_ids see different documents)
- [ ] Multi-document queries work
- [ ] Response format is professional and structured

## Next Steps

After successful testing:
1. Verify all endpoints work as expected
2. Test with multiple users and documents
3. Check response quality and accuracy
4. Monitor performance and resource usage
