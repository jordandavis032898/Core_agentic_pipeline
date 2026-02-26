# UI Testing Guide - New RAG Integration

This guide will help you test the new RAG implementation through the web UI.

## Prerequisites

1. **Backend API** running (see main TESTING_GUIDE.md)
2. **Node.js and npm** installed
3. **API Keys** configured in backend `.env`

## Quick Start

### Step 1: Start the Backend API

In terminal 1:
```bash
cd "/Users/harshagrawal/vivek's_workspace/agentic_router_v2/agentic_router_integrated/agentic-router"
source venv/bin/activate
uvicorn api:app --reload --host 0.0.0.0 --port 8000
```

Wait for: `Application startup complete` message

### Step 2: Configure UI Environment

```bash
cd agentic-router-ui

# Create .env file for UI (if it doesn't exist)
cat > .env << EOF
VITE_API_URL=http://localhost:8000
EOF
```

### Step 3: Install UI Dependencies (First Time Only)

```bash
cd agentic-router-ui
npm install
```

### Step 4: Start the UI Development Server

In terminal 2:
```bash
cd "/Users/harshagrawal/vivek's_workspace/agentic_router_v2/agentic_router_integrated/agentic-router/agentic-router-ui"
npm run dev
```

The UI will start at `http://localhost:5173` (or another port if 5173 is busy)

### Step 5: Open in Browser

Open your browser and navigate to:
```
http://localhost:5173
```

## Testing Workflow

### 1. Upload a Document

1. Click on the **"Documents"** tab (or it should be open by default)
2. Click **"Upload Document"** or drag and drop a PDF file
3. The UI will automatically:
   - Generate a unique `user_id` (stored in localStorage)
   - Include `user_id` in the upload request
   - Show upload progress

**Expected Result:**
- Success toast notification
- Document appears in the documents list
- Document shows as "processing" initially

### 2. Check Document Status

1. The document list will show your uploaded document
2. Status indicators:
   - **Processing**: Chatbot is still ingesting the document
   - **Ready**: Document is ready for querying

**Note:** Large documents may take 30-60 seconds to process

### 3. Query the Document

1. Click on the **"Chat"** tab
2. Select your document from the dropdown (if multiple documents)
3. Type a question in the input box, for example:
   - "What is this document about?"
   - "Summarize the key points"
   - "What are the main findings?"
4. Press Enter or click Send

**Expected Result:**
- Your question appears in the chat
- AI response appears with:
  - Professional formatting
  - Executive Summary
  - Key Metrics
  - Detailed Analysis
  - Source chunks (if available)

### 4. Test User Isolation

To test that `user_id` isolation works:

1. **Option A - Different Browser/Incognito:**
   - Open the UI in an incognito/private window
   - This will generate a new `user_id`
   - Upload a different document
   - Query - you should only see documents from this session

2. **Option B - Clear localStorage:**
   - Open browser console (F12)
   - Run: `localStorage.clear()`
   - Refresh the page
   - This generates a new `user_id`

### 5. Test Multi-Document Queries

1. Upload multiple PDFs (with the same `user_id` - same browser session)
2. Go to Chat tab
3. Don't select a specific document (or select "All Documents")
4. Ask a question that might span multiple documents
5. The query should search across all your uploaded documents

## UI Features to Test

### ✅ Document Management
- [ ] Upload PDF files
- [ ] View document list
- [ ] See document status (processing/ready)
- [ ] Delete documents

### ✅ Chat Interface
- [ ] Select specific document to query
- [ ] Query without document selection (searches all)
- [ ] View formatted responses
- [ ] See source chunks
- [ ] Filter by metadata (if available)

### ✅ User ID Management
- [ ] User ID automatically generated
- [ ] User ID persists across page refreshes
- [ ] Different browsers get different user IDs
- [ ] User isolation works correctly

### ✅ Response Quality
- [ ] Answers are professionally formatted
- [ ] Includes Executive Summary
- [ ] Includes Key Metrics (for numerical queries)
- [ ] Includes Detailed Analysis
- [ ] Source attribution works

## Troubleshooting

### UI won't start
```bash
# Check if port is in use
lsof -i :5173

# Try different port
npm run dev -- --port 3000
```

### UI can't connect to backend
1. **Check backend is running:**
   ```bash
   curl http://localhost:8000/health
   ```

2. **Check VITE_API_URL:**
   - Verify `.env` file in `agentic-router-ui/` has:
     ```
     VITE_API_URL=http://localhost:8000
     ```
   - Restart the dev server after changing `.env`

3. **Check CORS:**
   - Backend should allow CORS from `http://localhost:5173`
   - Check `api.py` has CORS middleware configured

### Upload fails
- Check browser console (F12) for errors
- Verify backend is running
- Check file size (may have limits)
- Verify LLAMA_CLOUD_API_KEY is set in backend `.env`

### Query returns "No documents found"
- Wait for document processing to complete
- Check document status in Documents tab
- Verify you're using the same browser session (same `user_id`)
- Check browser console for errors

### Responses are empty or errors
- Check browser console (F12) for API errors
- Verify OPENAI_API_KEY is set in backend `.env`
- Check backend logs for errors
- Verify Qdrant is running and accessible

## Browser Console Commands

Open browser console (F12) to debug:

```javascript
// Check current user_id
localStorage.getItem('agentic_router_user_id')

// Clear user_id (generates new one on refresh)
localStorage.removeItem('agentic_router_user_id')

// Clear all localStorage
localStorage.clear()

// Check API URL
import.meta.env.VITE_API_URL
```

## Expected API Calls

When using the UI, you should see these API calls in the Network tab (F12):

1. **GET /health** - Health check
2. **GET /documents** - Fetch all documents
3. **GET /filters** - Get available filters
4. **POST /upload** - Upload PDF (with `user_id` in form data)
5. **GET /status/{file_id}** - Check document status
6. **POST /query** - Query document (with `user_id` in JSON body)

## Testing Checklist

- [ ] Backend API starts successfully
- [ ] UI starts successfully
- [ ] UI connects to backend
- [ ] Can upload PDF files
- [ ] Documents appear in list
- [ ] Document status updates correctly
- [ ] Can query documents
- [ ] Responses are formatted correctly
- [ ] User isolation works (different browsers)
- [ ] Multi-document queries work
- [ ] No console errors
- [ ] No CORS errors

## Next Steps

After successful UI testing:
1. Test with different document types
2. Test with multiple users simultaneously
3. Verify response accuracy
4. Check performance with large documents
5. Test edge cases (empty queries, invalid files, etc.)
