# Advanced RAG Pipeline - Technical Documentation

## Overview

This pipeline implements an advanced Retrieval-Augmented Generation (RAG) system specifically designed for processing and querying PDF documents, with a focus on financial and tabular data. It uses a multi-document, multi-user architecture with intelligent routing between text and table retrieval strategies.

## Architecture

### Core Components

1. **FastAPI Server** (`main.py`)
   - RESTful API endpoints for document upload and querying
   - User-based document isolation
   - Multi-document state management
   - Query pipeline orchestration

2. **Helper Functions** (`helper.py`)
   - PDF parsing and processing
   - Node parsing with table extraction
   - Query engine creation
   - Dynamic tool mapping generation

3. **Query Pipeline** (LlamaIndex QueryPipeline)
   - Intelligent routing between text and table retrievers
   - Sub-question generation
   - Parallel retrieval and response synthesis

### Data Flow

```
PDF Upload → LlamaParse → Document Description Generation → Node Parsing (MarkdownElementNodeParser) 
→ Table Extraction & Summarization → Qdrant Vector Storage → Query Engine Creation → Tool Mapping 
→ Query Pipeline → Query Processing → Response Synthesis
```

## Key Integrations

### 1. **LlamaIndex Framework** (v0.12.30)
- **Purpose**: Core RAG framework for document processing and querying
- **Key Components Used**:
  - `QueryPipeline`: Orchestrates query processing workflow
  - `VectorStoreIndex`: Manages vector embeddings and retrieval
  - `LLMQuestionGenerator`: Generates sub-questions for complex queries
  - `ResponseSynthesizer`: Combines retrieved information into final answers
- **Location**: Used throughout `main.py` and `helper.py`

### 2. **Qdrant Vector Database**
- **Purpose**: Persistent vector storage for document embeddings
- **Configuration**: 
  - Default: `localhost:6333` (REST API), `localhost:6334` (gRPC)
  - Configurable via `QDRANT_HOST` and `QDRANT_PORT` environment variables
- **Storage Strategy**:
  - Separate collections per document: `{tool_name}_combined` and `{tool_name}_table`
  - Metadata filtering by `user_id`, `doc_id`, and `tool_name`
- **Location**: `helper.py::get_query_engine()`, `docker-compose.yml`

### 3. **OpenAI Integration**
- **LLM**: GPT-3.5-turbo for text generation, question generation, and response synthesis
- **Embeddings**: OpenAI embeddings for vectorization
- **Configuration**: Set via `OPENAI_API_KEY` environment variable
- **Location**: `main.py::initialize_models()`, used in query pipeline functions

### 4. **LlamaParse**
- **Purpose**: PDF parsing service (cloud-based)
- **Output**: Markdown format with preserved table structures
- **Configuration**: Set via `LLAMAPARSE_API_KEY` environment variable
- **Location**: `helper.py::parse_pdfs_parallel()`, `main.py::initialize_parser()`

### 5. **SentenceTransformer Reranker**
- **Model**: cross-encoder/ms-marco-MiniLM-L-2-v2
- **Purpose**: Reranks retrieved nodes to improve relevance
- **Configuration**: `top_n=5` (configurable in `helper.py::get_query_engine()`)
- **Thread Safety**: Uses `reranker_lock` for concurrent access
- **Location**: `helper.py::get_query_engine()`

### 6. **MarkdownElementNodeParser**
- **Purpose**: Custom node parser that extracts and processes:
  - Text nodes
  - Table nodes (with automatic summarization)
  - Code blocks
  - Titles/headings
- **Key Features**:
  - Table extraction with column analysis
  - Table summarization using LLM
  - Element merging and filtering
- **Location**: `helper.py::MarkdownElementNodeParser`

## File Structure

```
pipeline_v1_final/
├── main.py                    # FastAPI server, endpoints, query pipeline
├── helper.py                   # Core processing functions, node parsers, query engines
├── start_server.sh            # Server startup script (uses pyenv Python 3.12.8)
├── requirements.txt           # Python dependencies
├── docker-compose.yml         # Qdrant container configuration
├── README.md                  # This file
├── test_server.py             # Server testing utilities
└── complete_pipeline.ipynb    # Jupyter notebook for development/testing
```

## API Endpoints

### POST `/upload`
- **Purpose**: Upload and process PDF documents
- **Parameters**: 
  - `user_id` (form field): User identifier for document isolation
  - `files` (form files): One or more PDF files
- **Behavior**: 
  - Processes PDFs: parsing → description → nodes → query engines
  - Merges with existing documents for the same user
  - Creates/updates query pipeline with all user documents
- **Returns**: Upload status with document IDs

### POST `/query`
- **Purpose**: Query user's documents
- **Body**: `{"query": "question", "user_id": "user123"}`
- **Behavior**:
  - Generates sub-questions
  - Routes to appropriate retriever (table vs combined)
  - Retrieves from all user's documents in parallel
  - Synthesizes final response
- **Returns**: Answer with source information

### GET `/users/{user_id}/documents`
- **Purpose**: List all documents for a user
- **Returns**: List of document tool names and descriptions

### GET `/health`
- **Purpose**: Health check endpoint
- **Returns**: Server status, Qdrant connection, model initialization status

## State Management

### User Data Store (`user_data_store`)
- **Type**: In-memory dictionary (`Dict[str, Dict]`)
- **Structure**:
  ```python
  {
    "user_id": {
      "document_configs": [  # List of all document configs
        {
          "tool_name": "user_id_doc_id",
          "description": "Document description",
          "query_engine": QueryEngine,
          "table_query_engine": QueryEngine
        }
      ],
      "tool_choices": [ToolMetadata, ...],
      "query_engine_tools_map": {"tool_name": QueryEngine, ...},
      "table_query_engine_tools_map": {"tool_name": QueryEngine, ...},
      "qp": QueryPipeline  # Query pipeline with all documents
    }
  }
  ```
- **Multi-Document Support**: New uploads are merged with existing documents
- **Isolation**: Each user's documents are completely isolated

## Query Pipeline Logic

### Pipeline Flow

1. **Input** → User query string
2. **Generate Sub-Questions** → Breaks complex queries into sub-questions per document
3. **Decide Retriever** → LLM decides between:
   - **Table Retriever**: For numerical/financial data queries
   - **Combined Retriever**: For narrative/contextual queries
4. **Retrieve** → Parallel retrieval from all user documents
5. **Response** → Generate answers for each sub-question
6. **Response Synthesizer** → Combine all answers into final professional response

### Retriever Decision Logic

The pipeline uses an LLM-based decision system to route queries:
- **Table Retriever**: Numerical data, financial metrics, calculations, specific figures
- **Combined Retriever**: Explanations, descriptions, strategies, qualitative analysis

## Integration Guidelines

### For Integration into Existing Pipeline

#### 1. **Dependencies**
Ensure these packages are installed (see `requirements.txt`):
- `llama-index-core==0.12.30`
- `llama-index-embeddings-openai`
- `llama-index-llms-openai`
- `llama-index-vector-stores-qdrant`
- `llama-index-postprocessor-sentence-transformer-rerank`
- `llama-parse`
- `qdrant-client==1.11.0`
- `fastapi`, `uvicorn`, `python-multipart`
- `pandas`, `tqdm`, `python-dotenv`

#### 2. **Environment Variables**
Required:
- `OPENAI_API_KEY`: OpenAI API key for LLM and embeddings

Optional:
- `LLAMAPARSE_API_KEY`: For PDF parsing (required for upload functionality)
- `QDRANT_HOST`: Qdrant host (default: `localhost`)
- `QDRANT_PORT`: Qdrant port (default: `6333`)

#### 3. **External Services**
- **Qdrant**: Must be running (via Docker or standalone)
- **LlamaParse API**: Cloud service (requires API key)
- **OpenAI API**: Cloud service (requires API key)

#### 4. **Integration Points**

**A. Import Functions from Helper:**
```python
from helper import (
    process_multiple_documents,
    generate_pdf_paths_with_doc_ids,
    MarkdownElementNodeParser,
    get_qdrant_client,
    create_dynamic_tool_mappings
)
```

**B. Initialize Components:**
```python
# Initialize models (LLM and embeddings)
Settings.llm = OpenAI(model="gpt-3.5-turbo", api_key=os.getenv("OPENAI_API_KEY"))
Settings.embed_model = OpenAIEmbedding(api_key=os.getenv("OPENAI_API_KEY"))

# Initialize parser
parser = LlamaParse(result_type="markdown", api_key=os.getenv("LLAMAPARSE_API_KEY"))

# Initialize node parser
node_parser = MarkdownElementNodeParser(llm=None, num_workers=8)
```

**C. Process Documents:**
```python
# Process PDFs
pdf_paths_with_doc_ids = generate_pdf_paths_with_doc_ids(pdf_paths, user_id=user_id)
document_configs, tool_choices, query_engine_tools_map, table_query_engine_tools_map = \
    process_multiple_documents(user_id, pdf_paths_with_doc_ids, parser, node_parser)
```

**D. Create Query Pipeline:**
```python
from main import create_query_pipeline
qp = create_query_pipeline(tool_choices, query_engine_tools_map, table_query_engine_tools_map)
```

#### 5. **State Management Considerations**

- **Current Implementation**: In-memory dictionary (`user_data_store`)
- **For Production**: Consider persisting to database (Redis, PostgreSQL, etc.)
- **Multi-Document Merging**: Logic in `main.py::upload_documents()` (lines 501-515)
- **User Isolation**: All operations are scoped by `user_id`

#### 6. **Async Context Handling**

The pipeline handles both sync and async contexts:
- **FastAPI**: Async endpoints with running event loop
- **Notebooks**: Sync context
- **Fix Applied**: `helper.py::extract_table_summaries()` uses thread pool for async-safe execution

#### 7. **Thread Safety**

- **Reranker**: Protected by `reranker_lock` for concurrent access
- **Qdrant Client**: Thread-safe (creates new client per operation or shares singleton)
- **Query Engines**: Each document has isolated query engines

#### 8. **Error Handling**

- Environment variable validation on startup
- Try-catch blocks around document processing
- HTTP exception handling for API endpoints
- Temporary file cleanup in finally blocks

## Configuration

### Python Environment
- **Required**: Python 3.12.8 (pyenv) - as used in notebook
- **Script**: `start_server.sh` uses `/Users/harshagrawal/.pyenv/versions/3.12.8/bin/python`
- **Note**: Ensure all packages are installed in this environment

### Qdrant Configuration
- **Docker**: Use `docker-compose up -d` to start Qdrant
- **Standalone**: Configure `QDRANT_HOST` and `QDRANT_PORT` environment variables
- **Collections**: Automatically created per document with naming: `{tool_name}_combined` and `{tool_name}_table`

### Model Configuration
- **LLM**: GPT-3.5-turbo (configurable in `main.py::initialize_models()`)
- **Embeddings**: OpenAI embeddings (configurable in `main.py::initialize_models()`)
- **Reranker**: cross-encoder/ms-marco-MiniLM-L-2-v2 (configurable in `helper.py::get_query_engine()`)

## Key Design Decisions

1. **Multi-Document Support**: Documents are merged per user, allowing queries across all uploaded PDFs
2. **Dual Retrieval Strategy**: Separate retrievers for tables vs text improve accuracy for different query types
3. **User Isolation**: Complete data isolation using `user_id` in metadata and collection names
4. **Dynamic Tool Mapping**: Query engines are created dynamically per document and merged for multi-document queries
5. **Table Intelligence**: Automatic table extraction, summarization, and column analysis for better retrieval

## Important Notes

### Breaking Changes to Avoid

1. **Don't modify `user_data_store` structure** - Used throughout the codebase
2. **Don't change `tool_name` format** - Expected as `{user_id}_{doc_id}`
3. **Don't remove metadata fields** - `user_id`, `doc_id`, `tool_name` are required for filtering
4. **Don't change collection naming** - Qdrant collections follow `{tool_name}_{type}` pattern

### Known Limitations

1. **In-Memory State**: `user_data_store` is not persisted (restart loses state)
2. **Temporary Files**: PDFs are stored temporarily during processing
3. **Event Loop**: Requires special handling for async contexts (already fixed)
4. **Thread Safety**: Reranker uses lock, but consider for other shared resources

### Performance Considerations

- **Parallel Processing**: PDF parsing uses ThreadPoolExecutor
- **Parallel Retrieval**: Query processing uses ThreadPoolExecutor (max 5 workers)
- **Reranking**: Applied to top 10 results, reranked to top 5
- **Vector Search**: Qdrant handles similarity search efficiently

## Testing

Run the test script:
```bash
python test_server.py <path_to_pdf> [user_id]
```

Or use the test integration script:
```bash
python test_qdrant_integration.py
```

## Troubleshooting

### Common Issues

1. **"asyncio.run() cannot be called from a running event loop"**
   - **Fixed**: Code now handles both sync and async contexts
   - **Location**: `helper.py::extract_table_summaries()`

2. **"No module named 'openai.types'"**
   - **Solution**: Use the same Python environment as notebook (pyenv 3.12.8)
   - **Fix**: Update `start_server.sh` to use correct Python path

3. **"Can't patch loop of type uvloop.Loop"**
   - **Fixed**: Removed `nest_asyncio.apply()` from module level
   - **Location**: `helper.py` (removed at import time)

4. **Qdrant Connection Error**
   - **Solution**: Ensure Qdrant is running: `docker-compose up -d`
   - **Check**: `docker ps | grep qdrant`

5. **Multi-Document State Loss**
   - **Fixed**: Documents are now merged instead of replaced
   - **Location**: `main.py::upload_documents()` (lines 501-515)

## Development

- **Notebook**: `complete_pipeline.ipynb` for development and testing
- **Server**: FastAPI with auto-reload enabled in development mode
- **Logging**: Configured at INFO level, can be adjusted in `main.py`

## License

See project root LICENSE file.
