"""
FastAPI Backend for Agentic Router

Production-ready REST API for query routing, PDF processing,
table extraction, chatbot Q&A, and EDGAR data fetching.

Run with: uvicorn api:app --host 0.0.0.0 --port 8000
"""

import os
import sys
import uuid
import shutil
import logging
import tempfile
from pathlib import Path
from typing import Optional, List, Dict, Any
from datetime import datetime

# Apply compatibility patch for OpenAI SDK version mismatch
# Must be imported before any llama-index imports
try:
    import compat_patch
except ImportError:
    pass  # Patch file not found, continue without it

# Package is properly structured - no sys.path manipulation needed

from fastapi import FastAPI, HTTPException, UploadFile, File, Form, Query, Depends, Header, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel, Field
from contextlib import asynccontextmanager

# Load environment variables (use ENV_FILE e.g. .env.prod if set)
try:
    from dotenv import load_dotenv
    _env_file = os.getenv("ENV_FILE", ".env")
    _env_path = Path(__file__).parent / _env_file
    if _env_path.exists():
        load_dotenv(_env_path, override=True)
    else:
        load_dotenv(Path(__file__).parent / ".env", override=True)
except ImportError:
    pass

# Try to import PyMuPDF for PDF preview
try:
    import fitz  # PyMuPDF
    PYMUPDF_AVAILABLE = True
except ImportError:
    PYMUPDF_AVAILABLE = False

# Import agentic router components
# Use absolute imports to work when running directly with uvicorn
import sys
from pathlib import Path
# Add current directory to path for imports
sys.path.insert(0, str(Path(__file__).parent))

from pipeline import AgenticRouter
from config import RouterConfig
from router.query_router import RouteType

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ============================================================================
# GLOBAL STATE
# ============================================================================

# Router instance (initialized on startup)
router_instance: Optional[AgenticRouter] = None

# File storage (in production, use S3/GCS)
UPLOAD_DIR = Path(tempfile.gettempdir()) / "agentic_router_uploads"
UPLOAD_DIR.mkdir(exist_ok=True)

# File metadata storage (in production, use Redis/DB)
file_metadata: Dict[str, Dict[str, Any]] = {}


# ============================================================================
# PYDANTIC MODELS - Request/Response Schemas
# ============================================================================

# --- Route Endpoint ---
class RouteRequest(BaseModel):
    query: str = Field(..., description="User's natural language query")
    pdf_uploaded: bool = Field(False, description="Whether a PDF has been uploaded")

    class Config:
        json_schema_extra = {
            "example": {
                "query": "Get Apple's financials for the last 3 years",
                "pdf_uploaded": False
            }
        }


class RouteResponse(BaseModel):
    route: str = Field(..., description="Route type: public_data, table_extraction, chatbot, upload_pdf, unclear")
    confidence: float = Field(..., description="Confidence score 0.0-1.0")
    explanation: str = Field(..., description="Why this route was chosen")
    extracted_params: Dict[str, Any] = Field(default_factory=dict, description="Extracted parameters like ticker, num_years")
    requires_pdf: bool = Field(..., description="Whether this route requires a PDF")
    next_action: str = Field(..., description="Suggested next action for UI")
    next_endpoint: Optional[str] = Field(None, description="Suggested next API endpoint")


# --- Upload Endpoint ---
class UploadResponse(BaseModel):
    file_id: str
    filename: str
    status: str
    parsed: bool
    pages_count: int
    filtered_pages_count: int
    chatbot_status: str
    message: str


# --- Status Endpoint ---
class StatusResponse(BaseModel):
    file_id: str
    filename: str
    parsed: bool
    extractor_ready: bool
    chatbot_ready: bool
    chatbot_processing: bool
    chatbot_error: Optional[str]


# --- Pages Endpoint ---
class PageInfo(BaseModel):
    page_index: int
    page_number: int
    table_type: str
    confidence: float
    snippet: Optional[str]
    filter_result: Dict[str, Any]


class PagesResponse(BaseModel):
    file_id: str
    total_pages: int
    pages_with_tables: List[PageInfo]


# --- Extract Endpoint ---
class ExtractRequest(BaseModel):
    file_id: str = Field(..., description="File ID from upload")
    page_indices: List[int] = Field(..., description="List of page indices to extract")

    class Config:
        json_schema_extra = {
            "example": {
                "file_id": "f7a8b9c0-1234-5678-9abc-def012345678",
                "page_indices": [12, 15, 18]
            }
        }


class ExtractedTable(BaseModel):
    page_index: int
    page_number: int
    extraction_status: str
    data: Optional[Dict[str, Any]]
    table_metadata: Optional[Dict[str, Any]]
    explanation: Optional[str]
    error: Optional[str]


class ExtractResponse(BaseModel):
    file_id: str
    extracted_tables: List[ExtractedTable]
    summary: Dict[str, int]


# --- Query Endpoint ---
class QueryRequest(BaseModel):
    file_id: str = Field(..., description="File ID from upload")
    user_id: str = Field(..., description="User ID for document isolation")
    question: str = Field(..., description="Question to ask about the document")
    filters: Optional[Dict[str, str]] = Field(None, description="Optional metadata filters")
    auto_detect_filters: bool = Field(True, description="Auto-detect filters from query")

    class Config:
        json_schema_extra = {
            "example": {
                "file_id": "f7a8b9c0-1234-5678-9abc-def012345678",
                "user_id": "user123",
                "question": "What was the total revenue in 2024?",
                "auto_detect_filters": True
            }
        }


class ChunkInfo(BaseModel):
    content: str
    score: Optional[float]


class QueryResponse(BaseModel):
    question: str
    answer: str
    chunks: List[ChunkInfo]
    filters_applied: Optional[Dict[str, str]]


# --- EDGAR Endpoint ---
class EdgarResponse(BaseModel):
    ticker: str
    filings_count: int
    balance_sheet: Optional[Dict[str, Any]]
    income_statement: Optional[Dict[str, Any]]
    cash_flow: Optional[Dict[str, Any]]
    metadata: Dict[str, Any]


# --- Generic Response Wrapper ---
class APIResponse(BaseModel):
    success: bool
    data: Optional[Any] = None
    error: Optional[Dict[str, Any]] = None
    metadata: Optional[Dict[str, Any]] = None


# ============================================================================
# AUTHENTICATION
# ============================================================================

async def verify_api_key(x_api_key: str = Header(None, alias="X-API-Key")):
    """Verify API key from header"""
    expected_key = os.getenv("API_KEY", "")
    
    # If no API key is configured, allow all requests (development mode)
    if not expected_key:
        return True
    
    if not x_api_key or x_api_key != expected_key:
        raise HTTPException(
            status_code=401,
            detail={
                "code": "INVALID_API_KEY",
                "message": "Missing or invalid API key"
            }
        )
    return True


# ============================================================================
# LIFESPAN - Startup/Shutdown
# ============================================================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initialize and cleanup resources"""
    global router_instance
    
    # Startup
    logger.info("Starting Agentic Router API...")
    
    try:
        config = RouterConfig.from_env()
        router_instance = AgenticRouter(config)
        logger.info("✓ AgenticRouter initialized successfully")
    except Exception as e:
        logger.error(f"✗ Failed to initialize AgenticRouter: {e}")
        # Continue without router - endpoints will return errors
    
    yield
    
    # Shutdown
    logger.info("Shutting down Agentic Router API...")
    if router_instance:
        router_instance.close()
        logger.info("✓ AgenticRouter closed")


# ============================================================================
# FASTAPI APP
# ============================================================================

app = FastAPI(
    title="Agentic Router API",
    description="Intelligent query routing and financial document processing API",
    version="1.0.0",
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc"
)

# CORS middleware: allow deployed frontend + localhost for MVP demo / local dev
# Note: allow_credentials=True is incompatible with allow_origins=["*"],
# so we always enumerate explicit origins.
frontend_uri = os.getenv("FRONTEND_URI", "")
cors_origins = [o.strip() for o in frontend_uri.split(",") if o.strip()]
# Always allow deployed frontend + localhost for dev
extra_origins = [
    "https://web-production-4c4e8.up.railway.app",
    "https://agentic-router-standalone-production.up.railway.app",
    "http://localhost:5174",
    "http://localhost:5173",
    "http://localhost:3000",
    "http://127.0.0.1:5174",
    "http://127.0.0.1:5173",
    "http://127.0.0.1:3000",
]
for origin in extra_origins:
    if origin not in cors_origins:
        cors_origins.append(origin)
app.add_middleware(
    CORSMiddleware,
    allow_origins=cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Request logging middleware to help diagnose connection issues
@app.middleware("http")
async def log_requests(request, call_next):
    """Log all requests for debugging connection issues"""
    start_time = datetime.utcnow()
    try:
        response = await call_next(request)
        process_time = (datetime.utcnow() - start_time).total_seconds()
        logger.info(
            f"{request.method} {request.url.path} - "
            f"Status: {response.status_code} - "
            f"Time: {process_time:.3f}s"
        )
        return response
    except Exception as e:
        process_time = (datetime.utcnow() - start_time).total_seconds()
        logger.error(
            f"{request.method} {request.url.path} - "
            f"Error: {str(e)} - "
            f"Time: {process_time:.3f}s",
            exc_info=True
        )
        raise


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def get_router() -> AgenticRouter:
    """Get router instance or raise error"""
    if router_instance is None:
        raise HTTPException(
            status_code=503,
            detail={
                "code": "SERVICE_UNAVAILABLE",
                "message": "Router not initialized. Check API keys and configuration."
            }
        )
    return router_instance


def get_file_path(file_id: str) -> Path:
    """Get file path from file_id"""
    if file_id not in file_metadata:
        raise HTTPException(
            status_code=404,
            detail={
                "code": "FILE_NOT_FOUND",
                "message": f"File with ID '{file_id}' not found"
            }
        )
    return Path(file_metadata[file_id]["path"])


# ============================================================================
# API ENDPOINTS
# ============================================================================

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "router_initialized": router_instance is not None
    }


@app.post("/route", response_model=APIResponse)
async def route_query(
    request: RouteRequest,
    _: bool = Depends(verify_api_key)
):
    """
    Route a user query to the appropriate pipeline.
    
    This is the main entry point - call this first to determine
    which pipeline to use based on the user's natural language query.
    """
    router = get_router()
    
    try:
        result = router.route(request.query, pdf_uploaded=request.pdf_uploaded)
        
        # Determine next action based on route
        next_action_map = {
            RouteType.PUBLIC_DATA: "call_edgar",
            RouteType.TABLE_EXTRACTION: "get_pages",
            RouteType.CHATBOT: "query_chatbot",
            RouteType.UPLOAD_PDF: "upload_pdf",
            RouteType.UNCLEAR: "clarify_query"
        }
        
        next_endpoint_map = {
            RouteType.PUBLIC_DATA: f"/edgar/{result.extracted_params.get('ticker', '')}" if result.extracted_params.get('ticker') else "/edgar/{ticker}",
            RouteType.TABLE_EXTRACTION: "/pages/{file_id}",
            RouteType.CHATBOT: "/query",
            RouteType.UPLOAD_PDF: "/upload",
            RouteType.UNCLEAR: None
        }
        
        response_data = RouteResponse(
            route=result.route.value,
            confidence=result.confidence,
            explanation=result.explanation,
            extracted_params=result.extracted_params,
            requires_pdf=result.requires_pdf(),
            next_action=next_action_map.get(result.route, "unknown"),
            next_endpoint=next_endpoint_map.get(result.route)
        )
        
        return APIResponse(
            success=True,
            data=response_data.model_dump(),
            metadata={"original_query": request.query}
        )
        
    except Exception as e:
        logger.error(f"Route error: {e}")
        raise HTTPException(
            status_code=500,
            detail={"code": "ROUTING_ERROR", "message": str(e)}
        )


@app.post("/upload", response_model=APIResponse)
async def upload_pdf(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
    user_id: str = Form(...),
    metadata: Optional[str] = Form(None),
    _: bool = Depends(verify_api_key)
):
    """
    Upload and process a PDF file.
    
    The PDF will be parsed with LlamaParse, prefiltered for tables,
    and chatbot ingestion will start in the background.
    """
    router = get_router()
    
    # Validate file type
    if not file.filename.lower().endswith('.pdf'):
        raise HTTPException(
            status_code=400,
            detail={"code": "INVALID_FILE_TYPE", "message": "Only PDF files are accepted"}
        )
    
    # Generate file ID and save file
    file_id = str(uuid.uuid4())
    file_path = UPLOAD_DIR / f"{file_id}.pdf"
    
    try:
        with open(file_path, "wb") as f:
            shutil.copyfileobj(file.file, f)
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail={"code": "UPLOAD_FAILED", "message": str(e)}
        )
    
    # Parse metadata if provided
    meta_dict = {}
    if metadata:
        import json
        try:
            meta_dict = json.loads(metadata)
        except:
            pass
    
    # Store file metadata
    file_metadata[file_id] = {
        "path": str(file_path),
        "filename": file.filename,
        "uploaded_at": datetime.utcnow().isoformat(),
        "metadata": meta_dict,
        "user_id": user_id
    }
    
    # Process PDF with user_id
    # Create log callback to ensure ingestion logs are visible in Docker logs
    def log_callback(message: str, status: str = "info"):
        """Log callback that ensures logs are visible in Docker logs"""
        log_level = logging.INFO if status in ["success", "info", "running"] else logging.ERROR
        if status == "running":
            logger.info(f"[PDF Processing] {message}")
        elif status == "success":
            logger.info(f"[PDF Processing] ✓ {message}")
        elif status == "error":
            logger.error(f"[PDF Processing] ✗ {message}")
        else:
            logger.info(f"[PDF Processing] {message}")
    
    try:
        status = router.process_pdf(str(file_path), user_id=user_id, log_callback=log_callback)
        
        file_metadata[file_id].update({
            "parsed": status.get("parsed", False),
            "pages_count": len(router.parser.get_cached_documents(str(file_path)) or []),
            "filtered_pages_count": status.get("filtered_pages_count", 0)
        })
        
        response_data = UploadResponse(
            file_id=file_id,
            filename=file.filename,
            status="processing" if status.get("chatbot_processing") else "ready",
            parsed=status.get("parsed", False),
            pages_count=file_metadata[file_id]["pages_count"],
            filtered_pages_count=status.get("filtered_pages_count", 0),
            chatbot_status="ingesting" if status.get("chatbot_processing") else "ready",
            message="PDF uploaded and processing started"
        )
        
        return APIResponse(
            success=True,
            data=response_data.model_dump()
        )
        
    except Exception as e:
        logger.error(f"PDF processing error: {e}")
        # Clean up file on error
        file_path.unlink(missing_ok=True)
        file_metadata.pop(file_id, None)
        raise HTTPException(
            status_code=500,
            detail={"code": "PROCESSING_ERROR", "message": str(e)}
        )


@app.get("/status/{file_id}", response_model=APIResponse)
async def get_status(
    file_id: str,
    _: bool = Depends(verify_api_key)
):
    """
    Get processing status for an uploaded PDF.
    
    Use this to check if chatbot ingestion is complete before querying.
    """
    router = get_router()
    file_path = get_file_path(file_id)
    
    response_data = StatusResponse(
        file_id=file_id,
        filename=file_metadata[file_id]["filename"],
        parsed=file_metadata[file_id].get("parsed", False),
        extractor_ready=router.get_filtered_pages(str(file_path)) is not None,
        chatbot_ready=router.is_chatbot_ready(str(file_path)),
        chatbot_processing=router.is_chatbot_processing(str(file_path)),
        chatbot_error=router.get_chatbot_error(str(file_path))
    )
    
    return APIResponse(
        success=True,
        data=response_data.model_dump()
    )


@app.get("/pages/{file_id}", response_model=APIResponse)
async def get_pages(
    file_id: str,
    _: bool = Depends(verify_api_key)
):
    """
    Get list of pages containing tables.
    
    Returns filtered pages (by list index). page_index = source of truth for
    extraction/selection; page_number (from ##PAGE:n##, 0 = null) is for display/preview only.
    """
    router = get_router()
    file_path = get_file_path(file_id)
    
    filtered_pages = router.get_filtered_pages(str(file_path))
    
    if filtered_pages is None:
        raise HTTPException(
            status_code=400,
            detail={
                "code": "NOT_PROCESSED",
                "message": "PDF has not been processed yet. Call /upload first."
            }
        )
    
    pages_info = []
    for page in filtered_pages:
        pages_info.append(PageInfo(
            page_index=page["index"],
            page_number=page["page_number"],  # from LlamaParse ##PAGE:n## only; 0 = not found
            table_type=page["filter_result"].get("type", "unknown"),
            confidence=0.9,  # Could be extracted from filter_result
            snippet=page["page_content"][:200] if page.get("page_content") else None,
            filter_result=page["filter_result"]
        ))
    response_data = PagesResponse(
        file_id=file_id,
        total_pages=file_metadata[file_id].get("pages_count", 0),
        pages_with_tables=pages_info
    )
    
    return APIResponse(
        success=True,
        data=response_data.model_dump()
    )


@app.get("/pages/{file_id}/{page_index}/preview")
async def get_page_preview(
    file_id: str,
    page_index: int,
    _: bool = Depends(verify_api_key)
):
    """
    Get a preview image of a specific PDF page.
    
    Path param is 0-based page index (same as list index from GET /pages/{file_id}).
    Returns PNG of PDF page (page_index + 1). Requires PyMuPDF (fitz).
    """
    if not PYMUPDF_AVAILABLE:
        raise HTTPException(
            status_code=501,
            detail={
                "code": "PREVIEW_NOT_AVAILABLE",
                "message": "PDF preview requires PyMuPDF. Install with: pip install pymupdf"
            }
        )
    
    file_path = get_file_path(file_id)
    if not file_path.exists():
        raise HTTPException(
            status_code=404,
            detail={"code": "FILE_NOT_FOUND", "message": f"PDF file not found at {file_path}"}
        )
    
    doc = None
    try:
        doc = fitz.open(str(file_path))
        if page_index < 0 or page_index >= len(doc):
            raise HTTPException(
                status_code=400,
                detail={
                    "code": "INVALID_PAGE_INDEX",
                    "message": f"Page index {page_index} out of range (0-{len(doc) - 1})"
                }
            )
        page = doc[page_index]
        pix = page.get_pixmap(matrix=fitz.Matrix(1.5, 1.5))
        UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
        preview_path = UPLOAD_DIR / f"{file_id}_page_{page_index}.png"
        if hasattr(pix, "writePNG") and callable(getattr(pix, "writePNG")):
            pix.writePNG(str(preview_path))
        else:
            pix.save(str(preview_path))
        if not preview_path.exists():
            raise HTTPException(
                status_code=500,
                detail={"code": "PREVIEW_ERROR", "message": "Preview file was not written"}
            )
        return FileResponse(
            str(preview_path),
            media_type="image/png",
            filename=f"page_{page_index + 1}.png"
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Preview failed for file_id=%s page_index=%s: %s", file_id, page_index, e)
        raise HTTPException(
            status_code=500,
            detail={"code": "PREVIEW_ERROR", "message": str(e)}
        )
    finally:
        if doc is not None:
            try:
                doc.close()
            except Exception:
                pass


@app.get("/pages/{file_id}/preview-by-page/{page_number}")
async def get_page_preview_by_number(
    file_id: str,
    page_number: int,
    _: bool = Depends(verify_api_key)
):
    """
    Get preview image for a 1-based PDF page number (from ##PAGE:n##).
    Use this when list index != PDF page (e.g. LlamaParse extra/split pages).
    Renders doc[page_number - 1].
    """
    if not PYMUPDF_AVAILABLE:
        raise HTTPException(
            status_code=501,
            detail={
                "code": "PREVIEW_NOT_AVAILABLE",
                "message": "PDF preview requires PyMuPDF. Install with: pip install pymupdf"
            }
        )
    file_path = get_file_path(file_id)
    if not file_path.exists():
        raise HTTPException(
            status_code=404,
            detail={"code": "FILE_NOT_FOUND", "message": f"PDF file not found at {file_path}"}
        )
    if page_number < 1:
        raise HTTPException(
            status_code=400,
            detail={"code": "INVALID_PAGE_NUMBER", "message": "page_number must be >= 1"}
        )
    doc = None
    try:
        doc = fitz.open(str(file_path))
        num_pages = len(doc)
        if page_number > num_pages:
            raise HTTPException(
                status_code=400,
                detail={
                    "code": "INVALID_PAGE_NUMBER",
                    "message": f"Page number {page_number} out of range (PDF has {num_pages} pages)"
                }
            )
        page_index = page_number - 1
        page = doc[page_index]
        pix = page.get_pixmap(matrix=fitz.Matrix(1.5, 1.5))
        UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
        preview_path = UPLOAD_DIR / f"{file_id}_pdfpage_{page_number}.png"
        if hasattr(pix, "writePNG") and callable(getattr(pix, "writePNG")):
            pix.writePNG(str(preview_path))
        else:
            pix.save(str(preview_path))
        if not preview_path.exists():
            raise HTTPException(
                status_code=500,
                detail={"code": "PREVIEW_ERROR", "message": "Preview file was not written"}
            )
        return FileResponse(
            str(preview_path),
            media_type="image/png",
            filename=f"page_{page_number}.png"
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Preview-by-page failed for file_id=%s page_number=%s: %s", file_id, page_number, e)
        raise HTTPException(
            status_code=500,
            detail={"code": "PREVIEW_ERROR", "message": str(e)}
        )
    finally:
        if doc is not None:
            try:
                doc.close()
            except Exception:
                pass


@app.post("/extract", response_model=APIResponse)
async def extract_tables(
    request: ExtractRequest,
    _: bool = Depends(verify_api_key)
):
    """
    Extract tables from selected pages. Uses only page_indices (list index);
    page_number in the response is for display only (from ##PAGE:n##).
    """
    router = get_router()
    file_path = get_file_path(request.file_id)
    
    try:
        results = router.extract_tables(
            str(file_path),
            request.page_indices
        )
        
        extracted_tables = []
        successful = 0
        failed = 0
        
        for result in results:
            if result.get("data"):
                successful += 1
                extracted_tables.append(ExtractedTable(
                    page_index=result.get("page_index", 0),
                    page_number=result.get("page_number", 0),  # from LlamaParse ##PAGE:n## only; 0 = unknown
                    extraction_status="success",
                    data=result.get("data"),
                    table_metadata=result.get("table_metadata"),
                    explanation=result.get("explanation"),
                    error=None
                ))
            else:
                failed += 1
                extracted_tables.append(ExtractedTable(
                    page_index=result.get("page_index", 0),
                    page_number=result.get("page_number", 0),  # from LlamaParse ##PAGE:n## only; 0 = unknown
                    extraction_status="failed",
                    data=None,
                    table_metadata=None,
                    explanation=None,
                    error=result.get("error", "Unknown extraction error")
                ))
        
        response_data = ExtractResponse(
            file_id=request.file_id,
            extracted_tables=extracted_tables,
            summary={
                "total_pages_processed": len(request.page_indices),
                "successful_extractions": successful,
                "failed_extractions": failed
            }
        )
        
        return APIResponse(
            success=True,
            data=response_data.model_dump()
        )
        
    except Exception as e:
        logger.error(f"Extraction error: {e}")
        raise HTTPException(
            status_code=500,
            detail={"code": "EXTRACTION_ERROR", "message": str(e)}
        )


@app.post("/query", response_model=APIResponse)
async def query_chatbot(
    request: QueryRequest,
    _: bool = Depends(verify_api_key)
):
    """
    Ask a question about an uploaded PDF.
    
    Uses RAG (Retrieval Augmented Generation) to find relevant
    chunks and generate an answer.
    """
    router = get_router()
    file_path = get_file_path(request.file_id)
    
    # Check if chatbot is ready
    if not router.is_chatbot_ready(str(file_path)):
        if router.is_chatbot_processing(str(file_path)):
            raise HTTPException(
                status_code=202,
                detail={
                    "code": "CHATBOT_NOT_READY",
                    "message": "Chatbot is still processing the document. Please wait and try again.",
                    "status": "processing"
                }
            )
        else:
            error = router.get_chatbot_error(str(file_path))
            raise HTTPException(
                status_code=400,
                detail={
                    "code": "CHATBOT_ERROR",
                    "message": error or "Chatbot processing failed"
                }
            )
    
    try:
        # Get user_id from request (required for new RAG implementation)
        user_id = request.user_id
        
        # Query chatbot with user_id - call async method directly since we're in async context
        result = await router.chatbot.query_async(
            request.question,
            user_id=user_id,
            metadata_filters=request.filters,
            auto_detect_filters=request.auto_detect_filters
        )
        
        chunks = [
            ChunkInfo(content=chunk, score=None)
            for chunk in result.get("chunks", [])
        ]
        
        response_data = QueryResponse(
            question=request.question,
            answer=result.get("answer", ""),
            chunks=chunks,
            filters_applied=result.get("filters_applied")
        )
        
        return APIResponse(
            success=True,
            data=response_data.model_dump()
        )
        
    except Exception as e:
        logger.error(f"Query error: {e}", exc_info=True)
        # Check for connection-related errors
        error_str = str(e).lower()
        if "connection" in error_str or "timeout" in error_str or "pool" in error_str:
            logger.error("Connection pool or timeout error detected - this may indicate resource exhaustion")
            raise HTTPException(
                status_code=503,
                detail={
                    "code": "SERVICE_UNAVAILABLE",
                    "message": "Service temporarily unavailable due to connection issues. Please try again.",
                    "original_error": str(e)
                }
            )
        raise HTTPException(
            status_code=500,
            detail={"code": "QUERY_ERROR", "message": str(e)}
        )


@app.get("/edgar/{ticker}", response_model=APIResponse)
async def get_edgar_data(
    ticker: str,
    num_years: int = 3,
    _: bool = Depends(verify_api_key)
):
    """
    Fetch financial data from SEC EDGAR.
    
    Returns both the raw per-filing data and a merged, unified
    view of balance sheet, income statement, and cash flow for
    the specified company ticker.
    """
    router = get_router()
    
    ticker = ticker.upper()
    
    try:
        result = router.fetch_edgar_data(ticker, num_years=num_years)
        
        if not result or "error" in result:
            raise HTTPException(
                status_code=404,
                detail={
                    "code": "EDGAR_NOT_FOUND",
                    "message": result.get("error", f"No data found for ticker '{ticker}'")
                }
            )
        
        # Format response
        merged = result.get("merged_edgar") or {}
        merged_years = result.get("merged_years") or []

        response_data = {
            "ticker": ticker,
            "filings_count": result.get("summary", {}).get("total_filings", 0),
            "balance_sheet": {
                "periods": [],
                "sections": result.get("balance_sheet_data", [])
            },
            "income_statement": {
                "periods": [],
                "sections": result.get("income_statement_data", [])
            },
            "cash_flow": {
                "periods": [],
                "sections": result.get("cash_flow_data", [])
            },
            # New: merged, line-item × year catalogs, matching the
            # format used by the test_edgar_format notebook and Excel export.
            "merged": {
                "years": merged_years,
                "balance_sheet": merged.get("balance_sheet", {}),
                "income_statement": merged.get("income_statement", {}),
                "cash_flow_statement": merged.get("cash_flow_statement", {}),
            },
            "metadata": {
                # Number of years requested by the client
                "years_requested": num_years,
                # Actual years available in the merged view (ascending)
                "years_fetched": merged_years,
                "fetched_at": datetime.utcnow().isoformat()
            }
        }
        
        return APIResponse(
            success=True,
            data=response_data
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"EDGAR error: {e}")
        raise HTTPException(
            status_code=500,
            detail={"code": "EDGAR_ERROR", "message": str(e)}
        )


@app.get("/documents", response_model=APIResponse)
async def list_documents(
    user_id: Optional[str] = Query(None, description="Filter documents by user_id"),
    _: bool = Depends(verify_api_key)
):
    """
    List documents stored in the vector database, optionally filtered by user_id.

    Returns unique documents with their metadata from Qdrant.
    """
    router = get_router()

    try:
        documents = router.chatbot.get_all_documents(filter_user_id=user_id)
        
        return APIResponse(
            success=True,
            data={
                "documents": documents,
                "total": len(documents)
            }
        )
    except Exception as e:
        logger.error(f"Error listing documents: {e}")
        raise HTTPException(
            status_code=500,
            detail={"code": "LIST_ERROR", "message": str(e)}
        )


@app.get("/filters", response_model=APIResponse)
async def get_filters(
    _: bool = Depends(verify_api_key)
):
    """
    Get available metadata filter options.
    
    Returns distinct values for each filterable metadata field.
    """
    router = get_router()
    
    try:
        filters = router.chatbot.get_available_filters()
        
        return APIResponse(
            success=True,
            data={
                "filters": filters,
                "available_fields": list(filters.keys())
            }
        )
    except Exception as e:
        logger.error(f"Error getting filters: {e}")
        raise HTTPException(
            status_code=500,
            detail={"code": "FILTERS_ERROR", "message": str(e)}
        )


@app.delete("/files/{file_id}", response_model=APIResponse)
async def delete_file(
    file_id: str,
    _: bool = Depends(verify_api_key)
):
    """
    Delete an uploaded PDF and its associated data.
    """
    file_path = get_file_path(file_id)
    
    try:
        # Delete file
        Path(file_path).unlink(missing_ok=True)
        
        # Delete preview images
        for preview in UPLOAD_DIR.glob(f"{file_id}_page_*.png"):
            preview.unlink(missing_ok=True)
        
        # Clear metadata
        file_metadata.pop(file_id, None)
        
        # Clear router cache
        if router_instance:
            router_instance.clear_cache(str(file_path))
        
        return APIResponse(
            success=True,
            data={"file_id": file_id, "message": "File deleted successfully"}
        )
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail={"code": "DELETE_ERROR", "message": str(e)}
        )


# ============================================================================
# MAIN
# ============================================================================

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "api:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )

