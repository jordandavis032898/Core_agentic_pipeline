"""
Chatbot Adapter - Uses new RAG implementation from pipeline_v1_final

This adapter wraps the advanced RAG implementation from pipeline_v1_final
and provides a compatible interface for the agentic router.
"""
import os
import logging
import threading
import re
import asyncio
from typing import List, Dict, Optional, Any, Callable
from datetime import datetime
from pathlib import Path
import sys

# Add pipeline_v1_final to path for imports
_pipeline_v1_path = Path(__file__).parent.parent / "pipeline_v1_final"
if str(_pipeline_v1_path) not in sys.path:
    sys.path.insert(0, str(_pipeline_v1_path))

# Import new RAG components from pipeline_v1_final
from main import (
    RAGQueryWorkflow,
    create_query_pipeline,
    user_data_store,
    initialize_models,
    initialize_node_parser,
    QueryEvent,
    SubQuestion,
    SubQuestionAnswerPair,
)
from helper import (
    MarkdownElementNodeParser,
    get_query_engine,
    create_dynamic_tool_mappings,
    generate_document_description,
    get_qdrant_client,
)

logger = logging.getLogger(__name__)


# ==================== Helper Functions ====================
def extract_doc_id_from_path(file_path: str) -> str:
    """Extract document ID from file path."""
    filename = os.path.basename(file_path)
    doc_id = os.path.splitext(filename)[0]
    # Sanitize doc_id
    doc_id = re.sub(r'[^a-zA-Z0-9_-]', '_', doc_id)
    doc_id = re.sub(r'[_\s]+', '_', doc_id).strip('_')
    return doc_id


# ==================== ChatbotAdapter Class ====================
class ChatbotAdapter:
    """
    Chatbot Adapter using new RAG implementation from pipeline_v1_final.
    
    This adapter:
    1. Uses MarkdownElementNodeParser for intelligent node extraction
    2. Creates dual query engines (combined and table-only) per document
    3. Uses Workflow API for sophisticated query routing and synthesis
    4. Supports user isolation via user_id
    5. Provides reranking for improved retrieval quality
    """
    
    def __init__(
        self,
        openai_api_key: str,
        qdrant_url: str,
        collection_name: str = "policy_documents",
        qdrant_api_key: Optional[str] = None,
        embedding_model: str = "text-embedding-3-small",
        llm_model: str = "gpt-4o",
        top_k: int = 5,
        default_author: str = "unknown",
        default_company: str = "unknown",
        num_workers: int = 8
    ):
        """
        Initialize chatbot adapter.
        
        Args:
            openai_api_key: OpenAI API key
            qdrant_url: Qdrant URL (e.g., http://localhost:6333)
            collection_name: Qdrant collection name (legacy, not used in new implementation)
            qdrant_api_key: Optional Qdrant API key (for Qdrant Cloud)
            embedding_model: Embedding model name
            llm_model: LLM model name
            top_k: Number of top chunks to retrieve (legacy, reranker handles this)
            default_author: Default author value
            default_company: Default company value
            num_workers: Number of workers for node parser
        """
        # Set API keys
        os.environ["OPENAI_API_KEY"] = openai_api_key
        
        self.qdrant_url = qdrant_url
        self.qdrant_api_key = qdrant_api_key
        self.embedding_model = embedding_model
        self.llm_model = llm_model
        self.default_author = default_author
        self.default_company = default_company
        
        # Initialize models (shared with pipeline_v1_final)
        try:
            initialize_models()
        except Exception as e:
            logger.warning(f"Models may already be initialized: {e}")
        
        # Initialize node parser (shared with pipeline_v1_final)
        try:
            initialize_node_parser()
            # Import the global node_parser instance after initialization
            import main as pipeline_main
            if hasattr(pipeline_main, 'node_parser') and pipeline_main.node_parser is not None:
                self.node_parser = pipeline_main.node_parser
            else:
                # Create our own instance if global one doesn't exist
                self.node_parser = MarkdownElementNodeParser(llm=None, num_workers=num_workers)
        except Exception as e:
            logger.warning(f"Node parser initialization issue: {e}, creating new instance")
            # Create our own instance if initialization fails
            self.node_parser = MarkdownElementNodeParser(llm=None, num_workers=num_workers)
        
        # Initialize Qdrant client (supports local host/port or Cloud url + api_key)
        try:
            if qdrant_url and "://" in qdrant_url:
                self.qdrant_client = get_qdrant_client(url=qdrant_url, api_key=qdrant_api_key)
            else:
                from urllib.parse import urlparse
                parsed = urlparse(qdrant_url or "http://localhost:6333")
                host = parsed.hostname or "localhost"
                port = parsed.port or 6333
                self.qdrant_client = get_qdrant_client(host=host, port=port)
        except Exception as e:
            logger.error(f"Failed to initialize Qdrant client: {e}")
            raise
        
        # Use shared user_data_store from pipeline_v1_final
        # This ensures consistency across the system
        self.user_data_store = user_data_store
        
        # Track processing status per file_path
        self._processing_files = set()  # Files currently being processed
        self._processed_files = set()  # Files that have been processed
        self._error_files = {}  # Files that failed with error messages
        self._file_to_user_map = {}  # Map file_path -> user_id
        self._index_lock = threading.Lock()  # Lock for thread-safe updates
        
        logger.info("Chatbot adapter initialized successfully with new RAG from pipeline_v1_final")
    
    def ingest_documents_from_parsed(
        self,
        documents: List[Any],
        file_path: str,
        user_id: str,
        metadata_override: Optional[Dict[str, Any]] = None,
        background: bool = True,
        log_callback: Optional[Callable[[str, str], None]] = None
    ) -> bool:
        """
        Ingest documents from already-parsed documents (from shared parser).
        
        This processes documents using MarkdownElementNodeParser and creates
        dual query engines (combined and table-only) for Workflow-based query processing.
        
        Args:
            documents: List of parsed document objects (from shared parser)
            file_path: Path to the PDF file
            user_id: User ID for document isolation
            metadata_override: Optional metadata overrides
            background: If True, run in background thread
            log_callback: Optional callback function(log_message, status) for logging
            
        Returns:
            bool: True if ingestion started (or completed if not background)
        """
        if file_path in self._processing_files:
            if log_callback:
                log_callback(f"File {file_path} is already being processed", "info")
            return False
        
        # Check if already processed for this user
        file_key = f"{user_id}:{file_path}"
        if file_key in self._processed_files:
            if log_callback:
                log_callback(f"File {file_path} is already indexed for user {user_id}", "info")
            return False
        
        if background:
            # Run in background thread
            logger.info(f"[Chatbot Ingestion] Starting background ingestion thread for {file_path}, user_id: {user_id}")
            thread = threading.Thread(
                target=self._ingest_documents_sync,
                args=(documents, file_path, user_id, metadata_override, log_callback),
                daemon=True
            )
            thread.start()
            logger.info(f"[Chatbot Ingestion] Background ingestion thread started for {file_path}")
            if log_callback:
                log_callback(f"Started background ingestion for {file_path}", "running")
            return True
        else:
            # Run synchronously
            return self._ingest_documents_sync(
                documents, file_path, user_id, metadata_override, log_callback
            )
    
    def _ingest_documents_sync(
        self,
        documents: List[Any],
        file_path: str,
        user_id: str,
        metadata_override: Optional[Dict[str, Any]],
        log_callback: Optional[Callable[[str, str], None]]
    ) -> bool:
        """Internal method to ingest documents synchronously."""
        self._processing_files.add(file_path)
        self._file_to_user_map[file_path] = user_id
        
        try:
            logger.info(f"[Chatbot Ingestion] Starting ingestion for file: {file_path}, user_id: {user_id}")
            if log_callback:
                log_callback(f"Generating nodes for {file_path}...", "running")
            
            # Step 1: Generate nodes using MarkdownElementNodeParser
            nodes = self.node_parser.get_nodes_from_documents(documents)
            
            logger.info(f"[Chatbot Ingestion] Generated {len(nodes)} nodes from {len(documents)} documents")
            if log_callback:
                log_callback(f"Generated {len(nodes)} nodes", "success")
            
            # Step 2: Generate document description
            logger.info("[Chatbot Ingestion] Generating document description...")
            if log_callback:
                log_callback("Generating document description...", "running")
            description = generate_document_description(documents)
            
            # Step 3: Extract doc_id from file path
            doc_id = extract_doc_id_from_path(file_path)
            tool_name = f"{user_id}_{doc_id}"
            logger.info(f"[Chatbot Ingestion] Extracted doc_id: {doc_id}, tool_name: {tool_name}")
            
            # Step 4: Create query engines (combined and table-only)
            logger.info(f"[Chatbot Ingestion] Creating query engines for tool_name: {tool_name}...")
            if log_callback:
                log_callback("Creating query engines...", "running")
            
            query_engine, table_query_engine = get_query_engine(
                nodes, 
                self.node_parser, 
                tool_name=tool_name,
                qdrant_client=self.qdrant_client
            )
            
            logger.info(f"[Chatbot Ingestion] Query engines created successfully for {tool_name}")
            if log_callback:
                log_callback("Query engines created", "success")
            
            # Step 5: Create document config
            document_config = {
                "tool_name": tool_name,
                "description": description,
                "query_engine": query_engine,
                "table_query_engine": table_query_engine
            }
            
            # Step 6: Update user data store (shared with pipeline_v1_final)
            with self._index_lock:
                if user_id not in self.user_data_store:
                    self.user_data_store[user_id] = {
                        "document_configs": [],
                        "tool_choices": [],
                        "query_engine_tools_map": {},
                        "table_query_engine_tools_map": {},
                        "qp": None
                    }
                
                # Check if document already exists (by tool_name)
                existing_configs = self.user_data_store[user_id]["document_configs"]
                existing_tool_names = [c["tool_name"] for c in existing_configs]
                
                if tool_name not in existing_tool_names:
                    # Add new document
                    existing_configs.append(document_config)
                    
                    # Rebuild tool mappings from all document configs
                    all_tool_choices, all_query_engine_tools_map, all_table_query_engine_tools_map = \
                        create_dynamic_tool_mappings(existing_configs)
                    
                    # Create/update workflow (replaces query pipeline)
                    workflow = create_query_pipeline(
                        all_tool_choices,
                        all_query_engine_tools_map,
                        all_table_query_engine_tools_map
                    )
                    
                    # Update user data store
                    self.user_data_store[user_id].update({
                        "document_configs": existing_configs,
                        "tool_choices": all_tool_choices,
                        "query_engine_tools_map": all_query_engine_tools_map,
                        "table_query_engine_tools_map": all_table_query_engine_tools_map,
                        "qp": workflow  # Store workflow instead of query pipeline
                    })
                    logger.info(f"[Chatbot Ingestion] Updated user data store for {user_id} with {len(existing_configs)} document(s)")
                else:
                    logger.info(f"[Chatbot Ingestion] Document {tool_name} already exists for user {user_id}, skipping")
            
            file_key = f"{user_id}:{file_path}"
            self._processed_files.add(file_key)
            
            logger.info(f"[Chatbot Ingestion] ✓ Successfully ingested document {tool_name} for user {user_id}")
            if log_callback:
                log_callback(f"Successfully ingested document {tool_name}", "success")
            
            return True
            
        except Exception as e:
            error_msg = str(e)
            logger.error(f"Error ingesting documents from {file_path}: {error_msg}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            self._error_files[file_path] = error_msg
            if log_callback:
                log_callback(f"Error ingesting {file_path}: {error_msg}", "error")
            return False
        finally:
            self._processing_files.discard(file_path)
    
    async def query_async(
        self,
        query: str,
        user_id: str,
        metadata_filters: Optional[Dict[str, Any]] = None,
        auto_detect_filters: bool = True
    ) -> Dict[str, Any]:
        """
        Query the RAG pipeline using Workflow API (async version).
        
        Args:
            query: User query string
            user_id: User ID to filter documents
            metadata_filters: Optional manual metadata filters dict (legacy, not used in new implementation)
            auto_detect_filters: Whether to auto-detect filters from query (legacy, not used)
            
        Returns:
            dict: {
                "answer": str,
                "chunks": List[str],  # Source chunks
                "filters_applied": dict or None
            }
        """
        try:
            # Check if user has documents
            if user_id not in self.user_data_store:
                return {
                    "answer": f"No documents found for user {user_id}. Please upload documents first.",
                    "chunks": [],
                    "filters_applied": None
                }
            
            user_data = self.user_data_store[user_id]
            workflow = user_data.get("qp")
            
            if not workflow:
                return {
                    "answer": "Query pipeline not initialized. Please upload documents first.",
                    "chunks": [],
                    "filters_applied": None
                }
            
            logger.info(f"Processing query for user {user_id}: {query[:100]}...")
            
            # Run workflow directly in async context (no thread executor needed)
            # workflow.run() accepts keyword arguments that create QueryEvent internally
            result_event = await workflow.run(query=query)
            
            # Extract response from StopEvent
            response = result_event.result if hasattr(result_event, 'result') else result_event
            response_text = str(response)
            
            # Extract source chunks from response if available
            chunks = []
            if hasattr(response, 'source_nodes'):
                chunks = [node.get_content() for node in response.source_nodes[:5]]
            elif hasattr(response, 'get_formatted_sources'):
                chunks = response.get_formatted_sources(length=200)
            
            logger.info(f"Query processed successfully for user {user_id}")
            
            return {
                "answer": response_text,
                "chunks": chunks,
                "filters_applied": {"user_id": user_id}  # user_id is the main filter
            }
            
        except Exception as e:
            logger.error(f"Error during query: {str(e)}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            raise e
    
    def query(
        self,
        query: str,
        user_id: str,
        metadata_filters: Optional[Dict[str, Any]] = None,
        auto_detect_filters: bool = True
    ) -> Dict[str, Any]:
        """
        Query the RAG pipeline using Workflow API (sync wrapper for async method).
        
        This method is called from sync code in pipeline.py, so we need to run
        the async query in a new event loop.
        
        Args:
            query: User query string
            user_id: User ID to filter documents
            metadata_filters: Optional manual metadata filters dict (legacy, not used in new implementation)
            auto_detect_filters: Whether to auto-detect filters from query (legacy, not used)
            
        Returns:
            dict: {
                "answer": str,
                "chunks": List[str],  # Source chunks
                "filters_applied": dict or None
            }
        """
        # Check if we're already in an async context
        try:
            loop = asyncio.get_running_loop()
            # We're in an async context, but this is a sync method
            # We need to run in a thread with a new event loop
            import concurrent.futures
            
            def run_in_new_loop():
                new_loop = asyncio.new_event_loop()
                asyncio.set_event_loop(new_loop)
                try:
                    return new_loop.run_until_complete(
                        self.query_async(query, user_id, metadata_filters, auto_detect_filters)
                    )
                finally:
                    new_loop.close()
            
            with concurrent.futures.ThreadPoolExecutor() as executor:
                future = executor.submit(run_in_new_loop)
                return future.result(timeout=300)  # 5 minute timeout
        except RuntimeError:
            # No running event loop, create a new one
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                return loop.run_until_complete(
                    self.query_async(query, user_id, metadata_filters, auto_detect_filters)
                )
            finally:
                loop.close()
    
    def is_ready(self) -> bool:
        """Check if pipeline is ready."""
        return self.qdrant_client is not None
    
    def is_processing(self, file_path: str) -> bool:
        """Check if a file is currently being processed."""
        return file_path in self._processing_files
    
    def is_processed(self, file_path: str) -> bool:
        """Check if a file has been processed."""
        # Check all user contexts for this file
        user_id = self._file_to_user_map.get(file_path)
        if user_id:
            file_key = f"{user_id}:{file_path}"
            return file_key in self._processed_files
        return False
    
    def has_error(self, file_path: str) -> bool:
        """Check if a file has encountered an error."""
        return file_path in self._error_files
    
    def get_error(self, file_path: str) -> Optional[str]:
        """Get the error message for a file if it has one."""
        return self._error_files.get(file_path)
    
    def get_all_documents(self, filter_user_id: str = None) -> List[Dict[str, Any]]:
        """
        Get documents with their metadata, optionally filtered by user_id.

        Args:
            filter_user_id: If provided, only return documents for this user

        Returns:
            List of document metadata dicts
        """
        documents = []
        users_to_scan = self.user_data_store.items()
        if filter_user_id and filter_user_id in self.user_data_store:
            users_to_scan = [(filter_user_id, self.user_data_store[filter_user_id])]
        elif filter_user_id:
            return []  # User has no documents

        for user_id, user_data in users_to_scan:
            for config in user_data.get("document_configs", []):
                tool_name = config["tool_name"]
                # Extract doc_id from tool_name (format: user_id_doc_id)
                parts = tool_name.split("_", 1)
                doc_id = parts[1] if len(parts) == 2 else tool_name

                documents.append({
                    "hash": tool_name,  # Use tool_name as hash for compatibility
                    "file_id": tool_name,
                    "title": doc_id,
                    "source": "policy_document",
                    "author": self.default_author,
                    "company": self.default_company,
                    "upload_date": datetime.now().strftime("%Y-%m-%d"),
                    "user_id": user_id,
                    "tool_name": tool_name
                })

        return documents
    
    def get_available_filters(self) -> Dict[str, List[str]]:
        """
        Get all available metadata filter options.
        
        Returns:
            Dict with field names as keys and list of distinct values as values
        """
        filters = {
            "user_id": list(self.user_data_store.keys())
        }
        
        # Get distinct tool names (documents)
        tool_names = set()
        for user_data in self.user_data_store.values():
            for config in user_data.get("document_configs", []):
                tool_names.add(config["tool_name"])
        filters["tool_name"] = sorted(list(tool_names))
        
        return filters
    
    def close(self) -> None:
        """
        Close all connections and clean up resources.
        Call this when done using the adapter to prevent resource leaks.
        """
        try:
            # Close Qdrant client
            if hasattr(self, 'qdrant_client') and self.qdrant_client:
                self.qdrant_client.close()
                logger.info("Qdrant client closed")
            
            logger.info("ChatbotAdapter resources cleaned up")
        except Exception as e:
            logger.warning(f"Error during cleanup: {e}")
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - clean up resources."""
        self.close()
        return False
