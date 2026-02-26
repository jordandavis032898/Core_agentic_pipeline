"""
Agentic Router Main Class

Combines query routing, PDF processing, table extraction, chatbot, and EDGAR scraping.
"""
import logging
import threading
from typing import List, Dict, Optional, Any, Callable, Tuple

# Apply compatibility patch for OpenAI SDK version mismatch
# Must be imported before any llama-index imports
try:
    import compat_patch
except ImportError:
    pass  # Patch file not found, continue without it

# Use absolute imports to work when running directly with uvicorn
try:
    from .config import RouterConfig
    from .shared.parser import SharedParser
    from .extractor.extractor_adapter import ExtractorAdapter
    from .chatbot.chatbot_adapter import ChatbotAdapter
    from .router.query_router import QueryRouter, RouteResult, RouteType
    from .edgar.orchestrator import AggregatedFinancialScraper
    from .edgar.merger_final import build_unified_catalog_all_statements
except ImportError:
    # Fallback to absolute imports when running as a script
    from config import RouterConfig
    from shared.parser import SharedParser
    from extractor.extractor_adapter import ExtractorAdapter
    from chatbot.chatbot_adapter import ChatbotAdapter
    from router.query_router import QueryRouter, RouteResult, RouteType
    from edgar.orchestrator import AggregatedFinancialScraper
    from edgar.merger_final import build_unified_catalog_all_statements


class AgenticRouter:
    """
    Agentic Router that combines query routing with multiple pipelines.
    
    This router:
    1. Uses LLM to route queries to the appropriate pipeline
    2. Parses PDFs once using shared parser
    3. Provides table extraction from PDFs
    4. Provides chatbot/RAG for PDF Q&A
    5. Fetches public company data from EDGAR
    """
    
    def __init__(self, config: RouterConfig):
        """
        Initialize agentic router.
        
        Args:
            config: RouterConfig object with all configuration
        """
        self.config = config
        
        # Initialize query router
        self.query_router = QueryRouter(openai_api_key=config.openai_api_key)
        
        # Initialize shared parser
        self.parser = SharedParser(
            llama_api_key=config.llama_cloud_api_key,
            use_multimodal=config.use_multimodal,
            parsing_instruction=config.parsing_instruction
        )
        
        # Initialize extractor adapter
        self.extractor = ExtractorAdapter(
            openai_api_key=config.openai_api_key,
            model=config.extractor_model
        )
        
        # Initialize chatbot adapter with Qdrant
        self.chatbot = ChatbotAdapter(
            openai_api_key=config.openai_api_key,
            qdrant_url=config.qdrant_url,
            collection_name=config.collection_name,
            qdrant_api_key=config.qdrant_api_key,
            embedding_model=config.embedding_model,
            llm_model=config.llm_model,
            top_k=config.top_k,
            default_author=config.default_author,
            default_company=config.default_company,
            num_workers=config.num_workers
        )
        
        # Track processing status
        self._processing_status = {}  # file_path -> status dict
        
        logging.info("Agentic router initialized successfully")
    
    def route(self, query: str, pdf_uploaded: bool = False) -> RouteResult:
        """
        Route a user query to the appropriate pipeline.
        
        Args:
            query: The user's query string
            pdf_uploaded: Whether a PDF has been uploaded
            
        Returns:
            RouteResult with route type and extracted parameters
        """
        return self.query_router.route(query, pdf_uploaded)
    
    def process_pdf(
        self,
        pdf_path: str,
        user_id: str,
        metadata_override: Optional[Dict[str, Dict[str, Any]]] = None,
        log_callback: Optional[Callable[[str, str], None]] = None,
        force_reparse: bool = False
    ) -> Dict[str, Any]:
        """
        Process a PDF file: parse once, then run both pipelines in parallel.
        
        This method:
        1. Parses PDF using shared parser (once)
        2. Runs extractor prefilter (stores filtered page indices)
        3. Starts chatbot ingestion in background (embeddings + vector store)
        
        Args:
            pdf_path: Path to the PDF file
            metadata_override: Optional metadata overrides per file
            log_callback: Optional callback function(log_message, status) for logging
            force_reparse: If True, reparse even if cached
            
        Returns:
            dict: {
                "parsed": bool,
                "extractor_ready": bool,
                "chatbot_processing": bool,
                "filtered_pages_count": int,
                "error": str or None
            }
        """
        status = {
            "parsed": False,
            "extractor_ready": False,
            "chatbot_processing": False,
            "filtered_pages_count": 0,
            "error": None
        }
        self._processing_status[pdf_path] = status
        
        try:
            # Step 1: Parse PDF (once, shared by both pipelines)
            if log_callback:
                log_callback(f"Parsing PDF: {pdf_path}...", "running")
            
            documents = self.parser.parse_pdf(
                pdf_path,
                log_callback=log_callback,
                force_reparse=force_reparse
            )
            
            status["parsed"] = True
            
            if log_callback:
                log_callback(f"PDF parsed: {len(documents)} pages", "success")

            # Step 2: Run extractor prefilter (synchronous, fast)
            if log_callback:
                log_callback("Running extractor prefilter...", "running")
            self.extractor.clear_cache(pdf_path)
            filtered_pages = self.extractor.prefilter_pages(
                documents,
                pdf_path,
                log_callback=log_callback,
                cache=False
            )
            
            status["extractor_ready"] = True
            status["filtered_pages_count"] = len(filtered_pages)
            
            if log_callback:
                log_callback(f"Extractor ready: {len(filtered_pages)} pages with tables", "success")
            
            # Step 3: Start chatbot ingestion in background
            chatbot_metadata = None
            if metadata_override and pdf_path in metadata_override:
                chatbot_metadata = metadata_override[pdf_path]
            
            chatbot_started = self.chatbot.ingest_documents_from_parsed(
                documents,
                pdf_path,
                user_id=user_id,
                metadata_override=chatbot_metadata,
                background=True,
                log_callback=log_callback
            )
            
            status["chatbot_processing"] = chatbot_started
            
            if log_callback:
                if chatbot_started:
                    log_callback("Chatbot ingestion started in background", "running")
                else:
                    log_callback("Chatbot ingestion skipped (already processed or processing)", "info")
            
            return status
            
        except Exception as e:
            error_msg = f"Error processing PDF: {str(e)}"
            logging.error(error_msg)
            status["error"] = error_msg
            if log_callback:
                log_callback(error_msg, "error")
            return status
    
    def get_filtered_pages(self, pdf_path: str) -> Optional[List[Dict[str, Any]]]:
        """
        Get filtered pages for extractor (ready for user selection).
        Rebuilds from parser cache each time (no extractor cache).
        """
        documents = self.parser.get_cached_documents(pdf_path)
        if not documents:
            return None
        return self.extractor.prefilter_pages(
            documents,
            pdf_path,
            log_callback=None,
            cache=False
        )
    
    def extract_tables(
        self,
        pdf_path: str,
        selected_page_indices: List[int],
        log_callback: Optional[Callable[[str, str], None]] = None
    ) -> List[Dict[str, Any]]:
        """
        Extract tables from selected pages.
        
        Args:
            pdf_path: Path to the PDF file
            selected_page_indices: List of page indices to extract
            log_callback: Optional callback function(log_message, status) for logging
            
        Returns:
            List of validated result dictionaries
        """
        # Get filtered pages
        filtered_pages = self.get_filtered_pages(pdf_path)
        if not filtered_pages:
            if log_callback:
                log_callback("No filtered pages available. Please process PDF first.", "error")
            return []
        
        # Get selected pages data
        selected_pages_data = [
            page for page in filtered_pages
            if page["index"] in selected_page_indices
        ]
        
        if not selected_pages_data:
            if log_callback:
                log_callback("No pages selected or pages not found.", "warning")
            return []
        
        # Validate selected pages
        return self.extractor.validate_selected_pages(
            selected_pages_data,
            log_callback=log_callback
        )
    
    def query_chatbot(
        self,
        query: str,
        user_id: str,
        metadata_filters: Optional[Dict[str, Any]] = None,
        auto_detect_filters: bool = True
    ) -> Dict[str, Any]:
        """
        Query the chatbot/RAG pipeline.
        
        Args:
            query: User query string
            user_id: User ID for document isolation
            metadata_filters: Optional manual metadata filters (legacy, not used in new implementation)
            auto_detect_filters: Whether to auto-detect filters from query (legacy, not used)
            
        Returns:
            dict: {
                "answer": str,
                "chunks": List[str],
                "filters_applied": dict or None
            }
        """
        return self.chatbot.query(
            query,
            user_id=user_id,
            metadata_filters=metadata_filters,
            auto_detect_filters=auto_detect_filters
        )
    
    def fetch_edgar_data(
        self,
        ticker: str,
        num_years: int = 3,
        log_callback: Optional[Callable[[str, str], None]] = None
    ) -> Dict[str, Any]:
        """
        Fetch financial data from SEC EDGAR.
        
        Args:
            ticker: Stock ticker symbol (e.g., AAPL, MSFT)
            num_years: Number of years of data to fetch (max 5)
            log_callback: Optional callback function(log_message, status) for logging
            
        Returns:
            dict: Aggregated financial data
        """
        if log_callback:
            log_callback(f"Fetching EDGAR data for {ticker}...", "running")
        
        try:
            scraper = AggregatedFinancialScraper(ticker, max_workers=3)
            result = scraper.run() or {}

            if log_callback:
                if result:
                    log_callback(f"Successfully fetched data for {ticker}", "success")
                else:
                    log_callback(f"No data found for {ticker}", "warning")

            # If we have no statement data, return whatever we have
            balance_sheets = result.get("balance_sheet_data") or []
            income_statements = result.get("income_statement_data") or []
            cash_flows = result.get("cash_flow_data") or []

            if not (balance_sheets or income_statements or cash_flows):
                return result

            # IMPORTANT:
            # Do NOT slice these lists by num_years here.
            # We want to:
            #   1) merge using ALL available filings
            #   2) then, from the merged catalogs, select the latest N years
            #      and expose only those years (in ascending order).

            # Build merger input in the same format used by the test_edgar_format notebook
            merger_input = {
                "ticker": result.get("ticker", ticker),
                "years": {}
            }

            # Helper to attach a statement dict to the merger_input.years map
            def _attach_statement(stmt_list, key_name: str) -> None:
                for stmt in stmt_list:
                    year = str(stmt.get("filing_year", "")).strip()
                    if not year:
                        continue
                    years_map = merger_input["years"]
                    if year not in years_map:
                        years_map[year] = {}
                    # Do not overwrite if another source already set this key for the year
                    if key_name not in years_map[year]:
                        years_map[year][key_name] = stmt

            _attach_statement(balance_sheets, "balance_sheet")
            _attach_statement(income_statements, "income_statement")
            _attach_statement(cash_flows, "cash_flow_statement")

            # Only run merger if we actually have year data
            merged_catalogs = {}
            merged_years: List[str] = []
            if merger_input["years"]:
                merged_catalogs = build_unified_catalog_all_statements(merger_input) or {}

                # ------------------------------------------------------------------
                # IMPORTANT: mirror notebook behaviour (create_dataframe_from_unified_catalog)
                # ------------------------------------------------------------------
                # In the test notebook we:
                #   - collect ALL years from the unified catalog "values" dicts
                #   - sort them in ascending order
                #   - keep only the first `num_years` entries
                #   - and then build the tables using ONLY those years.
                #
                # To ensure the API returns the same N-year view per line item, we
                # apply the same logic here and trim each payload's "values" dict
                # down to those selected years.
                all_years: set[str] = set()
                for stmt_type in ("income_statement", "balance_sheet", "cash_flow_statement"):
                    catalog = merged_catalogs.get(stmt_type) or {}
                    for item_data in catalog.values():
                        values_dict = item_data.get("values") or {}
                        all_years.update(values_dict.keys())

                if all_years:
                    # Ascending order (oldest -> newest)
                    sorted_years = sorted(all_years)
                    if num_years is not None and num_years > 0:
                        # Keep the LATEST `num_years` (highest years), but
                        # still in ascending order, to mirror notebook behaviour:
                        # e.g. available: [2018,2019,2020,2021,2022], num_years=3
                        # -> keep [2020,2021,2022]
                        if len(sorted_years) > num_years:
                            sorted_years = sorted_years[-num_years:]

                    years_to_keep = set(sorted_years)

                    # Trim each line item's values to just the selected years
                    for stmt_type in ("income_statement", "balance_sheet", "cash_flow_statement"):
                        catalog = merged_catalogs.get(stmt_type) or {}
                        for item_data in catalog.values():
                            values_dict = item_data.get("values") or {}
                            if values_dict:
                                item_data["values"] = {
                                    year: value
                                    for year, value in values_dict.items()
                                    if year in years_to_keep
                                }

                    # These are the years the merged view actually exposes
                    merged_years = sorted_years

            # Attach merged view alongside the existing raw data so existing
            # callers are not broken but the EDGAR endpoint can use the unified view.
            result["merged_edgar"] = merged_catalogs
            result["merged_years"] = merged_years
            result["requested_years"] = num_years

            return result

        except Exception as e:
            error_msg = f"Error fetching EDGAR data: {str(e)}"
            logging.error(error_msg)
            if log_callback:
                log_callback(error_msg, "error")
            return {"error": error_msg}
    
    def get_chatbot_metadata_values(self, field_name: str) -> List[str]:
        """
        Get distinct metadata values from chatbot index.
        
        Args:
            field_name: Name of metadata field (e.g., 'company', 'author')
            
        Returns:
            List of distinct values
        """
        return self.chatbot.get_metadata_values(field_name)
    
    def get_processing_status(self, pdf_path: str) -> Optional[Dict[str, Any]]:
        """
        Get processing status for a PDF file.
        
        Args:
            pdf_path: Path to the PDF file
            
        Returns:
            Status dictionary or None if not found
        """
        return self._processing_status.get(pdf_path)
    
    def is_chatbot_ready(self, pdf_path: str) -> bool:
        """
        Check if chatbot is ready for a PDF file.
        
        Args:
            pdf_path: Path to the PDF file
            
        Returns:
            bool: True if chatbot has finished processing
        """
        return self.chatbot.is_processed(pdf_path)
    
    def is_chatbot_processing(self, pdf_path: str) -> bool:
        """
        Check if chatbot is currently processing a PDF file.
        
        Args:
            pdf_path: Path to the PDF file
            
        Returns:
            bool: True if chatbot is processing
        """
        return self.chatbot.is_processing(pdf_path)
    
    def has_chatbot_error(self, pdf_path: str) -> bool:
        """
        Check if chatbot has encountered an error for a PDF file.
        
        Args:
            pdf_path: Path to the PDF file
            
        Returns:
            bool: True if there was an error
        """
        return self.chatbot.has_error(pdf_path)
    
    def get_chatbot_error(self, pdf_path: str) -> Optional[str]:
        """
        Get the chatbot error message for a PDF file.
        
        Args:
            pdf_path: Path to the PDF file
            
        Returns:
            str or None: Error message if any
        """
        return self.chatbot.get_error(pdf_path)
    
    def clear_cache(self, pdf_path: Optional[str] = None):
        """
        Clear cached data.
        
        Args:
            pdf_path: If provided, clear only this file's cache.
                     If None, clear all cache.
        """
        self.parser.clear_cache(pdf_path)
        self.extractor.clear_cache(pdf_path)
        if pdf_path:
            self._processing_status.pop(pdf_path, None)
        else:
            self._processing_status.clear()
        logging.info(f"Cleared cache for {pdf_path if pdf_path else 'all files'}")
    
    def get_info(self) -> Dict[str, Any]:
        """Get router information"""
        return {
            "pipeline_type": "agentic_router",
            "extractor_ready": True,
            "chatbot_ready": self.chatbot.is_ready(),
            "chatbot_collection": self.config.collection_name,
            "qdrant_url": self.config.qdrant_url,
            "embedding_model": self.config.embedding_model,
            "llm_model": self.config.llm_model,
            "extractor_model": self.config.extractor_model,
        }
    
    def close(self) -> None:
        """
        Close all connections and clean up resources.
        Call this when done using the router to prevent resource leaks.
        """
        if self.chatbot:
            self.chatbot.close()
        logging.info("AgenticRouter closed")
    
    def __enter__(self):
        """Context manager entry"""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - clean up resources"""
        self.close()
        return False
