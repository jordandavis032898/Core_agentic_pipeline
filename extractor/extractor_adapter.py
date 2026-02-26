"""
Simplified Extractor Adapter.

This version keeps the public interface but uses trivial logic
for prefiltering and validation. It is not intended to be
feature-complete or production-ready.
"""
import os
import re
import logging
from typing import List, Dict, Any, Optional, Callable

from .prefilter import prefilter_statement_page_from_rmd


class ExtractorAdapter:
    """
    Adapter for extractor pipeline that uses shared parsed documents.
    """
    
    def __init__(
        self,
        openai_api_key: str,
        model: str = "gpt-4o-mini"
    ):
        """
        Initialize extractor adapter.
        
        Args:
            openai_api_key: OpenAI API key for validation
            model: OpenAI model to use (default: gpt-4o-mini)
        """
        self.openai_api_key = openai_api_key
        self.model = model
        self._filtered_pages = {}  # Cache: normalized file_path -> filtered_pages

    @staticmethod
    def _cache_key(file_path: str) -> str:
        """Normalize path so cache hit is consistent from API and pipeline."""
        return os.path.normpath(os.path.abspath(file_path))

    def prefilter_pages(
        self,
        documents: List[Any],
        file_path: str,
        log_callback: Optional[Callable[[str, str], None]] = None,
        cache: bool = False
    ) -> List[Dict[str, Any]]:
        """
        Filter pages that contain tables using prefilter logic.
        
        Design: List index is the source of truth for all operations (extraction,
        selection, which cards to show). For each document we attach a page number
        from ##PAGE:n## (or 0 if missing/blank). Page number is used only for
        UI display and for choosing which PDF page to render in preview.
        
        Args:
            documents: List of parsed document objects (from shared parser)
            file_path: Path to the PDF file (for caching when cache=True)
            log_callback: Optional callback function(log_message, status) for logging
            cache: If True, read/write _filtered_pages cache. If False, always recompute (no cache).
            
        Returns:
            List of filtered page dictionaries with metadata
        """
        if log_callback:
            log_callback("Filtering pages (simplified prefilter)...", "running")

        logging.info(f"Simplified prefiltering for {file_path} (cache={cache})")
        key = self._cache_key(file_path)
        if cache and key in self._filtered_pages:
            if log_callback:
                log_callback(f"Using cached filtered pages for {file_path}", "info")
            return self._filtered_pages[key]
        
        def _page_number_from_text(text: str) -> int:
            m = re.search(r"##PAGE:(\d+)##", text or "")
            return int(m.group(1)) if m else 0

        filtered_pages: List[Dict[str, Any]] = []
        for idx, doc in enumerate(documents or []):
            text = getattr(doc, "text", None) or (
                doc.get_content() if hasattr(doc, "get_content") else ""
            ) or ""
            page_number = _page_number_from_text(text)
            result = prefilter_statement_page_from_rmd(text)
            if result.get("pass", False):
                filtered_pages.append(
                    {
                        "index": idx,
                        "display_page": page_number,
                        "page_number": page_number,
                        "page_content": text,
                        "metadata": getattr(doc, "metadata", {}) or {},
                        "filter_result": result,
                    }
                )
        
        if cache:
            self._filtered_pages[key] = filtered_pages

        if log_callback:
            log_callback(f"Found {len(filtered_pages)} pages (simplified).", "success")

        logging.info(f"Simplified prefilter selected {len(filtered_pages)} pages")

        return filtered_pages
    
    def get_filtered_pages(self, file_path: str) -> Optional[List[Dict[str, Any]]]:
        """
        Get cached filtered pages if available.
        
        Args:
            file_path: Path to the PDF file
            
        Returns:
            List of filtered pages if cached, None otherwise
        """
        return self._filtered_pages.get(self._cache_key(file_path))
    
    def validate_selected_pages(
        self,
        selected_pages_data: List[Dict[str, Any]],
        log_callback: Optional[Callable[[str, str], None]] = None
    ) -> List[Dict[str, Any]]:
        """
        Trivial non-LLM "validation" that simply echoes page metadata.

        No external validator or schema enforcement is used here.
        """
        if log_callback:
            log_callback(
                f"Validation is disabled; echoing {len(selected_pages_data)} pages.",
                "info",
            )

        validated_results: List[Dict[str, Any]] = []
        for page_data in selected_pages_data or []:
            validated_results.append(
                {
                    "page_number": page_data.get("page_number"),
                    "page_index": page_data.get("index"),
                    "metadata": page_data.get("metadata", {}),
                    "validation_result": None,
                    "data": None,
                    "table_metadata": None,
                    "explanation": None,
                    "error": "validation_disabled",
                }
            )

        return validated_results
    
    def clear_cache(self, file_path: Optional[str] = None):
        """
        Clear cached filtered pages.
        
        Args:
            file_path: If provided, clear only this file's cache.
                     If None, clear all cache.
        """
        if file_path:
            self._filtered_pages.pop(self._cache_key(file_path), None)
            logging.info(f"Cleared extractor cache for {file_path}")
        else:
            self._filtered_pages.clear()
            logging.info("Cleared all extractor cached pages")

