"""
Extractor Adapter — real LLM-based table extraction.

Uses GPT-4o-mini to extract structured table data from parsed PDF pages.
"""
import os
import re
import asyncio
import logging
import concurrent.futures
from typing import List, Dict, Any, Optional, Callable

from .prefilter import prefilter_statement_page_from_rmd
from .validator import (
    LLMOnlyFinancialTableValidatorV2,
    OpenAILLM,
    run_validator_on_pages_llm_v2,
)


def run_async_in_thread(coro):
    """
    Run an async coroutine from a sync context, even if an event loop is already running.
    Uses a thread pool to create a new event loop.
    """
    def run_in_new_loop():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            return loop.run_until_complete(coro)
        finally:
            loop.close()

    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(run_in_new_loop)
        return future.result()


class ExtractorAdapter:
    """
    Adapter for extractor pipeline that uses shared parsed documents.
    """

    def __init__(
        self,
        openai_api_key: str,
        model: str = "gpt-4o-mini"
    ):
        self.openai_api_key = openai_api_key
        self.model = model
        self._filtered_pages = {}  # Cache: normalized file_path -> filtered_pages
        self._validator = None

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
        """
        if log_callback:
            log_callback("Filtering pages with prefilter...", "running")

        logging.info(f"Prefiltering for {file_path} (cache={cache})")
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
            log_callback(f"Found {len(filtered_pages)} pages with tables.", "success")

        logging.info(f"Prefilter selected {len(filtered_pages)} pages")

        return filtered_pages

    def get_filtered_pages(self, file_path: str) -> Optional[List[Dict[str, Any]]]:
        """Get cached filtered pages if available."""
        return self._filtered_pages.get(self._cache_key(file_path))

    def validate_selected_pages(
        self,
        selected_pages_data: List[Dict[str, Any]],
        log_callback: Optional[Callable[[str, str], None]] = None
    ) -> List[Dict[str, Any]]:
        """
        Extract structured table data from selected pages using LLM.
        """
        if log_callback:
            log_callback(
                f"Extracting tables from {len(selected_pages_data)} pages with GPT...",
                "running"
            )

        # Initialize validator if not already done
        if self._validator is None:
            self._validator = LLMOnlyFinancialTableValidatorV2(
                OpenAILLM(api_key=self.openai_api_key),
                model=self.model
            )

        # Convert to format expected by validator
        validator_input = [
            {"page_content": page["page_content"]} for page in selected_pages_data
        ]

        # Run validator
        dict_results = run_async_in_thread(
            run_validator_on_pages_llm_v2(self._validator, validator_input, max_concurrency=3)
        )

        # Combine results with page metadata
        validated_results = []
        for idx, result in enumerate(dict_results):
            page_data = selected_pages_data[idx]
            explanation = None
            if hasattr(result, "explanation") and result.explanation:
                explanation = result.explanation

            validated_results.append({
                "page_number": page_data["page_number"],
                "page_index": page_data["index"],
                "metadata": page_data["metadata"],
                "validation_result": result,
                "data": result.data if result.data else None,
                "table_metadata": result.metadata if hasattr(result, "metadata") and result.metadata else None,
                "explanation": explanation,
                "error": result.error if result.error else None
            })

        if log_callback:
            success_count = len([r for r in validated_results if r['data']])
            log_callback(
                f"Extraction complete. {success_count} pages extracted successfully.",
                "success"
            )

        return validated_results

    def clear_cache(self, file_path: Optional[str] = None):
        """Clear cached filtered pages."""
        if file_path:
            self._filtered_pages.pop(self._cache_key(file_path), None)
            logging.info(f"Cleared extractor cache for {file_path}")
        else:
            self._filtered_pages.clear()
            logging.info("Cleared all extractor cached pages")
