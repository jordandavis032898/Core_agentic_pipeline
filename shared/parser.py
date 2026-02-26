"""
Shared PDF Parser Module

This module provides unified PDF parsing using LlamaParse that can be used
by both the extractor and chatbot pipelines.
"""
import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Optional, Callable, Any, Dict
from llama_index.core import SimpleDirectoryReader
from llama_parse import LlamaParse


_DEFAULT_PARSING_INSTRUCTION = (
"ABSOLUTE RULE — One chunk per physical PDF page, no exceptions. The total number of output "
"chunks must equal exactly the total number of physical pages in the PDF. Never produce more "
"chunks than there are pages. Never split one page into multiple chunks. Never merge two pages "
"into one chunk. "
"\n\n"
"ABSOLUTE RULE — At the very start of each chunk's output, on its own line, write exactly: "
"##PAGE:<N>## where N is the 1-based page number of that page in the PDF document. "
"Never omit this tag. For blank pages or pages with only headers/footers, output only "
"##PAGE:<N>## followed by [BLANK PAGE]. "
"\n\n"
"ABSOLUTE RULE — Zero-tolerance page boundary enforcement: Every word, number, symbol, "
"table cell, footer, page number, horizontal rule, caption, footnote, or whitespace character "
"must be assigned to the page it physically appears on — determined by its pixel/coordinate "
"position in the PDF — and must never appear in any other page's chunk. "
"Footers, page numbers (e.g. 'Page X of Y'), and horizontal rules (----) that appear at the "
"bottom of page N belong exclusively to page N and must NEVER appear at the beginning or "
"anywhere in page N+1's chunk. "
"If a table, paragraph, or section begins on page N and continues onto page N+1, split it "
"precisely at the physical page boundary: everything above the boundary goes into chunk N, "
"everything below the boundary goes into chunk N+1. Do not wait until a 'logical' break. "
"\n\n"
"CRITICAL — On the line immediately after ##PAGE:<N>##, add 'tables: TRUE' if the page "
"contains any tables, and 'graphs: TRUE' if the page contains any graphs, charts, or images. "
"Use both if the page has both; omit if the page has neither. "
"\n\n"
"CRITICAL — Preserve all structure and formatting: Keep section headings, table titles, and "
"captions immediately above their tables. Keep ALL footnotes, annotations, and endnotes "
"(e.g. [1], [2]) immediately after the table they reference on the same page — never move "
"them to a different chunk. For every table, preserve column headers and row labels exactly "
"as they appear. Preserve all numerical values, percentages, and units without modification. "
"For every block derived from a true table, prepend '[SOURCE:TABLE]'. For content derived "
"from graphs, charts, or images, prepend '[SOURCE:GRAPH]'. "
"\n\n"
"SELF-CHECK — Before finalizing output, verify: (1) chunk count == total PDF page count, "
"(2) every chunk starts with ##PAGE:<N>## in correct sequence, (3) no content from page N "
"appears in chunk N+1 or any other chunk, (4) no chunk is missing or duplicated."
)


class SharedParser:
    """
    Shared PDF parser that uses LlamaParse once and provides
    parsed documents to both pipelines.
    """
    
    def __init__(
        self,
        llama_api_key: str,
        use_multimodal: bool = True,
        parsing_instruction: Optional[str] = None
    ):
        """
        Initialize shared parser.
        
        Args:
            llama_api_key: LlamaParse API key
            use_multimodal: Whether to use multimodal model (default: True)
            parsing_instruction: Optional parsing instruction for LlamaParse
        """
        self.llama_api_key = llama_api_key
        self.use_multimodal = use_multimodal
        self.parsing_instruction = parsing_instruction
        self._parsed_documents = {}  # Cache: file_path -> documents
    
    def parse_pdf(
        self,
        pdf_path: str,
        log_callback: Optional[Callable[[str, str], None]] = None,
        force_reparse: bool = False
    ) -> List[Any]:
        """
        Parse PDF using LlamaParse and return documents.
        
        This method caches parsed documents to avoid re-parsing.
        
        Args:
            pdf_path: Path to the PDF file
            log_callback: Optional callback function(log_message, status) for logging
            force_reparse: If True, reparse even if cached
            
        Returns:
            List of document objects from LlamaParse (one per page)
        """
        # Check cache
        if not force_reparse and pdf_path in self._parsed_documents:
            if log_callback:
                log_callback(f"Using cached parsed documents for {pdf_path}", "info")
            return self._parsed_documents[pdf_path]
        
        if log_callback:
            log_callback("Parsing PDF with LlamaParse...", "running")
        
        logging.info(f"Parsing PDF file: {pdf_path}")
        
        # Create parser with premium mode (self-sufficient, includes multimodal support)
        # Note: premium_mode is incompatible with use_vendor_multimodal_model
        # Premium mode already includes advanced parsing capabilities.
        # The parsing instruction preserves footnotes co-located with tables
        # and section headings as context — improving embedding quality.
        parser = LlamaParse(
            api_key=self.llama_api_key,
            result_type="markdown",
            premium_mode=True,
            split_by_page=True,  # Premium mode includes multimodal and advanced parsing
            adaptive_long_table=True,  # Enable adaptive long table parsing
            system_prompt_append=self.parsing_instruction or _DEFAULT_PARSING_INSTRUCTION,
        )
        
        # LlamaParse.load_data() is async, but we need to call it from sync context.
        # When running under uvicorn (uvloop), nest_asyncio doesn't work ("Can't patch loop of type uvloop.Loop").
        # So we run the async parser in a dedicated thread with asyncio.run() instead of patching the loop.
        def _run_parser_in_thread():
            return asyncio.run(parser.aload_data(pdf_path))

        try:
            loop = asyncio.get_running_loop()
            # We're in an async context (e.g. FastAPI/uvicorn). Run parser in a thread so we don't
            # need nest_asyncio (which fails with uvloop). In the thread there's no running loop.
            with ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(_run_parser_in_thread)
                documents = future.result()
        except RuntimeError:
            # No running loop, safe to use asyncio.run() directly
            documents = asyncio.run(parser.aload_data(pdf_path))
        
        # Cache the results
        self._parsed_documents[pdf_path] = documents
        
        if log_callback:
            log_callback(f"PDF parsing complete. Found {len(documents)} pages.", "success")
        
        logging.info(f"Successfully parsed PDF: {len(documents)} pages")
        
        return documents
    
    def get_cached_documents(self, pdf_path: str) -> Optional[List[Any]]:
        """
        Get cached parsed documents if available.
        
        Args:
            pdf_path: Path to the PDF file
            
        Returns:
            List of documents if cached, None otherwise
        """
        return self._parsed_documents.get(pdf_path)
    
    def parse_pdfs_parallel(
        self,
        pdf_paths: List[str],
        log_callback: Optional[Callable[[str, str], None]] = None,
        force_reparse: bool = False
    ) -> Dict[str, List[Any]]:
        """
        Parse multiple PDFs in parallel using LlamaParse.
        
        This method uses parallel processing to parse multiple PDFs simultaneously,
        which is much faster than sequential parsing.
        
        Args:
            pdf_paths: List of PDF file paths to parse
            log_callback: Optional callback function(log_message, status) for logging
            force_reparse: If True, reparse even if cached
            
        Returns:
            dict: Maps PDF path to parsed documents list
        """
        # Filter out already cached PDFs if not forcing reparse
        pdfs_to_parse = []
        if not force_reparse:
            for pdf_path in pdf_paths:
                if pdf_path not in self._parsed_documents:
                    pdfs_to_parse.append(pdf_path)
        else:
            pdfs_to_parse = pdf_paths
        
        if not pdfs_to_parse:
            # All PDFs are cached
            return {path: self._parsed_documents[path] for path in pdf_paths}
        
        if log_callback:
            log_callback(f"Parsing {len(pdfs_to_parse)} PDFs in parallel...", "running")
        
        parsed_docs_map = {}
        
        def parse_single(pdf_path):
            """Parse a single PDF with a new parser instance and event loop per thread."""
            loop = None
            old_loop = None
            try:
                try:
                    old_loop = asyncio.get_event_loop()
                except RuntimeError:
                    old_loop = None
                
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                
                parser = LlamaParse(
                    api_key=self.llama_api_key,
                    result_type="markdown",
                    premium_mode=True,
                    adaptive_long_table=True,
                    system_prompt_append=self.parsing_instruction or _DEFAULT_PARSING_INSTRUCTION,
                )
                
                docs = parser.load_data(pdf_path)
                return (pdf_path, docs)
            except Exception as e:
                logging.error(f"Error parsing {pdf_path}: {e}")
                import traceback
                logging.error(traceback.format_exc())
                return (pdf_path, None)
            finally:
                if loop is not None:
                    try:
                        pending = [t for t in asyncio.all_tasks(loop) if not t.done()]
                        for task in pending:
                            task.cancel()
                        if pending:
                            loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
                    except Exception:
                        pass
                    finally:
                        loop.close()
                
                if old_loop is not None:
                    try:
                        asyncio.set_event_loop(old_loop)
                    except Exception:
                        pass
                else:
                    try:
                        asyncio.set_event_loop(None)
                    except Exception:
                        pass
        
        with ThreadPoolExecutor(max_workers=3) as executor:
            futures = {executor.submit(parse_single, path): path for path in pdfs_to_parse}
            
            for future in as_completed(futures):
                pdf_path, docs = future.result()
                if docs is not None:
                    parsed_docs_map[pdf_path] = docs
                    self._parsed_documents[pdf_path] = docs  # Cache the results
                    if log_callback:
                        log_callback(f"✓ Parsed {pdf_path}: {len(docs)} pages", "success")
                    logging.info(f"✓ Parsed {pdf_path}: {len(docs)} documents")
                else:
                    if log_callback:
                        log_callback(f"✗ Failed to parse {pdf_path}", "error")
                    logging.error(f"✗ Failed to parse {pdf_path}")
        
        # Add cached PDFs to the result
        for pdf_path in pdf_paths:
            if pdf_path not in parsed_docs_map and pdf_path in self._parsed_documents:
                parsed_docs_map[pdf_path] = self._parsed_documents[pdf_path]
        
        return parsed_docs_map
    
    def clear_cache(self, pdf_path: Optional[str] = None):
        """
        Clear cached documents.
        
        Args:
            pdf_path: If provided, clear only this file's cache.
                     If None, clear all cache.
        """
        if pdf_path:
            self._parsed_documents.pop(pdf_path, None)
            logging.info(f"Cleared cache for {pdf_path}")
        else:
            self._parsed_documents.clear()
            logging.info("Cleared all cached documents")

