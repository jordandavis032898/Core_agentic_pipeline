"""
RAG Helper Functions
Imports and adapts helper functions from pipeline_v1_final for use in chatbot adapter.
"""
import sys
import os
from pathlib import Path

# Add pipeline_v1_final to path to import helper functions
_pipeline_v1_path = Path(__file__).parent.parent / "pipeline_v1_final"
if str(_pipeline_v1_path) not in sys.path:
    sys.path.insert(0, str(_pipeline_v1_path))

# Import helper functions from pipeline_v1_final
from helper import (
    parse_pdfs_parallel,
    generate_document_description,
    MarkdownElementNodeParser,
    get_query_engine,
    create_dynamic_tool_mappings,
    generate_pdf_paths_with_doc_ids,
    get_qdrant_client,
    TableOutput,
    TableColumnOutput,
)

__all__ = [
    "parse_pdfs_parallel",
    "generate_document_description",
    "MarkdownElementNodeParser",
    "get_query_engine",
    "create_dynamic_tool_mappings",
    "generate_pdf_paths_with_doc_ids",
    "get_qdrant_client",
    "TableOutput",
    "TableColumnOutput",
]
