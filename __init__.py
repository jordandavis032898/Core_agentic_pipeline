"""
Agentic Router - Intelligent Query Routing & Financial Document Processing Pipeline

This package provides:
1. LLM-powered query routing to appropriate pipelines
2. PDF table extraction with financial statement validation
3. RAG chatbot for PDF Q&A
4. EDGAR SEC filing scraper for public company data

Usage:
    from agentic_router import AgenticRouter, RouterConfig
    
    config = RouterConfig.from_env()
    router = AgenticRouter(config)
    
    # Route a query
    result = router.route("Get Apple's financials for 3 years", pdf_uploaded=False)
    
    # Process based on route
    if result.route == RouteType.PUBLIC_DATA:
        data = router.fetch_edgar_data(result.extracted_params)
    elif result.route == RouteType.TABLE_EXTRACTION:
        tables = router.extract_tables(pdf_path, page_indices)
    elif result.route == RouteType.CHATBOT:
        answer = router.query(query)
"""

from .config import RouterConfig
from .pipeline import AgenticRouter
from .router import QueryRouter, RouteResult
from .router.query_router import RouteType

__all__ = [
    'AgenticRouter',
    'RouterConfig', 
    'QueryRouter',
    'RouteResult',
    'RouteType'
]

__version__ = "1.0.0"

