"""
Intelligent Query Router Module

Routes user queries to the appropriate pipeline feature:
- Public company data (EDGAR)
- PDF table extraction
- Chatbot Q&A
"""

from .query_router import QueryRouter, RouteResult, RouteType

__all__ = ['QueryRouter', 'RouteResult', 'RouteType']

