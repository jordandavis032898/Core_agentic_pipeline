"""
LLM-based Query Router

Routes user queries to the appropriate pipeline feature using OpenAI.
"""

import os
import logging
from typing import Optional, Dict, Any, List
from dataclasses import dataclass
from enum import Enum

try:
    import openai
    from pydantic import BaseModel, Field
    OPENAI_AVAILABLE = True
except ImportError:
    OPENAI_AVAILABLE = False


class RouteType(str, Enum):
    """Types of routes available"""
    PUBLIC_DATA = "public_data"           # EDGAR financial data
    TABLE_EXTRACTION = "table_extraction"  # PDF table extraction
    CHATBOT = "chatbot"                    # PDF Q&A chatbot
    UPLOAD_PDF = "upload_pdf"              # User needs to upload PDF first
    UNCLEAR = "unclear"                    # Query is unclear


@dataclass
class RouteResult:
    """Result of query routing"""
    route: RouteType
    confidence: float
    explanation: str
    extracted_params: Dict[str, Any]
    original_query: str
    
    def requires_pdf(self) -> bool:
        """Check if this route requires a PDF to be uploaded"""
        return self.route in [RouteType.TABLE_EXTRACTION, RouteType.CHATBOT]


class QueryClassificationSchema(BaseModel):
    """Schema for LLM query classification"""
    route: str = Field(
        description="The type of query: 'public_data' for public company financial data/SEC filings, "
                    "'table_extraction' for extracting tables from uploaded PDF, "
                    "'chatbot' for asking questions about uploaded PDF content, "
                    "'unclear' if the query is ambiguous"
    )
    confidence: float = Field(
        ge=0.0, le=1.0,
        description="Confidence score from 0.0 to 1.0"
    )
    explanation: str = Field(
        description="Brief explanation of why this route was chosen"
    )
    ticker: Optional[str] = Field(
        default=None,
        description="Stock ticker if public_data route (e.g., AAPL, MSFT)"
    )
    num_years: Optional[int] = Field(
        default=None,
        description="Number of years of data if public_data route"
    )
    table_pages: Optional[List[int]] = Field(
        default=None,
        description="Specific page numbers if table_extraction route"
    )


class QueryRouter:
    """
    Intelligent query router using LLM to classify and route user queries.
    
    Features:
    - Routes to EDGAR for public company financial data
    - Routes to table extraction for PDF table requests
    - Routes to chatbot for PDF Q&A
    - Handles cases where PDF upload is required
    """
    
    SYSTEM_PROMPT = """You are an intelligent query router for a financial data platform. Your job is to classify user queries and route them to the appropriate feature.

Available Features:
1. **public_data**: For queries about PUBLIC company financial data from SEC filings (10-K reports).
   - Triggers: Company names (Apple, Microsoft, Tesla), tickers (AAPL, MSFT), "public", "SEC", "10-K", "financial statements", "balance sheet", "income statement", "cash flow" for known companies.
   - Examples: "Get Apple's financials", "Show me MSFT balance sheet", "Tesla income statement for 3 years"

2. **table_extraction**: For extracting tables from an UPLOADED PDF document.
   - Triggers: "extract tables", "get tables", "show tables", "table data", "extract financial tables from PDF"
   - Examples: "Extract tables from the document", "Show me the tables", "Get table data from page 2"

3. **chatbot**: For asking questions about the content of an UPLOADED PDF document.
   - Triggers: Questions about document content, "what does the document say", "summarize", "explain"
   - Examples: "What is this document about?", "Summarize the main points", "What are the key findings?"

Classification Rules:
- If the query mentions a well-known PUBLIC company (Apple, Microsoft, Google, Tesla, Amazon, Meta, etc.) or stock ticker, route to 'public_data'
- If the query asks about extracting/showing tables from a document, route to 'table_extraction'
- If the query asks questions about document content or wants to chat about the document, route to 'chatbot'
- If the query is ambiguous, use 'unclear'

IMPORTANT: 
- Public company queries (public_data) do NOT require a PDF upload
- Table extraction and chatbot REQUIRE a PDF to be uploaded first"""

    def __init__(self, openai_api_key: Optional[str] = None):
        """
        Initialize the query router.
        
        Args:
            openai_api_key: OpenAI API key. If not provided, uses environment variable.
        """
        if not OPENAI_AVAILABLE:
            raise ImportError("OpenAI and Pydantic are required. Install with: pip install openai pydantic")
        
        self.api_key = openai_api_key or os.getenv("OPENAI_API_KEY")
        if not self.api_key:
            raise ValueError("OpenAI API key is required")
        
        self.client = openai.OpenAI(api_key=self.api_key)
        logging.info("QueryRouter initialized")
    
    def route(self, query: str, pdf_uploaded: bool = False) -> RouteResult:
        """
        Route a user query to the appropriate feature.
        
        Args:
            query: The user's query string
            pdf_uploaded: Whether a PDF has been uploaded
            
        Returns:
            RouteResult with route type and extracted parameters
        """
        logging.info(f"Routing query: '{query}' (pdf_uploaded={pdf_uploaded})")
        
        try:
            # Call OpenAI to classify the query
            response = self.client.beta.chat.completions.parse(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": self.SYSTEM_PROMPT},
                    {"role": "user", "content": f"Classify this query: \"{query}\""}
                ],
                response_format=QueryClassificationSchema
            )
            
            parsed = response.choices[0].message.parsed
            
            # Convert string route to enum
            route_map = {
                "public_data": RouteType.PUBLIC_DATA,
                "table_extraction": RouteType.TABLE_EXTRACTION,
                "chatbot": RouteType.CHATBOT,
                "unclear": RouteType.UNCLEAR
            }
            route_type = route_map.get(parsed.route, RouteType.UNCLEAR)
            
            # Check if PDF is required but not uploaded
            if route_type in [RouteType.TABLE_EXTRACTION, RouteType.CHATBOT] and not pdf_uploaded:
                route_type = RouteType.UPLOAD_PDF
                parsed.explanation = f"This query requires a PDF to be uploaded first. Original intent: {parsed.route}"
            
            # Extract parameters
            extracted_params = {}
            if parsed.ticker:
                extracted_params["ticker"] = parsed.ticker
            if parsed.num_years:
                extracted_params["num_years"] = parsed.num_years
            if parsed.table_pages:
                extracted_params["table_pages"] = parsed.table_pages
            
            result = RouteResult(
                route=route_type,
                confidence=parsed.confidence,
                explanation=parsed.explanation,
                extracted_params=extracted_params,
                original_query=query
            )
            
            logging.info(f"Route result: {result.route.value} (confidence={result.confidence:.2f})")
            return result
            
        except Exception as e:
            logging.error(f"Error routing query: {e}")
            # Return unclear route on error
            return RouteResult(
                route=RouteType.UNCLEAR,
                confidence=0.0,
                explanation=f"Error during routing: {str(e)}",
                extracted_params={},
                original_query=query
            )
    
    def get_route_description(self, route: RouteType) -> str:
        """Get a human-readable description of a route"""
        descriptions = {
            RouteType.PUBLIC_DATA: "Fetching public company financial data from SEC EDGAR",
            RouteType.TABLE_EXTRACTION: "Extracting tables from uploaded PDF",
            RouteType.CHATBOT: "Answering questions about uploaded PDF",
            RouteType.UPLOAD_PDF: "Please upload a PDF first",
            RouteType.UNCLEAR: "Could not understand the query"
        }
        return descriptions.get(route, "Unknown route")

