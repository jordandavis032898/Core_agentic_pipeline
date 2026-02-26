"""
Configuration for Agentic Router
"""
from dataclasses import dataclass
from pathlib import Path
from typing import Optional
import os

# Try to load .env file if python-dotenv is available (respect ENV_FILE e.g. .env.prod)
try:
    from dotenv import load_dotenv
    _env_file = os.getenv("ENV_FILE", ".env")
    _env_path = Path(__file__).resolve().parent / _env_file
    if _env_path.exists():
        load_dotenv(_env_path)
    else:
        load_dotenv()
except ImportError:
    pass  # dotenv not installed, will use environment variables directly


@dataclass
class RouterConfig:
    """Configuration for Agentic Router"""
    
    # API Keys
    openai_api_key: str
    llama_cloud_api_key: str
    
    # Qdrant Configuration (for chatbot)
    qdrant_url: str
    qdrant_api_key: Optional[str] = None
    collection_name: str = "policy_documents"
    
    # Model Configuration
    embedding_model: str = "text-embedding-3-small"
    llm_model: str = "gpt-4o"
    extractor_model: str = "gpt-4o-mini"  # Model for table extraction
    
    # Retrieval Configuration
    top_k: int = 5
    
    # Default Metadata (for chatbot)
    default_author: str = "unknown"
    default_company: str = "unknown"
    
    # Parser Configuration
    use_multimodal: bool = True
    parsing_instruction: Optional[str] = None
    
    # Node Parser Configuration (for new RAG)
    num_workers: int = 8  # Number of workers for MarkdownElementNodeParser
    
    # Reranker Configuration (for new RAG)
    reranker_model: str = "BAAI/bge-reranker-large"
    reranker_top_n: int = 5
    
    @classmethod
    def from_env(cls) -> 'RouterConfig':
        """Create config from environment variables (supports .env file)"""
        return cls(
            openai_api_key=os.getenv("OPENAI_API_KEY", ""),
            llama_cloud_api_key=os.getenv("LLAMA_CLOUD_API_KEY", ""),
            qdrant_url=os.getenv("QDRANT_URL", "http://localhost:6333"),
            qdrant_api_key=os.getenv("QDRANT_API_KEY"),
            collection_name=os.getenv("COLLECTION_NAME", "policy_documents"),
            embedding_model=os.getenv("EMBEDDING_MODEL", "text-embedding-3-small"),
            llm_model=os.getenv("LLM_MODEL", "gpt-4o"),
            extractor_model=os.getenv("EXTRACTOR_MODEL", "gpt-4o-mini"),
            top_k=int(os.getenv("TOP_K", "5")),
            default_author=os.getenv("DEFAULT_AUTHOR", "unknown"),
            default_company=os.getenv("DEFAULT_COMPANY", "unknown"),
            use_multimodal=os.getenv("USE_MULTIMODAL", "true").lower() == "true",
            parsing_instruction=os.getenv("PARSING_INSTRUCTION") or None,
            num_workers=int(os.getenv("NUM_WORKERS", "8")),
            reranker_model=os.getenv("RERANKER_MODEL", "BAAI/bge-reranker-large"),
            reranker_top_n=int(os.getenv("RERANKER_TOP_N", "5")),
        )
    
    def to_dict(self) -> dict:
        """Convert config to dictionary"""
        return {
            "openai_api_key": self.openai_api_key,
            "llama_cloud_api_key": self.llama_cloud_api_key,
            "qdrant_url": self.qdrant_url,
            "qdrant_api_key": self.qdrant_api_key,
            "collection_name": self.collection_name,
            "embedding_model": self.embedding_model,
            "llm_model": self.llm_model,
            "extractor_model": self.extractor_model,
            "top_k": self.top_k,
            "default_author": self.default_author,
            "default_company": self.default_company,
            "use_multimodal": self.use_multimodal,
            "parsing_instruction": self.parsing_instruction,
            "num_workers": self.num_workers,
            "reranker_model": self.reranker_model,
            "reranker_top_n": self.reranker_top_n,
        }
