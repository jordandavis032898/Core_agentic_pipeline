"""
Helper functions for the complete RAG pipeline.
Contains: parallel PDF parsing, description generation, node parser classes,
dynamic query engine creation, dynamic tool mappings, and user ID filtering.
"""

import asyncio
from abc import abstractmethod
from concurrent.futures import ThreadPoolExecutor, as_completed
from io import StringIO
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple, cast

import pandas as pd
from llama_index.core import Settings
from llama_index.core.async_utils import DEFAULT_NUM_WORKERS, run_jobs
from llama_index.core.bridge.pydantic import BaseModel, Field
from llama_index.core.callbacks.base import CallbackManager
from llama_index.core.llms.llm import LLM
from llama_index.core.node_parser.interface import NodeParser
from llama_index.core.node_parser.relational.base_element import Element
from llama_index.core.question_gen.llm_generators import LLMQuestionGenerator
from llama_index.core.schema import BaseNode, Document, IndexNode, NodeWithScore, QueryBundle, TextNode
from llama_index.core.tools import ToolMetadata
from llama_index.core.utils import get_tqdm_iterable
# Import QdrantVectorStore
try:
    from llama_index.vector_stores.qdrant import QdrantVectorStore
except ImportError:
    raise ImportError(
        "llama-index-vector-stores-qdrant package is not installed. "
        "Please install it with: pip install llama-index-vector-stores-qdrant"
    )
from llama_index.core import VectorStoreIndex
from llama_index.core.postprocessor import SentenceTransformerRerank
from llama_index.core import StorageContext
from qdrant_client import QdrantClient
from tqdm import tqdm
import os
import time
import logging
import threading

_logger = logging.getLogger(__name__)

# Global lock so concurrent index builds (combined + table) don't double embedding rate
_embedding_lock = threading.Lock()


# ==================== Rate-limited embedding wrapper ====================
class ThrottledEmbedding:
    """
    Wraps an embedding model to throttle requests and avoid OpenAI 429 rate limits.
    Uses small batches, delay between batches, and a global lock to serialize all embedding calls.
    """
    def __init__(self, embed_model: Any, embed_batch_size: int = 4, delay_seconds: float = 2.0):
        self._model = embed_model
        self._batch_size = embed_batch_size
        self._delay = delay_seconds

    def _get_text_embeddings(self, texts: List[str]) -> List[List[float]]:
        with _embedding_lock:
            results: List[List[float]] = []
            for i in range(0, len(texts), self._batch_size):
                batch = texts[i : i + self._batch_size]
                if i > 0:
                    time.sleep(self._delay)
                batch_embeddings = self._model._get_text_embeddings(batch)
                results.extend(batch_embeddings)
            return results

    def get_text_embedding_batch(self, texts: List[str], **kwargs: Any) -> List[List[float]]:
        """Throttled batch embedding used by VectorStoreIndex when building from nodes."""
        with _embedding_lock:
            results: List[List[float]] = []
            for i in range(0, len(texts), self._batch_size):
                batch = texts[i : i + self._batch_size]
                if i > 0:
                    time.sleep(self._delay)
                batch_embeddings = self._model.get_text_embedding_batch(batch, **kwargs)
                results.extend(batch_embeddings)
            return results

    def __getattr__(self, name: str) -> Any:
        """Forward all other attributes to the wrapped model (e.g. model_name, async methods)."""
        return getattr(self._model, name)


# Constants
DEFAULT_SUMMARY_QUERY_STR = """\
What is this table about? Give a very concise summary (imagine you are adding a new caption and summary for this table), \
and output the real/existing table title/caption if context provided.\
and output the real/existing table id if context provided.\
and also output whether or not the table should be kept.\
"""


# ==================== Pydantic Models ====================
class TableColumnOutput(BaseModel):
    """Output from analyzing a table column."""
    col_name: str
    col_type: str
    summary: Optional[str] = None

    def __str__(self) -> str:
        """Convert to string representation."""
        return (
            f"Column: {self.col_name}\nType: {self.col_type}\nSummary: {self.summary}"
        )


class TableOutput(BaseModel):
    """Output from analyzing a table."""
    summary: str
    table_title: Optional[str] = None
    table_id: Optional[str] = None
    columns: List[TableColumnOutput]


# ==================== Parallel PDF Parsing ====================
def parse_pdfs_parallel(pdf_paths, parser=None, parser_config=None):
    """
    Parse multiple PDFs in parallel using LlamaParse.
    Fixes asyncio event loop issue by creating a new parser and event loop per thread.
    
    Args:
        pdf_paths: List of PDF file paths to parse
        parser: LlamaParse instance (optional, used to extract config if parser_config not provided)
        parser_config: Dict with parser configuration (api_key, num_workers, etc.)
                       If None and parser provided, extracts config from parser
    
    Returns:
        dict: Maps PDF path to parsed documents list
    """
    from llama_parse import LlamaParse
    
    # Extract config from parser if provided and parser_config not set
    if parser_config is None and parser is not None:
        parser_config = {}
        if hasattr(parser, 'api_key'):
            parser_config['api_key'] = parser.api_key
        if hasattr(parser, 'num_workers'):
            parser_config['num_workers'] = parser.num_workers
        if hasattr(parser, 'show_progress'):
            parser_config['show_progress'] = parser.show_progress
    
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
            
            if parser_config:
                thread_parser = LlamaParse(**parser_config)
            else:
                thread_parser = LlamaParse()
            
            docs = thread_parser.load_data(pdf_path)
            return (pdf_path, docs)
        except Exception as e:
            print(f"Error parsing {pdf_path}: {e}")
            import traceback
            traceback.print_exc()
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
        futures = {executor.submit(parse_single, path): path for path in pdf_paths}
        
        for future in as_completed(futures):
            pdf_path, docs = future.result()
            if docs is not None:
                parsed_docs_map[pdf_path] = docs
                print(f"✓ Parsed {pdf_path}: {len(docs)} documents")
            else:
                print(f"✗ Failed to parse {pdf_path}")
    
    return parsed_docs_map


# ==================== Document Description Generation ====================
def generate_document_description(documents, max_preview_chars=1000):
    """
    Generate a concise description for a document using LLM based on document preview.
    
    Args:
        documents: List of parsed document objects from LlamaParse
        max_preview_chars: Maximum characters to extract from document for LLM context
    
    Returns:
        str: Generated description (max 50 words)
    """
    preview_text = ""
    if documents and len(documents) > 0:
        first_doc = documents[0]
        if hasattr(first_doc, 'text'):
            preview_text = first_doc.text[:max_preview_chars]
        elif hasattr(first_doc, 'get_content'):
            preview_text = first_doc.get_content()[:max_preview_chars]
        elif isinstance(first_doc, str):
            preview_text = first_doc[:max_preview_chars]
    
    if not preview_text and documents and len(documents) > 0:
        first_doc = documents[0]
        if hasattr(first_doc, 'metadata'):
            metadata = first_doc.metadata
            if isinstance(metadata, dict):
                preview_text = str(metadata).get('file_name', '')[:max_preview_chars]
    
    if not preview_text:
        return f"Document with {len(documents)} pages"
    
    prompt = f"""Generate a one-sentence description (maximum 50 words) for this document to help an LLM decide when to query it for answering user questions.

Document preview:
{preview_text}

Description:"""
    
    try:
        response = Settings.llm.complete(prompt)
        description = str(response).strip()
        description = description.strip('"').strip("'").strip()
        
        words = description.split()
        if len(words) > 50:
            description = ' '.join(words[:50]) + '...'
        
        return description
    except Exception as e:
        print(f"Error generating description: {e}")
        return f"Document with {len(documents)} pages"


# ==================== MarkdownElementNodeParser Classes ====================
def md_to_df(md_str: str) -> pd.DataFrame:
    """Convert Markdown to dataframe."""
    md_str = md_str.replace('"', '""')
    md_str = md_str.replace("|", '","')
    lines = md_str.split("\n")
    md_str = "\n".join(lines[:1] + lines[2:])
    lines = md_str.split("\n")
    md_str = "\n".join([line[2:-2] for line in lines])
    
    if len(md_str) == 0:
        return None
    
    return pd.read_csv(StringIO(md_str))


class BaseElementNodeParser(NodeParser):
    """Splits a document into Text Nodes and Index Nodes corresponding to embedded objects."""
    
    callback_manager: CallbackManager = Field(
        default_factory=CallbackManager, exclude=True
    )
    llm: Optional[LLM] = Field(
        default=None, description="LLM model to use for summarization."
    )
    summary_query_str: str = Field(
        default=DEFAULT_SUMMARY_QUERY_STR,
        description="Query string to use for summarization.",
    )
    num_workers: int = Field(
        default=DEFAULT_NUM_WORKERS,
        description="Num of works for async jobs.",
    )
    show_progress: bool = Field(default=True, description="Whether to show progress.")

    @classmethod
    def class_name(cls) -> str:
        return "BaseStructuredNodeParser"

    @classmethod
    def from_defaults(
        cls,
        callback_manager: Optional[CallbackManager] = None,
        **kwargs: Any,
    ) -> "BaseElementNodeParser":
        callback_manager = callback_manager or CallbackManager([])
        return cls(callback_manager=callback_manager, **kwargs)

    def _parse_nodes(
        self,
        nodes: Sequence[BaseNode],
        show_progress: bool = False,
        **kwargs: Any,
    ) -> List[BaseNode]:
        all_nodes: List[BaseNode] = []
        nodes_with_progress = get_tqdm_iterable(nodes, show_progress, "Parsing nodes")
        for node in nodes_with_progress:
            nodes = self.get_nodes_from_node(node)
            all_nodes.extend(nodes)
        return all_nodes

    @abstractmethod
    def get_nodes_from_node(self, node: TextNode) -> List[BaseNode]:
        """Get nodes from node."""

    @abstractmethod
    def extract_elements(self, text: str, **kwargs: Any) -> List[Element]:
        """Extract elements from text."""

    def get_table_elements(self, elements: List[Element]) -> List[Element]:
        """Get table elements."""
        return [e for e in elements if e.type == "table" or e.type == "table_text"]

    def get_text_elements(self, elements: List[Element]) -> List[Element]:
        """Get text elements."""
        return [e for e in elements if e.type != "table"]

    def extract_table_summaries(self, elements: List[Element]) -> None:
        """Go through elements, extract out summaries that are tables."""
        llm = cast(LLM, self.llm)
        table_context_list = []
        for idx, element in tqdm(enumerate(elements)):
            if element.type not in ("table", "table_text"):
                continue
            table_context = str(element.element)
            if idx > 0 and str(elements[idx - 1].element).lower().strip().startswith("table"):
                table_context = str(elements[idx - 1].element) + "\n" + table_context
            if idx < len(elements) + 1 and str(elements[idx - 1].element).lower().strip().startswith("table"):
                table_context += "\n" + str(elements[idx + 1].element)
            table_context_list.append(table_context)

        async def _get_table_output(table_context: str, summary_query_str: str) -> Any:
            return TableOutput(summary=str(table_context), columns=[])

        summary_jobs = [
            _get_table_output(table_context, self.summary_query_str)
            for table_context in table_context_list
        ]
        # Handle both sync and async contexts (FastAPI has running event loop)
        async def _run_jobs_async():
            return await run_jobs(summary_jobs, show_progress=self.show_progress, workers=self.num_workers)
        
        try:
            loop = asyncio.get_running_loop()
            # Running in async context - use thread pool with new event loop
            def run_in_thread():
                new_loop = asyncio.new_event_loop()
                asyncio.set_event_loop(new_loop)
                try:
                    return new_loop.run_until_complete(_run_jobs_async())
                finally:
                    new_loop.close()
            
            with ThreadPoolExecutor() as executor:
                future = executor.submit(run_in_thread)
                summary_outputs = future.result()
        except RuntimeError:
            # No running loop - safe to use asyncio.run()
            summary_outputs = asyncio.run(_run_jobs_async())
        for element, summary_output in zip(elements, summary_outputs):
            element.table_output = summary_output

    def get_base_nodes_and_mappings(
        self, nodes: List[BaseNode]
    ) -> Tuple[List[BaseNode], Dict]:
        """Get base nodes and mappings."""
        node_dict = {node.node_id: node for node in nodes}
        node_mappings = {}
        base_nodes = []
        nonbase_node_ids = set()
        for node in nodes:
            if isinstance(node, IndexNode):
                node_mappings[node.index_id] = node_dict[node.index_id]
                nonbase_node_ids.add(node.index_id)
        for node in nodes:
            if node.node_id not in nonbase_node_ids:
                base_nodes.append(node)
        return base_nodes, node_mappings

    def get_nodes_and_objects(
        self, nodes: List[BaseNode]
    ) -> Tuple[List[BaseNode], List[IndexNode]]:
        base_nodes, node_mappings = self.get_base_nodes_and_mappings(nodes)
        nodes_list = []
        objects = []
        for node in base_nodes:
            if isinstance(node, IndexNode):
                node.obj = node_mappings[node.index_id]
                objects.append(node)
            else:
                nodes_list.append(node)
        return nodes_list, objects

    def _get_nodes_from_buffer(
        self, buffer: List[str], node_parser: NodeParser
    ) -> List[BaseNode]:
        """Get nodes from buffer."""
        doc = Document(text="\n\n".join(list(buffer)))
        return node_parser.get_nodes_from_documents([doc])

    def get_nodes_from_elements(self, elements: List[Element]) -> List[BaseNode]:
        """Get nodes and mappings.
        
        Includes footnote co-location: footnote text (lines starting with [N])
        is collected from all text elements and appended to every table node so
        that the embeddings for tables carry their explanatory footnotes.
        """
        import re
        from llama_index.core.node_parser import SentenceSplitter
        node_parser = SentenceSplitter()

        # ---- Pre-scan: collect ALL footnote lines from text elements ----
        footnote_lines = []
        for element in elements:
            if element.type in ("table", "table_text"):
                continue
            text = str(element.element)
            for line in text.split("\n"):
                stripped = line.strip()
                if re.match(r'^\[\d+\]', stripped):
                    footnote_lines.append(stripped)
        footnote_block = "\n".join(footnote_lines) if footnote_lines else ""

        # ---- Also collect the nearest section heading for table context ----
        last_heading = ""

        nodes = []
        cur_text_el_buffer: List[str] = []
        for element in elements:
            # Track headings for context injection into tables
            elem_text = str(element.element).strip()
            if element.type == "text" and elem_text.startswith("#"):
                last_heading = elem_text.lstrip("#").strip()
            elif element.type == "title":
                last_heading = elem_text

            if element.type == "table" or element.type == "table_text":
                if len(cur_text_el_buffer) > 0:
                    cur_text_nodes = self._get_nodes_from_buffer(cur_text_el_buffer, node_parser)
                    nodes.extend(cur_text_nodes)
                    cur_text_el_buffer = []
                table_output = cast(TableOutput, element.table_output)
                table_md = ""
                if element.type == "table":
                    table_df = cast(pd.DataFrame, element.table)
                    table_md = "|"
                    for col_name, col in table_df.items():
                        table_md += f"{col_name}|"
                    table_md += "\n|"
                    for col_name, col in table_df.items():
                        table_md += f"---|"
                    table_md += "\n"
                    for row in table_df.itertuples():
                        table_md += "|"
                        for col in row[1:]:
                            table_md += f"{col}|"
                        table_md += "\n"
                elif element.type == "table_text":
                    table_md = str(element.element)
                table_id = element.id + "_table"
                table_ref_id = element.id + "_table_ref"
                col_schema = "\n\n".join([str(col) for col in table_output.columns])
                table_summary = str(table_output.summary)
                if table_output.table_title:
                    table_summary += ",\nwith the following table title:\n"
                    table_summary += str(table_output.table_title)
                table_summary += ",\nwith the following columns:\n"
                for col in table_output.columns:
                    table_summary += f"- {col.col_name}: {col.summary}\n"

                # ---- Inject section heading context ----
                heading_context = ""
                if last_heading:
                    heading_context = f"Section: {last_heading}\n"

                # ---- Build final table string with footnotes co-located ----
                table_str = heading_context + table_summary + "\n" + table_md
                if footnote_block:
                    table_str += "\n\nFootnotes:\n" + footnote_block

                index_node = IndexNode(
                    text=heading_context + table_summary + (
                        ("\nFootnotes:\n" + footnote_block) if footnote_block else ""
                    ),
                    metadata={"col_schema": col_schema},
                    excluded_embed_metadata_keys=["col_schema"],
                    id_=table_ref_id,
                    index_id=table_id,
                )
                text_node = TextNode(
                    text=table_str,
                    id_=table_id,
                    metadata={
                        "table_df": (
                            str(table_df.to_dict())
                            if element.type == "table"
                            else table_md
                        ),
                        "table_summary": table_summary,
                    },
                    excluded_embed_metadata_keys=["table_df", "table_summary"],
                    excluded_llm_metadata_keys=["table_df", "table_summary"],
                )
                nodes.extend([index_node, text_node])
            else:
                cur_text_el_buffer.append(str(element.element))
        if len(cur_text_el_buffer) > 0:
            cur_text_nodes = self._get_nodes_from_buffer(cur_text_el_buffer, node_parser)
            nodes.extend(cur_text_nodes)
            cur_text_el_buffer = []
        return [node for node in nodes if len(node.text) > 0]


class MarkdownElementNodeParser(BaseElementNodeParser):
    """Markdown element node parser."""
    
    @classmethod
    def class_name(cls) -> str:
        return "MarkdownElementNodeParser"

    def get_nodes_from_node(self, node: TextNode) -> List[BaseNode]:
        """Get nodes from node."""
        elements = self.extract_elements(
            node.get_content(),
            table_filters=[self.filter_table],
            node_id=node.id_,
        )
        table_elements = self.get_table_elements(elements)
        self.extract_table_summaries(table_elements)
        return self.get_nodes_from_elements(elements)

    def extract_elements(
        self,
        text: str,
        node_id: Optional[str] = None,
        table_filters: Optional[List[Callable]] = None,
        **kwargs: Any,
    ) -> List[Element]:
        """Extract elements from text."""
        lines = text.split("\n")
        currentElement = None
        elements: List[Element] = []
        
        for line in lines:
            if line.startswith("```"):
                if currentElement is not None and currentElement.type == "code":
                    elements.append(currentElement)
                    currentElement = None
                    if len(line) > 3:
                        elements.append(Element(id=f"id_{len(elements)}", type="text", element=line.lstrip("```")))
                elif line.count("```") == 2 and line[-3] != "`":
                    if currentElement is not None:
                        elements.append(currentElement)
                    currentElement = Element(id=f"id_{len(elements)}", type="code", element=line.lstrip("```"))
                elif currentElement is not None and currentElement.type == "text":
                    currentElement.element += "\n" + line
                else:
                    if currentElement is not None:
                        elements.append(currentElement)
                    currentElement = Element(id=f"id_{len(elements)}", type="text", element=line)
            elif currentElement is not None and currentElement.type == "code":
                currentElement.element += "\n" + line
            elif line.startswith("|"):
                if currentElement is not None and currentElement.type != "table":
                    if currentElement is not None:
                        elements.append(currentElement)
                    currentElement = Element(id=f"id_{len(elements)}", type="table", element=line)
                elif currentElement is not None:
                    currentElement.element += "\n" + line
                else:
                    currentElement = Element(id=f"id_{len(elements)}", type="table", element=line)
            elif line.startswith("#"):
                if currentElement is not None:
                    elements.append(currentElement)
                currentElement = Element(
                    id=f"id_{len(elements)}",
                    type="title",
                    element=line.lstrip("#"),
                    title_level=len(line) - len(line.lstrip("#")),
                )
            else:
                if currentElement is not None and currentElement.type != "text":
                    elements.append(currentElement)
                    currentElement = Element(id=f"id_{len(elements)}", type="text", element=line)
                elif currentElement is not None:
                    currentElement.element += "\n" + line
                else:
                    currentElement = Element(id=f"id_{len(elements)}", type="text", element=line)
        if currentElement is not None:
            elements.append(currentElement)

        for idx, element in enumerate(elements):
            if element.type == "table":
                should_keep = True
                perfect_table = True
                table_lines = element.element.split("\n")
                table_columns = [len(line.split("|")) for line in table_lines]
                if len(set(table_columns)) > 1:
                    perfect_table = False
                if len(table_lines) < 2:
                    should_keep = False
                if should_keep and perfect_table and table_filters is not None:
                    should_keep = all(tf(element) for tf in table_filters)
                if should_keep:
                    if perfect_table:
                        table = md_to_df(element.element)
                        elements[idx] = Element(
                            id=f"id_{node_id}_{idx}" if node_id else f"id_{idx}",
                            type="table",
                            element=element,
                            table=table,
                        )
                    else:
                        elements[idx] = Element(
                            id=f"id_{node_id}_{idx}" if node_id else f"id_{idx}",
                            type="table_text",
                            element=element.element,
                        )
                else:
                    elements[idx] = Element(
                        id=f"id_{node_id}_{idx}" if node_id else f"id_{idx}",
                        type="text",
                        element=element.element,
                    )
            else:
                elements[idx] = Element(
                    id=f"id_{node_id}_{idx}" if node_id else f"id_{idx}",
                    type="text",
                    element=element.element,
                )

        merged_elements: List[Element] = []
        for element in elements:
            if (
                len(merged_elements) > 0
                and element.type == "text"
                and merged_elements[-1].type == "text"
            ):
                merged_elements[-1].element += "\n" + element.element
            else:
                merged_elements.append(element)
        return merged_elements

    def filter_table(self, table_element: Any) -> bool:
        """Filter tables."""
        table_df = md_to_df(table_element.element)
        return table_df is not None and not table_df.empty and len(table_df.columns) > 1


# ==================== Qdrant Configuration ====================
def get_qdrant_client(host=None, port=None, url=None, api_key=None):
    """
    Get or create Qdrant client connection.
    Supports local (host/port) and Qdrant Cloud (url + api_key).

    Args:
        host: Qdrant host (default: localhost or QDRANT_HOST env var)
        port: Qdrant port (default: 6333 or QDRANT_PORT env var)
        url: Full Qdrant URL (e.g. https://xxx.cloud.qdrant.io:6333). If set with "://", uses Cloud.
        api_key: API key for Qdrant Cloud (optional; also from QDRANT_API_KEY env)

    Returns:
        QdrantClient instance
    """
    url = url or os.getenv("QDRANT_URL")
    api_key = api_key if api_key is not None else os.getenv("QDRANT_API_KEY")
    if url and "://" in str(url):
        return QdrantClient(url=url, api_key=api_key)
    host = host or os.getenv("QDRANT_HOST", "localhost")
    port = port or int(os.getenv("QDRANT_PORT", "6333"))
    return QdrantClient(host=host, port=port)


# ==================== Dynamic Query Engine Creation ====================
def get_query_engine(nodes, node_parser, tool_name=None, qdrant_client=None):
    """
    Create combined and table-only query engines with SentenceTransformer reranker using Qdrant.
    
    Args:
        nodes: List of parsed nodes
        node_parser: MarkdownElementNodeParser instance
        tool_name: Tool name (format: user_id_doc_id) for collection naming and metadata
        qdrant_client: Optional QdrantClient instance (creates new if not provided)
    
    Returns:
        tuple: (combined_query_engine, table_query_engine)
    """
    # Get Qdrant client
    if qdrant_client is None:
        qdrant_client = get_qdrant_client()
    
    # Extract user_id and doc_id from tool_name if provided
    user_id = None
    doc_id = None
    if tool_name:
        parts = tool_name.split("_", 1)
        if len(parts) == 2:
            user_id = parts[0]
            doc_id = parts[1]
    
    # Add metadata to nodes for filtering
    base_nodes, objects = node_parser.get_nodes_and_objects(nodes)
    all_nodes = base_nodes + objects
    
    # Sanitize node IDs to ensure they're valid UUIDs for Qdrant
    # Qdrant requires point IDs to be either UUIDs or integers, not strings with underscores
    import uuid
    
    def sanitize_node_id(node_id: str) -> str:
        """Convert node ID to a valid UUID format for Qdrant."""
        if not node_id:
            return str(uuid.uuid4())
        # If already a valid UUID, return as is
        try:
            uuid.UUID(node_id)
            return node_id
        except (ValueError, AttributeError):
            # Not a valid UUID - generate one based on the ID string for consistency
            # Use MD5 hash of the ID to ensure same ID always gets same UUID
            import hashlib
            md5_hash = hashlib.md5(node_id.encode()).hexdigest()
            # Convert MD5 hash to UUID format (128 bits)
            return str(uuid.UUID(md5_hash))
    
    # Add user_id and doc_id to node metadata for Qdrant filtering
    # Also sanitize node IDs to ensure they're valid UUIDs
    for node in all_nodes:
        # Sanitize node ID for Qdrant compatibility
        if hasattr(node, 'id_') and node.id_:
            node.id_ = sanitize_node_id(node.id_)
        
        if not hasattr(node, 'metadata'):
            node.metadata = {}
        if user_id:
            node.metadata['user_id'] = user_id
        if doc_id:
            node.metadata['doc_id'] = doc_id
        if tool_name:
            node.metadata['tool_name'] = tool_name
    
    # Create collection names based on tool_name or use defaults
    if tool_name:
        combined_collection = f"{tool_name}_combined"
        table_collection = f"{tool_name}_table"
    else:
        combined_collection = "default_combined"
        table_collection = "default_table"
    
    # Create Qdrant vector stores
    combined_vector_store = QdrantVectorStore(
        collection_name=combined_collection,
        client=qdrant_client,
    )
    
    table_vector_store = QdrantVectorStore(
        collection_name=table_collection,
        client=qdrant_client,
    )
    
    # Create storage contexts
    combined_storage_context = StorageContext.from_defaults(vector_store=combined_vector_store)
    table_storage_context = StorageContext.from_defaults(vector_store=table_vector_store)
    
    # Create indices using Qdrant
    combined_index = VectorStoreIndex(
        nodes=all_nodes,
        storage_context=combined_storage_context,
    )
    table_index = VectorStoreIndex(
        nodes=objects,
        storage_context=table_storage_context,
    )

    # Initialize SentenceTransformerRerank for reranking retrieved nodes
    # Using cross-encoder/ms-marco-MiniLM-L-2-v2 model for good performance and compatibility
    sentence_reranker = SentenceTransformerRerank(
        model="cross-encoder/ms-marco-MiniLM-L-2-v2",
        top_n=5
    )
    node_postprocessors = [sentence_reranker]

    combined_index_query_engine = combined_index.as_query_engine(
        similarity_top_k=10, 
        node_postprocessors=node_postprocessors, 
        verbose=False
    )
    table_index_query_engine = table_index.as_query_engine(
        similarity_top_k=10, 
        node_postprocessors=node_postprocessors, 
        verbose=False
    )
    return combined_index_query_engine, table_index_query_engine


def create_query_engines_dynamically(document_nodes_configs, node_parser):
    """
    Dynamically create query engines for multiple documents from their parsed nodes.
    
    Args:
        document_nodes_configs: List of dicts with keys: 'tool_name', 'nodes'
        node_parser: MarkdownElementNodeParser instance
    
    Returns:
        dict: Maps tool_name to dict with 'query_engine' and 'table_query_engine'
    """
    query_engines_map = {}
    
    for config in document_nodes_configs:
        tool_name = config['tool_name']
        nodes = config['nodes']
        query_engine, table_query_engine = get_query_engine(nodes, node_parser, tool_name=tool_name)
        query_engines_map[tool_name] = {
            'query_engine': query_engine,
            'table_query_engine': table_query_engine
        }
    
    return query_engines_map


# ==================== Dynamic Tool Mappings ====================
def create_dynamic_tool_mappings(document_configs):
    """
    Dynamically create tool_choices, query_engine_tools_map, and table_query_engine_tools_map.
    
    Args:
        document_configs: List of dicts with keys: 'tool_name', 'description', 'query_engine', 'table_query_engine'
    
    Returns:
        tuple: (tool_choices, query_engine_tools_map, table_query_engine_tools_map)
    """
    tool_choices = []
    query_engine_tools_map = {}
    table_query_engine_tools_map = {}
    
    for config in document_configs:
        tool_name = config['tool_name']
        description = config.get('description', f"Provides information from {tool_name}")
        query_engine = config['query_engine']
        table_query_engine = config['table_query_engine']
        
        tool_choices.append(ToolMetadata(name=tool_name, description=description))
        query_engine_tools_map[tool_name] = query_engine
        table_query_engine_tools_map[tool_name] = table_query_engine
    
    return tool_choices, query_engine_tools_map, table_query_engine_tools_map


# ==================== User ID Filtering ====================
def filter_tool_choices_by_user_id(tool_choices, user_id):
    """Filter tool_choices to only include tools for a specific user_id."""
    filtered = []
    for tool in tool_choices:
        if tool.name.startswith(f"{user_id}_"):
            filtered.append(tool)
    return filtered


def filter_engine_maps_by_user_id(engine_map, user_id):
    """Filter query_engine_tools_map or table_query_engine_tools_map by user_id."""
    filtered = {}
    for tool_name, engine in engine_map.items():
        if tool_name.startswith(f"{user_id}_"):
            filtered[tool_name] = engine
    return filtered


def generate_subquestions_with_user_id(query, user_id, tool_choices):
    """
    Generate sub questions filtered by user_id - only generates questions for user's documents.
    
    Args:
        query: User query string
        user_id: User ID to filter tools by
        tool_choices: Full list of ToolMetadata objects
    
    Returns:
        Dict with generated_questions_map and query
    """
    user_tool_choices = filter_tool_choices_by_user_id(tool_choices, user_id)
    
    if not user_tool_choices:
        return {"generated_questions_map": [], "query": query}
    
    question_gen = LLMQuestionGenerator.from_defaults(llm=Settings.llm)
    choices = question_gen.generate(user_tool_choices, QueryBundle(query_str=query))
    
    return {"generated_questions_map": choices, "query": query}


# ==================== Complete Workflow Functions ====================
def process_document_upload(user_id, doc_id, pdf_path, parser, node_parser):
    """
    Complete workflow to process a single PDF upload.
    Handles: parsing -> description generation -> node generation -> query engine creation.
    
    Args:
        user_id: User ID
        doc_id: Document ID
        pdf_path: Path to PDF file
        parser: LlamaParse instance
        node_parser: MarkdownElementNodeParser instance
    
    Returns:
        dict: Document config with tool_name, description, query_engine, table_query_engine
    """
    # Step 1: Parse PDF
    print(f"Step 1/4: Parsing PDF: {pdf_path}")
    parsed_results = parse_pdfs_parallel([pdf_path], parser=parser)
    docs = parsed_results.get(pdf_path)
    
    if not docs:
        raise ValueError(f"Failed to parse PDF: {pdf_path}")
    print(f"✓ Parsed {len(docs)} documents")
    
    # Step 2: Generate description
    print(f"Step 2/4: Generating description for document...")
    description = generate_document_description(docs)
    print(f"✓ Description: {description[:100]}...")
    
    # Step 3: Generate nodes
    print(f"Step 3/4: Generating nodes from document...")
    nodes = node_parser.get_nodes_from_documents(docs)
    print(f"✓ Generated {len(nodes)} nodes")
    
    # Step 4: Create query engines
    print(f"Step 4/4: Creating query engines...")
    tool_name = f"{user_id}_{doc_id}"
    query_engine, table_query_engine = get_query_engine(nodes, node_parser, tool_name=tool_name)
    print(f"✓ Query engines created")
    
    # Step 5: Create document config
    document_config = {
        "tool_name": tool_name,
        "description": description,
        "query_engine": query_engine,
        "table_query_engine": table_query_engine
    }
    
    print(f"✓ Document processing complete! Tool name: {tool_name}\n")
    return document_config


def process_multiple_documents(user_id, pdf_paths_with_doc_ids, parser, node_parser):
    """
    Process multiple PDFs and create complete tool mappings.
    
    Args:
        user_id: User ID
        pdf_paths_with_doc_ids: List of tuples (pdf_path, doc_id)
        parser: LlamaParse instance
        node_parser: MarkdownElementNodeParser instance
    
    Returns:
        tuple: (document_configs, tool_choices, query_engine_tools_map, table_query_engine_tools_map)
    """
    document_configs = []
    
    for pdf_path, doc_id in pdf_paths_with_doc_ids:
        document_config = process_document_upload(user_id, doc_id, pdf_path, parser, node_parser)
        document_configs.append(document_config)
    
    # Generate tool mappings
    tool_choices, query_engine_tools_map, table_query_engine_tools_map = create_dynamic_tool_mappings(document_configs)
    
    print(f"✓ Created tool mappings for {len(tool_choices)} documents")
    return document_configs, tool_choices, query_engine_tools_map, table_query_engine_tools_map


def generate_pdf_paths_with_doc_ids(pdf_paths, user_id=None):
    """
    Automatically generate pdf_paths_with_doc_ids from a list of PDF paths.
    Creates doc_ids from filenames by removing extension and sanitizing.
    
    Args:
        pdf_paths: List of PDF file paths
        user_id: Optional user_id prefix for doc_id
    
    Returns:
        List of tuples: [(pdf_path, doc_id), ...]
    """
    import re
    pdf_paths_with_doc_ids = []
    
    for pdf_path in pdf_paths:
        # Extract filename without extension
        filename = os.path.basename(pdf_path)
        doc_id = os.path.splitext(filename)[0]
        
        # Sanitize doc_id: remove special characters, replace spaces/underscores with underscores
        doc_id = re.sub(r'[^a-zA-Z0-9_-]', '_', doc_id)
        doc_id = re.sub(r'[_\s]+', '_', doc_id).strip('_')
        
        # Add user_id prefix if provided
        if user_id:
            doc_id = f"{user_id}_{doc_id}"
        
        pdf_paths_with_doc_ids.append((pdf_path, doc_id))
        print(f"✓ Mapped {pdf_path} -> doc_id: {doc_id}")
    
    return pdf_paths_with_doc_ids

