from typing import Any, Dict, List, Tuple, Optional


class MarkdownElementNodeParser:
    def __init__(self, llm: Any = None, num_workers: int = 0):
        self.llm = llm
        self.num_workers = num_workers

    def get_nodes_from_documents(self, documents: List[Any]) -> List[Any]:
        return []


def get_qdrant_client(host: Optional[str] = None, port: Optional[int] = None, url: Optional[str] = None, api_key: Optional[str] = None) -> Any:
    return None


def get_query_engine(nodes: List[Any], node_parser: Any, tool_name: Optional[str] = None, qdrant_client: Any = None) -> Tuple[Any, Any]:
    class Engine:
        def retrieve(self, *args: Any, **kwargs: Any) -> List[Any]:
            return []

    engine = Engine()
    return engine, engine


def create_dynamic_tool_mappings(document_configs: List[Dict[str, Any]]) -> Tuple[List[Any], Dict[str, Any], Dict[str, Any]]:
    tool_choices: List[Any] = []
    query_engine_tools_map: Dict[str, Any] = {}
    table_query_engine_tools_map: Dict[str, Any] = {}
    return tool_choices, query_engine_tools_map, table_query_engine_tools_map


def generate_document_description(documents: List[Any], max_preview_chars: int = 1000) -> str:
    return "Document"


def process_multiple_documents(
    user_id: str,
    pdf_paths_with_doc_ids: List[Tuple[str, str]],
    parser: Any,
    node_parser: Any,
) -> Tuple[List[Dict[str, Any]], List[Any], Dict[str, Any], Dict[str, Any]]:
    document_configs: List[Dict[str, Any]] = []
    tool_choices, query_engine_tools_map, table_query_engine_tools_map = create_dynamic_tool_mappings(document_configs)
    return document_configs, tool_choices, query_engine_tools_map, table_query_engine_tools_map


def generate_pdf_paths_with_doc_ids(pdf_paths: List[str], user_id: Optional[str] = None) -> List[Tuple[str, str]]:
    result: List[Tuple[str, str]] = []
    for index, path in enumerate(pdf_paths):
        doc_id = f"{user_id}_{index}" if user_id else str(index)
        result.append((path, doc_id))
    return result

