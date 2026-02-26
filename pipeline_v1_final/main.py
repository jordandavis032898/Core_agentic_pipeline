from typing import Any, Dict, List, Optional

from pydantic import BaseModel


user_data_store: Dict[str, Dict[str, Any]] = {}


class SubQuestion(BaseModel):
    sub_question: str
    tool_name: str


class SubQuestionAnswerPair(BaseModel):
    sub_q: SubQuestion
    answer: Optional[str] = None
    sources: List[Any] = []


class QueryEvent:
    def __init__(self, query: str):
        self.query = query


class RAGQueryWorkflow:
    def __init__(self, *args: Any, **kwargs: Any):
        pass

    async def run(self, query: str) -> Any:
        class Result:
            def __init__(self, text: str):
                self.result = text

        return Result("")


def create_query_pipeline(
    tool_choices: List[Any],
    query_engine_tools_map: Dict[str, Any],
    table_query_engine_tools_map: Dict[str, Any],
) -> RAGQueryWorkflow:
    return RAGQueryWorkflow()


def initialize_models() -> None:
    return None


def initialize_node_parser() -> None:
    return None

