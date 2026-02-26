from dataclasses import dataclass
from typing import Any, Dict, List, Optional
import asyncio


@dataclass
class ValidationOutput:
    data: Optional[Any] = None
    error: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None
    explanation: Optional[str] = None


@dataclass
class ValidationResult:
    data: Dict[str, Any]
    metadata: Optional[Dict[str, Any]] = None
    explanation: Optional[str] = None


class LLMClient:
    def chat(self, model: str, system_prompt: str, user_prompt: str) -> str:
        return "{}"


class OpenAILLM(LLMClient):
    def __init__(self, api_key: str):
        self.api_key = api_key


class LLMOnlyFinancialTableValidatorV2:
    def __init__(self, llm: LLMClient, model: str = "gpt-4o-mini"):
        self.llm = llm
        self.model = model

    def run(self, page_text: str, context: Optional[str] = None) -> ValidationResult:
        return ValidationResult(data={}, metadata={}, explanation="")


async def run_validator_on_pages_llm_v2(
    vt: LLMOnlyFinancialTableValidatorV2,
    selected_pages: List[Dict[str, Any]],
    max_concurrency: int = 3,
) -> List[ValidationOutput]:
    sem = asyncio.Semaphore(max_concurrency)

    async def _process(i: int, page: Dict[str, Any]):
        async with sem:
            try:
                result = await asyncio.to_thread(vt.run, page.get("page_content", ""), None)
                return i, ValidationOutput(data=result.data, metadata=result.metadata, explanation=result.explanation)
            except Exception as e:
                return i, ValidationOutput(error=str(e))

    tasks = [asyncio.create_task(_process(i, p)) for i, p in enumerate(selected_pages)]
    out = await asyncio.gather(*tasks)
    out.sort(key=lambda x: x[0])
    return [r for _, r in out]
 

