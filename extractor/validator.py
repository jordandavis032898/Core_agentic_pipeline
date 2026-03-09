# robust_llm_validator_v2.py
# Generic table extraction using GPT-4o-mini
# Self-contained for router integration

from dataclasses import dataclass
from typing import List, Dict, Any, Optional
import json
import asyncio

@dataclass
class ValidationOutput:
    data: Optional[Any] = None
    error: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None
    explanation: Optional[str] = None

# ----- LLM CLIENT -----
class LLMClient:
    def chat(self, model: str, system_prompt: str, user_prompt: str) -> str:
        raise NotImplementedError

class OpenAILLM(LLMClient):
    def __init__(self, api_key:str):
        from openai import OpenAI
        self.client = OpenAI(api_key=api_key)

    def chat(self, model:str, system_prompt:str, user_prompt:str) -> str:
        resp = self.client.chat.completions.create(
            model=model,
            temperature=0,
            messages=[
                {"role":"system","content":system_prompt},
                {"role":"user","content":user_prompt}
            ]
        )
        return resp.choices[0].message.content

# ----- PROMPT -----
SYSTEM_PROMPT = """You are a precise table extraction tool. Given markdown content (which may contain pipe tables), extract ALL tables into structured JSON.

For each table found:
1. **title**: The heading or caption immediately before the table, or a descriptive title if none exists.
2. **headers**: The column header names from the first row of the table.
3. **rows**: Each data row as an array of cell values.
   - Numeric values: remove commas and currency symbols. Parentheses mean negative (e.g. "(123)" → -123).
   - Empty cells → null.
   - Preserve text values as-is.
4. **summary**: One sentence describing what the table contains.

Rules:
- Return ALL tables found in the content.
- If no tables are found, return {"tables": []}.
- Output ONLY valid JSON, no markdown fences, no commentary.
- Preserve the original row order.
"""

USER_TEMPLATE = """Extract all tables from this page content into structured JSON.

CONTENT:
{snippet}

Return JSON with format: {{"tables": [{{"title": str, "headers": [str], "rows": [[values]], "summary": str}}]}}"""

@dataclass
class ValidationResult:
    data: Dict[str, Any]
    metadata: Optional[Dict[str, Any]] = None
    explanation: Optional[str] = None

# ----- VALIDATOR -----
class LLMOnlyFinancialTableValidatorV2:
    def __init__(self, llm: LLMClient, model="gpt-4o-mini"):
        self.llm = llm
        self.model = model

    def run(self, page_text: str, context: Optional[str] = None) -> ValidationResult:
        user_prompt = USER_TEMPLATE.format(snippet=page_text.strip())
        raw = self.llm.chat(self.model, SYSTEM_PROMPT, user_prompt)
        data = self._parse_response(raw)

        tables = data.get("tables", [])
        metadata = {
            "table_count": len(tables),
            "table_titles": [t.get("title", "") for t in tables],
        }
        explanation = self._generate_explanation(tables)

        return ValidationResult(data=data, metadata=metadata, explanation=explanation)

    def _parse_response(self, raw: str) -> Dict[str, Any]:
        cleaned = raw.strip()
        # Strip markdown code fences
        if cleaned.startswith("```"):
            first_newline = cleaned.find("\n")
            if first_newline > 0:
                cleaned = cleaned[first_newline + 1:]
            if cleaned.rstrip().endswith("```"):
                cleaned = cleaned.rstrip()[:-3].rstrip()
        try:
            obj = json.loads(cleaned)
        except json.JSONDecodeError:
            # Try to extract a JSON object from the response
            start = cleaned.find("{")
            end = cleaned.rfind("}")
            if start >= 0 and end > start:
                obj = json.loads(cleaned[start : end + 1])
            else:
                raise ValueError("No JSON found in LLM response")

        if "tables" in obj:
            return obj
        # Single table object without wrapper
        if "headers" in obj and "rows" in obj:
            return {"tables": [obj]}
        return {"tables": []}

    def _generate_explanation(self, tables: List[Dict[str, Any]]) -> str:
        if not tables:
            return "No tables found on this page."
        parts = [f"Found {len(tables)} table(s):"]
        for i, t in enumerate(tables, 1):
            title = t.get("title", "Untitled")
            summary = t.get("summary", "")
            row_count = len(t.get("rows", []))
            parts.append(f"  {i}. **{title}** ({row_count} rows) - {summary}")
        return "\n".join(parts)

# ----- RUNNER -----
async def run_validator_on_pages_llm_v2(vt, selected_pages: List[Dict[str, Any]], max_concurrency: int = 3):
    sem = asyncio.Semaphore(max_concurrency)

    async def _process(i, page):
        async with sem:
            try:
                # Get context from previous page if available
                context = None
                if i > 0 and "page_content" in selected_pages[i-1]:
                    context = selected_pages[i-1]["page_content"][-500:]  # Last 500 chars
                
                res = await asyncio.to_thread(vt.run, page["page_content"], context)
                # vt.run returns ValidationResult with data, metadata, and explanation
                if hasattr(res, "data"):
                    # Create a custom object that preserves all ValidationResult attributes
                    class ResultWrapper:
                        def __init__(self, validation_result):
                            self.data = validation_result.data
                            self.metadata = validation_result.metadata
                            self.explanation = validation_result.explanation
                            self.error = None
                    
                    return i, ResultWrapper(res)
                else:
                    return i, ValidationOutput(data=res)
            except Exception as e:
                return i, ValidationOutput(error=str(e))

    tasks = [asyncio.create_task(_process(i, p)) for i, p in enumerate(selected_pages)]
    out = await asyncio.gather(*tasks)
    out.sort(key=lambda x: x[0])
    return [r for _, r in out]

