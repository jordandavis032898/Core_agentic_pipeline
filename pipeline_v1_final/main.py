"""
RAG Query Workflow — real LlamaIndex Workflow-based query pipeline.

Provides: intent classification, query rewriting, sub-question generation,
dual retrieval, data inventory, reasoning/cross-table analysis, and
intent-aware response synthesis.

Extracted from the accelerate79ers reference implementation (pipeline_v1_final/main.py)
with FastAPI server code removed — only the workflow classes and helpers remain.
"""

import os
import sys
import logging
import asyncio
from typing import Dict, List, Optional, Any, cast
from typing import List as TypingList, Optional as TypingOptional
from concurrent.futures import ThreadPoolExecutor

# Apply compatibility patch before importing OpenAI-related packages
try:
    import openai.types.responses as responses_module
    if not hasattr(responses_module, 'ResponseTextAnnotationDeltaEvent'):
        try:
            from openai.types.responses import ResponseOutputTextAnnotationAddedEvent
            responses_module.ResponseTextAnnotationDeltaEvent = ResponseOutputTextAnnotationAddedEvent
        except ImportError:
            class ResponseTextAnnotationDeltaEvent:
                def __init__(self, *args, **kwargs):
                    self.annotation = kwargs.get('annotation', {})
            responses_module.ResponseTextAnnotationDeltaEvent = ResponseTextAnnotationDeltaEvent
except ImportError:
    pass

from llama_index.core import Settings
from llama_index.embeddings.openai import OpenAIEmbedding
from llama_index.llms.openai import OpenAI
from llama_index.core.workflow import (
    Workflow,
    step,
    StartEvent,
    StopEvent,
    Event,
)
from llama_index.core.schema import QueryBundle, NodeWithScore, TextNode
from llama_index.core.prompts import PromptTemplate
from llama_index.core.tools import ToolMetadata
from llama_index.core.question_gen.llm_generators import LLMQuestionGenerator
from llama_index.core.bridge.pydantic import BaseModel as PydanticBaseModel, Field

from helper import MarkdownElementNodeParser

logger = logging.getLogger(__name__)

# Global storage for user data
# Format: {user_id: {document_configs, tool_choices, query_engine_tools_map, table_query_engine_tools_map, qp}}
user_data_store: Dict[str, Dict] = {}

# Global node parser
node_parser = None


# ==================== Pipeline Pydantic Models ====================
class SubQuestion(PydanticBaseModel):
    sub_question: str
    tool_name: str

class SubQuestionAnswerPair(PydanticBaseModel):
    sub_q: SubQuestion
    answer: Optional[str] = None
    sources: TypingList[NodeWithScore] = Field(default_factory=list)


# ==================== Workflow Event Classes ====================
class QueryEvent(StartEvent):
    """Start event carrying the user query string."""
    query: str

    class Config:
        arbitrary_types_allowed = True

    @property
    def is_running(self) -> bool:
        return False


class IntentClassificationEvent(Event):
    """Event carrying the classified query intent."""
    query: str
    original_query: str
    query_intent: str  # "data_lookup", "analytical_question", or "deep_dive_report"


class QueryRewriteEvent(Event):
    """Event carrying the rewritten (expanded) query."""
    rewritten_query: str
    original_query: str
    query_intent: str


class SubQuestionsEvent(Event):
    """Event carrying generated sub-questions and original query."""
    generated_questions_map: List[SubQuestion]
    query: str
    original_query: str
    query_intent: str


class RetrievalEvent(Event):
    """Event carrying retrieved nodes and answers."""
    qa_pair_all: List[SubQuestionAnswerPair]
    original_query: str
    query_intent: str


class ResponseEvent(Event):
    """Event carrying individual question-answer pairs."""
    qa_pair_all: List[SubQuestionAnswerPair]
    original_query: str
    query_intent: str


class DataInventoryEvent(Event):
    """Event carrying a unified data inventory extracted from all retrieved chunks."""
    qa_pair_all: List[SubQuestionAnswerPair]
    original_query: str
    data_inventory: str
    query_intent: str


class ReasoningEvent(Event):
    """Event carrying reasoning scratchpad, cross-table insights, selected templates, and data inventory."""
    qa_pair_all: List[SubQuestionAnswerPair]
    original_query: str
    reasoning_scratchpad: str
    cross_table_insights: str
    selected_template_keys: List[str]
    data_inventory: str
    query_intent: str


# ==================== Analytical Template Library ====================
ANALYTICAL_TEMPLATES = {
    "profitability_analysis": {
        "name": "Profitability Analysis",
        "description": "Applicable when data contains revenue, COGS, margins, or gross/operating profit figures",
        "section": (
            "**Profitability Analysis**\n"
            "Compute and present: gross margin, operating margin, and net margin for each\n"
            "period. Show margin expansion or compression trends with exact basis-point\n"
            "changes. If revenue is not directly available but can be derived (e.g., from\n"
            "COGS as a % of revenue), compute it and show the full calculation. Flag any\n"
            "period where margins deviated significantly from the multi-year trend."
        ),
    },
    "cost_structure_deepdive": {
        "name": "Cost Structure Deep-Dive",
        "description": "Applicable when detailed operating expense line-item breakdowns are present",
        "section": (
            "**Cost Structure Deep-Dive**\n"
            "Rank ALL expense categories by magnitude in the most recent period. Show each\n"
            "as a % of total OpEx AND as a % of revenue. Identify the top 3 fastest-growing\n"
            "categories (absolute $ and %) and the top 3 that declined. Highlight any\n"
            "category that grew faster than revenue."
        ),
    },
    "revenue_quality": {
        "name": "Revenue Quality Assessment",
        "description": "Applicable when revenue or revenue-proxy data exists across multiple periods",
        "section": (
            "**Revenue Quality Assessment**\n"
            "Analyze revenue trajectory: absolute growth, growth rate acceleration /\n"
            "deceleration, and any signs of seasonality or one-time spikes. If revenue is\n"
            "derived, note the margin of error. Assess whether revenue growth is outpacing\n"
            "or lagging cost growth."
        ),
    },
    "anomaly_footnote_analysis": {
        "name": "Anomaly & Footnote Analysis",
        "description": "Applicable when footnotes, annotations [N], or large unexplained line-item swings exist",
        "section": (
            "**Anomaly & Footnote Analysis**\n"
            "For EVERY line item that moved more than 50% or more than $100K period-over-\n"
            "period, cite the exact footnote or qualifier from the document that explains\n"
            "WHY. If no footnote exists, flag it as 'Unexplained — requires management\n"
            "commentary'. Tag each anomaly as 'recurring', 'one-time / non-recurring', or\n"
            "'uncertain'. Quantify the normalized figure after stripping one-time items."
        ),
    },
    "executive_compensation": {
        "name": "Executive Compensation Benchmarking",
        "description": "Applicable when owner or executive compensation data is present",
        "section": (
            "**Executive Compensation Benchmarking**\n"
            "List each named executive / owner, their total compensation for every period,\n"
            "and compute compensation as a % of revenue and as a % of total OpEx. Note if\n"
            "compensation is flat, rising, or declining relative to the business."
        ),
    },
    "working_capital_liquidity": {
        "name": "Working Capital & Liquidity Indicators",
        "description": "Applicable when balance sheet, cash, receivables, payables, or interest expense data is present",
        "section": (
            "**Working Capital & Liquidity Indicators**\n"
            "Extract interest expense trend and compute interest-coverage ratio if\n"
            "operating income or EBITDA can be derived. Flag any liquidity warning signs\n"
            "such as rising interest costs, rent reductions suggesting distress, or large\n"
            "tax accruals."
        ),
    },
    "risk_flags": {
        "name": "Risk Flags & Red-Flag Scan",
        "description": "Always applicable as a final check",
        "section": (
            "**Risk Flags & Red-Flag Scan**\n"
            "List 3-7 concrete, specific risk flags. Each MUST cite a number or trend from\n"
            "the data. Examples: margin compression, revenue decline, single-item expense\n"
            "concentration (>30% of OpEx), disappearing R&D, one-time items recurring, or\n"
            "unusual fee spikes. Rate each risk as Low / Medium / High severity."
        ),
    },
    "contextual_ratios": {
        "name": "Contextual Ratio Computation",
        "description": "Always applicable — compute any meaningful ratios from the available data",
        "section": (
            "**Contextual Ratios & Derived Metrics**\n"
            "IMPORTANT: Use the Data Inventory AND the Pre-Analysis Reasoning as your\n"
            "source of figures — do NOT rely only on raw retrieved text. If a figure\n"
            "appears ANYWHERE in the collected data (research findings, reasoning\n"
            "scratchpad, data inventory, or cross-table analysis), it is available.\n"
            "Only mark a ratio as 'Not available' if NEITHER the numerator NOR the\n"
            "denominator can be found or derived from any source.\n\n"
            "Compute every ratio that is meaningful given the data. MUST include at\n"
            "minimum (where data exists): gross margin, operating margin (derive\n"
            "Operating Income = Revenue - COGS - OpEx if needed — label as [Derived]),\n"
            "OpEx-to-revenue, COGS-to-revenue, each major expense as % of OpEx,\n"
            "each major expense as % of revenue, expense growth vs. revenue growth,\n"
            "compensation-to-revenue, shipping-to-COGS, insurance-to-OpEx.\n"
            "Present ALL ratios in a table with values for each period.\n"
            "Label each metric as [Reported] or [Derived] with the formula shown."
        ),
    },
}


# ==================== Initialize Models ====================
def initialize_models():
    """Initialize LLM and embedding models."""
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise ValueError("OPENAI_API_KEY environment variable is required")

    http_timeout = float(os.getenv("OPENAI_HTTP_TIMEOUT", "300"))
    Settings.llm = OpenAI(
        model="gpt-4o",
        api_key=api_key,
        timeout=http_timeout,
    )
    Settings.embed_model = OpenAIEmbedding(api_key=api_key, timeout=http_timeout)
    logger.info("LLM and embedding models initialized")


def initialize_node_parser():
    """Initialize the node parser."""
    global node_parser
    node_parser = MarkdownElementNodeParser(llm=None, num_workers=8)
    logger.info("Node parser initialized")


# ==================== Workflow Class ====================
class RAGQueryWorkflow(Workflow):
    """Workflow-based query processing pipeline."""

    def __init__(
        self,
        tool_choices: List[ToolMetadata],
        query_engine_tools_map: Dict[str, Any],
        table_query_engine_tools_map: Dict[str, Any],
        **kwargs
    ):
        super().__init__(**kwargs)
        self.tool_choices = tool_choices
        self.query_engine_tools_map = query_engine_tools_map
        self.table_query_engine_tools_map = table_query_engine_tools_map

    @step
    async def classify_intent(self, ev: QueryEvent) -> IntentClassificationEvent:
        """Classify the user query into one of three intents."""
        raw_query = ev.query
        if not raw_query:
            raise ValueError("Query not found in QueryEvent.")

        classification_prompt = PromptTemplate(
            """\
You are a query classifier for a financial document analysis system.

Classify the following user query into exactly ONE of these three categories:

1. **data_lookup** — The user wants to retrieve specific data, tables, or figures
   directly from the document with minimal or no analysis.

2. **analytical_question** — The user is asking a specific analytical question
   that requires reasoning, interpretation, or opinion based on the document data.

3. **deep_dive_report** — The user wants a comprehensive, full-format financial
   analysis or report covering multiple aspects of the data.

User Query: "{query_str}"

Output ONLY one of: data_lookup, analytical_question, deep_dive_report

Classification:"""
        )

        formatted = classification_prompt.format(query_str=raw_query)
        loop = asyncio.get_running_loop()
        llm_response = await loop.run_in_executor(
            None, lambda: Settings.llm.complete(formatted)
        )
        intent = str(llm_response).strip().lower().replace(" ", "_")

        valid_intents = {"data_lookup", "analytical_question", "deep_dive_report"}
        if intent not in valid_intents:
            intent = "analytical_question"

        logger.info(f"Query intent classified as: {intent} for query: '{raw_query[:80]}'")

        return IntentClassificationEvent(
            query=raw_query,
            original_query=raw_query,
            query_intent=intent,
        )

    @step
    async def rewrite_query(self, ev: IntentClassificationEvent) -> QueryRewriteEvent:
        """Rewrite the user query — scope depends on classified intent."""
        raw_query = ev.query
        intent = ev.query_intent

        if intent == "data_lookup":
            rewrite_prompt = PromptTemplate(
                """\
You are a financial data retrieval specialist.

The user asked: "{query_str}"

Rewrite this into a clear, precise data retrieval directive. Stay close to the
original request — do NOT expand it into a broad analysis.

Output ONLY the rewritten directive (one sentence). Do NOT answer the query.

Rewritten directive:"""
            )
        elif intent == "analytical_question":
            rewrite_prompt = PromptTemplate(
                """\
You are a senior financial analyst preparing to answer a specific question.

The user asked: "{query_str}"

Rewrite this into a focused research directive that will gather the data needed
to answer this specific question thoroughly. Include:
1. The core question to be answered
2. What supporting data points, figures, and trends are needed
3. Any comparative or contextual data that would strengthen the analysis

Stay focused on what's needed to answer the question.

Output ONLY the rewritten directive (one paragraph). Do NOT answer the query.

Rewritten directive:"""
            )
        else:  # deep_dive_report
            rewrite_prompt = PromptTemplate(
                """\
You are a senior financial analyst preparing to research a document.

The user asked: "{query_str}"

Rewrite this into a single, detailed research directive that covers ALL of the
following analytical angles:

1. **Exact figures & trends** – Extract every relevant number across all periods.
2. **Derived / computed metrics** – If revenue, margins, growth rates, or ratios
   can be back-calculated, ask for them explicitly.
3. **Anomalies & outliers** – Identify unusually large increases or decreases.
4. **Footnotes, qualifiers & context** – Retrieve ALL footnotes and annotations.
5. **Recurring vs. one-time items** – Separate ongoing from non-recurring charges.
6. **Composition & concentration** – Which categories dominate, how has the mix shifted?

Output ONLY the rewritten research directive (one paragraph). Do NOT answer the query.

Rewritten directive:"""
            )

        formatted = rewrite_prompt.format(query_str=raw_query)
        loop = asyncio.get_running_loop()
        llm_response = await loop.run_in_executor(
            None, lambda: Settings.llm.complete(formatted)
        )
        rewritten = str(llm_response).strip()
        logger.info(f"Query rewrite ({intent}): '{raw_query[:80]}' -> '{rewritten[:120]}...'")

        return QueryRewriteEvent(
            rewritten_query=rewritten,
            original_query=raw_query,
            query_intent=intent,
        )

    @step
    async def generate_subquestions(self, ev: QueryRewriteEvent) -> SubQuestionsEvent:
        """Generate sub-questions from the rewritten query."""
        query_str = ev.rewritten_query
        original_query = ev.original_query
        intent = ev.query_intent

        question_gen = LLMQuestionGenerator.from_defaults(llm=Settings.llm)
        loop = asyncio.get_running_loop()
        choices = await loop.run_in_executor(
            None,
            lambda: question_gen.generate(self.tool_choices, QueryBundle(query_str=query_str))
        )

        choices_dicts = [choice.model_dump() if hasattr(choice, 'model_dump') else dict(choice) for choice in choices]
        choices_objects = [SubQuestion(**d) if isinstance(d, dict) else d for d in choices_dicts]

        if intent == "data_lookup" and len(choices_objects) > 2:
            choices_objects = choices_objects[:2]
        elif intent == "analytical_question" and len(choices_objects) > 3:
            choices_objects = choices_objects[:3]

        return SubQuestionsEvent(
            generated_questions_map=choices_objects,
            query=query_str,
            original_query=original_query,
            query_intent=intent,
        )

    @step
    async def retrieve_dual(self, ev: SubQuestionsEvent) -> RetrievalEvent:
        """Run BOTH combined and table retrievers in parallel for every sub-question."""

        def _retrieve_both(subq):
            qb = QueryBundle(query_str=subq.sub_question)
            combined_nodes = []
            table_nodes = []

            if subq.tool_name in self.query_engine_tools_map:
                try:
                    combined_nodes = self.query_engine_tools_map[subq.tool_name].retrieve(qb)
                except Exception as e:
                    logger.warning(f"Combined retriever failed for {subq.tool_name}: {e}")

            if subq.tool_name in self.table_query_engine_tools_map:
                try:
                    table_nodes = self.table_query_engine_tools_map[subq.tool_name].retrieve(qb)
                except Exception as e:
                    logger.warning(f"Table retriever failed for {subq.tool_name}: {e}")

            seen = {}
            for node in combined_nodes + table_nodes:
                nid = node.node.node_id if hasattr(node, 'node') else id(node)
                existing = seen.get(nid)
                if existing is None:
                    seen[nid] = node
                else:
                    if (node.score or 0) > (existing.score or 0):
                        seen[nid] = node
            merged = sorted(seen.values(), key=lambda n: n.score or 0, reverse=True)

            return SubQuestionAnswerPair(sub_q=subq.model_dump(), sources=merged)

        loop = asyncio.get_running_loop()
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [
                loop.run_in_executor(executor, _retrieve_both, subq)
                for subq in ev.generated_questions_map
            ]
            qa_pair_all = await asyncio.gather(*futures)

        logger.info(f"Dual retrieval complete: {len(qa_pair_all)} sub-questions")

        return RetrievalEvent(
            qa_pair_all=list(qa_pair_all),
            original_query=ev.original_query,
            query_intent=ev.query_intent,
        )

    @step
    async def response(self, ev: RetrievalEvent) -> ResponseEvent:
        """Generate answers for each sub-question."""
        qa_prompt = PromptTemplate(
            """\
You are a meticulous financial analyst. Use ONLY the context below to answer.

Context (tables, text, and footnotes from the source document):
---------------------
{context_str}
---------------------

Instructions:
1. Extract every relevant number (absolute values AND percentages) for ALL
   available periods and state the direction of change.
2. If the context contains footnotes or annotations, quote them verbatim and
   tie each one to the specific line item it explains.
3. Clearly label any item that is one-time, non-recurring, or unusual.
4. If you can back-calculate a metric, show the calculation step.
5. If the context does not contain enough information, say so explicitly.

Query: {query_str}
Answer: \
"""
        )

        def answer_single(qa_pair):
            context_str = "\n\n".join([r.get_content() for r in qa_pair.sources])
            fmt = qa_prompt.format(context_str=context_str, query_str=qa_pair.sub_q.sub_question)
            response = Settings.llm.complete(fmt)
            return SubQuestionAnswerPair(sub_q=qa_pair.sub_q.model_dump(), answer=str(response), sources=qa_pair.sources)

        loop = asyncio.get_running_loop()
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = {
                loop.run_in_executor(executor, answer_single, qa_pair)
                for qa_pair in ev.qa_pair_all
            }
            qa_pair_all = await asyncio.gather(*futures)

        return ResponseEvent(
            qa_pair_all=list(qa_pair_all),
            original_query=ev.original_query,
            query_intent=ev.query_intent,
        )

    @step
    async def build_data_inventory(self, ev: ResponseEvent) -> DataInventoryEvent:
        """Extract a unified data inventory from ALL retrieved source chunks."""
        intent = ev.query_intent

        if intent == "data_lookup":
            return DataInventoryEvent(
                qa_pair_all=ev.qa_pair_all,
                original_query=ev.original_query,
                data_inventory="",
                query_intent=intent,
            )

        all_source_texts = []
        seen_ids = set()
        for qa in ev.qa_pair_all:
            for src in qa.sources:
                src_id = src.node.node_id if hasattr(src, 'node') else id(src)
                if src_id not in seen_ids:
                    seen_ids.add(src_id)
                    all_source_texts.append(src.get_content())

        raw_data = "\n\n---\n\n".join(all_source_texts)
        answer_digest = "\n\n".join(
            f"[Sub-Q] {qa.sub_q.sub_question}\nAnswer: {(qa.answer or '').strip()}"
            for qa in ev.qa_pair_all
        )

        inventory_prompt = PromptTemplate(
            """\
You are a meticulous data analyst. Below are ALL retrieved source chunks from a
financial document, plus the answers generated from them.

Source Chunks:
---------------------
{raw_data}
---------------------

Sub-Question Answers:
---------------------
{answer_digest}
---------------------

Your task: produce a COMPLETE DATA INVENTORY — a structured, exhaustive list of
every single data point found across ALL chunks.

Format:

**TABLES FOUND**
Reproduce each table in full markdown — every row, every column, every period.

**LINE ITEMS WITH VALUES**
- [Line Item Name]: Period1=Value, Period2=Value, ...

**FOOTNOTES & ANNOTATIONS**
- [N] Full footnote text

**DERIVED / BACK-CALCULATED FIGURES**
- [Metric]: Formula -> Result for each period

CRITICAL RULES:
- Include EVERY row from EVERY table. Do NOT skip items.
- Preserve exact numerical values — do not round.

Data Inventory:
"""
        )

        formatted = inventory_prompt.format(raw_data=raw_data, answer_digest=answer_digest)
        loop = asyncio.get_running_loop()
        inv_resp = await loop.run_in_executor(None, lambda: Settings.llm.complete(formatted))
        data_inventory = str(inv_resp).strip()
        logger.info(f"Data inventory built ({len(data_inventory)} chars)")

        return DataInventoryEvent(
            qa_pair_all=ev.qa_pair_all,
            original_query=ev.original_query,
            data_inventory=data_inventory,
            query_intent=intent,
        )

    @step
    async def reasoning_and_cross_reference(self, ev: DataInventoryEvent) -> ReasoningEvent:
        """Reasoning scratchpad, cross-table insights, and template selection."""
        intent = ev.query_intent

        if intent == "data_lookup":
            return ReasoningEvent(
                qa_pair_all=ev.qa_pair_all,
                original_query=ev.original_query,
                reasoning_scratchpad="",
                cross_table_insights="",
                selected_template_keys=[],
                data_inventory=ev.data_inventory,
                query_intent=intent,
            )

        digest_parts = []
        for i, qa in enumerate(ev.qa_pair_all, 1):
            answer_text = (qa.answer or "No answer").strip()
            digest_parts.append(f"[Sub-Q {i}] {qa.sub_q.sub_question}\nAnswer: {answer_text}")
        full_digest = "\n\n".join(digest_parts)

        # 1. Reasoning scratchpad
        reasoning_prompt = PromptTemplate(
            """\
You are a senior financial analyst preparing to write a research note.

Research Findings:
---------------------
{context_str}
---------------------

Complete Data Inventory:
---------------------
{data_inventory}
---------------------

Original Query: {query_str}

Produce a structured reasoning scratchpad:
1. **Derived Metrics**: Compute EVERY possible ratio, margin, growth rate.
2. **Non-Obvious Insights**: Patterns, anomalies, or relationships the data reveals.
3. **Data Gaps**: What important figures are truly missing?

Output ONLY the scratchpad. Be specific — cite exact numbers.

Reasoning Scratchpad:
"""
        )

        formatted_reasoning = reasoning_prompt.format(
            context_str=full_digest,
            data_inventory=ev.data_inventory,
            query_str=ev.original_query,
        )

        loop = asyncio.get_running_loop()
        reasoning_resp = await loop.run_in_executor(
            None, lambda: Settings.llm.complete(formatted_reasoning)
        )
        reasoning_scratchpad = str(reasoning_resp).strip()

        # 2. Cross-table reasoning
        cross_ref_prompt = PromptTemplate(
            """\
You are a financial analyst looking for cross-references and hidden relationships.

Sub-Question Answers:
---------------------
{context_str}
---------------------

Complete Data Inventory:
---------------------
{data_inventory}
---------------------

Examine ALL data together and produce a concise list of:
1. **Contradictions** — numbers that conflict across answers or tables.
2. **Corroborations** — numbers confirmed by multiple sources.
3. **Cross-table relationships** — insights from combining different tables.
4. **Trend confirmations / reversals** — multi-period patterns.
5. **Completeness check** — any line items NOT addressed in any answer.

Be specific. Cite exact numbers. Output a bullet list.

Cross-Reference Analysis:
"""
        )

        formatted_cross = cross_ref_prompt.format(
            context_str=full_digest,
            data_inventory=ev.data_inventory,
        )
        cross_resp = await loop.run_in_executor(
            None, lambda: Settings.llm.complete(formatted_cross)
        )
        cross_table_insights = str(cross_resp).strip()

        if intent == "analytical_question":
            return ReasoningEvent(
                qa_pair_all=ev.qa_pair_all,
                original_query=ev.original_query,
                reasoning_scratchpad=reasoning_scratchpad,
                cross_table_insights=cross_table_insights,
                selected_template_keys=[],
                data_inventory=ev.data_inventory,
                query_intent=intent,
            )

        # 3. Template selection for deep_dive_report
        template_descriptions = "\n".join(
            f"- {key}: {tmpl['description']}"
            for key, tmpl in ANALYTICAL_TEMPLATES.items()
        )
        selection_prompt = PromptTemplate(
            """\
Given the following research findings and the original query, select the 2-4
MOST relevant analytical templates.

Research Findings (summary):
---------------------
{context_str}
---------------------

Original Query: {query_str}

Available Templates:
{template_list}

Output ONLY a comma-separated list of template keys. No explanation.

Selected templates:
"""
        )

        formatted_selection = selection_prompt.format(
            context_str=full_digest[:3000],
            query_str=ev.original_query,
            template_list=template_descriptions,
        )
        selection_resp = await loop.run_in_executor(
            None, lambda: Settings.llm.complete(formatted_selection)
        )
        raw_keys = str(selection_resp).strip().strip(".")
        selected_keys = [
            k.strip() for k in raw_keys.split(",")
            if k.strip() in ANALYTICAL_TEMPLATES
        ]
        for must_have in ("risk_flags", "contextual_ratios"):
            if must_have not in selected_keys:
                selected_keys.append(must_have)

        return ReasoningEvent(
            qa_pair_all=ev.qa_pair_all,
            original_query=ev.original_query,
            reasoning_scratchpad=reasoning_scratchpad,
            cross_table_insights=cross_table_insights,
            selected_template_keys=selected_keys,
            data_inventory=ev.data_inventory,
            query_intent=intent,
        )

    @step
    async def response_synthesizer(self, ev: ReasoningEvent) -> StopEvent:
        """Synthesize final response with intent-tailored prompt."""
        intent = ev.query_intent

        qa_pairs_all = cast(TypingList[TypingOptional[SubQuestionAnswerPair]], ev.qa_pair_all)
        qa_pairs: TypingList[SubQuestionAnswerPair] = list(filter(None, qa_pairs_all))

        def _construct_node(qa_pair: SubQuestionAnswerPair) -> NodeWithScore:
            node_text = f"Sub question: {qa_pair.sub_q.sub_question}\nResponse: {qa_pair.answer}"
            return NodeWithScore(node=TextNode(text=node_text))

        nodes = [_construct_node(pair) for pair in qa_pairs]
        source_nodes = [node for qa_pair in qa_pairs for node in qa_pair.sources]
        context_parts = [node.node.text for node in nodes]
        context_str = "\n\n".join(context_parts)

        if intent == "data_lookup":
            raw_source_parts = []
            seen_ids = set()
            for qa in qa_pairs:
                for src in qa.sources:
                    src_id = src.node.node_id if hasattr(src, 'node') else id(src)
                    if src_id not in seen_ids:
                        seen_ids.add(src_id)
                        raw_source_parts.append(src.get_content())
            raw_source_str = "\n\n---\n\n".join(raw_source_parts)

        if intent == "data_lookup":
            synthesis_prompt = PromptTemplate(
                """\
You are a helpful financial data assistant. The user has asked for specific data.

Research findings:
---------------------
{context_str}
---------------------

Raw source content:
---------------------
{raw_source}
---------------------

User's Question: {query_str}

Instructions:
- Return the requested data EXACTLY as found in the source document.
- Preserve original table formatting (use Markdown tables).
- If the user asked for a specific table, reproduce it in full.
- Add NO commentary or analysis unless explicitly asked for.

Response:
"""
            )
            final_prompt_text = synthesis_prompt.format(
                context_str=context_str,
                raw_source=raw_source_str,
                query_str=ev.original_query,
            )

        elif intent == "analytical_question":
            synthesis_prompt = PromptTemplate(
                """\
You are a senior financial analyst having a conversation with a client.

Research findings:
---------------------
{context_str}
---------------------

Complete Data Inventory:
---------------------
{data_inventory}
---------------------

Pre-Analysis Reasoning:
---------------------
{reasoning_scratchpad}
---------------------

Cross-Table Analysis:
---------------------
{cross_table_insights}
---------------------

Client's Question: {query_str}

Instructions:
1. Lead with your direct answer or conclusion.
2. Support with specific figures, trends, or data points. Cite exact numbers.
3. Structure your response naturally based on what the question demands.
4. If the question involves judgment, present both supporting and opposing evidence.
5. Keep the tone conversational but professional.
6. Do NOT include boilerplate sections unless the user asked for them.
7. Format dollar amounts with appropriate units and percentages to one decimal place.

Response:
"""
            )
            final_prompt_text = synthesis_prompt.format(
                context_str=context_str,
                data_inventory=ev.data_inventory,
                reasoning_scratchpad=ev.reasoning_scratchpad,
                cross_table_insights=ev.cross_table_insights,
                query_str=ev.original_query,
            )

        else:  # deep_dive_report
            template_sections = []
            for key in ev.selected_template_keys:
                tmpl = ANALYTICAL_TEMPLATES.get(key)
                if tmpl:
                    template_sections.append(tmpl["section"])
            dynamic_sections = "\n\n".join(template_sections)

            synthesis_prompt = PromptTemplate(
                """\
You are a senior financial analyst writing a research note for investment professionals.

Research findings:
---------------------
{context_str}
---------------------

Complete Data Inventory:
---------------------
{data_inventory}
---------------------

Pre-Analysis Reasoning:
---------------------
{reasoning_scratchpad}
---------------------

Cross-Table Analysis:
---------------------
{cross_table_insights}
---------------------

Original Query: {query_str}

Produce a professional, data-dense report using the following structure:

**Executive Summary**
2-3 sentences capturing the most important story the data tells.

**Key Metrics & Period-over-Period Changes**
Present a COMPLETE table with EVERY line item, its value for EACH period, and
the YoY change ($ and %). Do NOT omit line items.

{dynamic_sections}

**Analyst Observations & Actionable Takeaways**
List 3-7 concrete observations. Each must reference a specific number or trend.

**Data Coverage Audit**
List any line items from the Data Inventory NOT discussed above.

CRITICAL RULES:
1. Treat the ENTIRE document as ONE interconnected dataset.
2. Use the Data Inventory as your primary source of truth.
3. Only mark something as "Not available" if it cannot be found OR derived.
4. When a metric can be partially derived, compute it and label as [Derived].
5. Format all dollar amounts with appropriate units and percentages to one decimal.

Report:
"""
            )
            final_prompt_text = synthesis_prompt.format(
                context_str=context_str,
                data_inventory=ev.data_inventory,
                reasoning_scratchpad=ev.reasoning_scratchpad,
                cross_table_insights=ev.cross_table_insights,
                query_str=ev.original_query,
                dynamic_sections=dynamic_sections,
            )

        logger.info(f"Synthesizing response (intent={intent}, prompt_len={len(final_prompt_text)})")
        loop = asyncio.get_running_loop()
        response = await loop.run_in_executor(
            None, lambda: Settings.llm.complete(final_prompt_text)
        )

        class _SynthesisResponse:
            def __init__(self, text: str, source_nodes):
                self.response = text
                self.source_nodes = source_nodes
            def __str__(self):
                return self.response

        final_response = _SynthesisResponse(
            text=str(response),
            source_nodes=source_nodes,
        )

        return StopEvent(result=final_response)


def create_query_pipeline(tool_choices, query_engine_tools_map, table_query_engine_tools_map):
    """Create a Workflow-based query pipeline."""
    workflow = RAGQueryWorkflow(
        tool_choices=tool_choices,
        query_engine_tools_map=query_engine_tools_map,
        table_query_engine_tools_map=table_query_engine_tools_map,
        verbose=False,
        timeout=1800,
    )
    return workflow
