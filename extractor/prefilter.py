# Enhanced prefilter_rmd_statement_v2.py
# Multi-page table aware version - handles tables spanning multiple pages
# Does not require headers on same page as table data
# Self-contained for router integration

import re
from dataclasses import dataclass
from typing import Dict, Any, List, Optional, Tuple

FILTER_VERSION = "multi_page_v3_lenient_1row_2025-02-19"

@dataclass
class PreFilterConfig:
    # Table detection (lenient: catch 1–2 row tables; avoid skipping pages with tables)
    min_pipe_rows: int = 1  # Accept single-row pipe tables (with 2+ pipes to avoid noise)
    min_space_rows: int = 1  # Accept single-row space-aligned tables
    min_space_cols: int = 2
    context_lines_before_table: int = 20  # Increased to catch headers from previous context
    table_scan_rows: int = 100  # Increased to scan more of the page

CFG = PreFilterConfig()

def normalize_text(text: str) -> str:
    if not text:
        return ""
    text = text.replace("\u2013", "-").replace("\u2014", "-").replace("\u2212", "-")
    text = re.sub(r"(\w)-\s*\n\s*(\w)", r"\1\2", text)
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{2,}", "\n", text)
    return text.strip()

PIPE_CHARS = r"[|\u2502\u2503\u2506]"
PIPE_ROW = re.compile(rf"^\s*{PIPE_CHARS}.*$", re.UNICODE)
PIPE_SEP = re.compile(
    rf"^\s*{PIPE_CHARS}?\s*:?-{{3,}}:?(?:\s*{PIPE_CHARS}\s*:?-{{3,}}:?)*\s*{PIPE_CHARS}?\s*$",
    re.UNICODE
)
HTML_TABLE = re.compile(r"<\s*(table|thead|tbody|tr|td)\b", re.I)
SPACE_COLS = re.compile(r"\S.{0,100}?\s{{2,}}\S", re.UNICODE)

@dataclass
class TableHit:
    found: bool
    kind: Optional[str] = None
    start_idx: Optional[int] = None
    end_idx: Optional[int] = None
    rows: int = 0
    source: str = "raw"

def _count_pipes(line: str) -> int:
    """Count pipe-like characters in a line (used to avoid single-pipe false positives)."""
    return sum(1 for c in line if c in "|\u2502\u2503\u2506")

def _detect_pipe_blocks(lines: List[str], min_rows: int) -> List[TableHit]:
    hits: List[TableHit] = []
    i, n = 0, len(lines)
    while i < n:
        if PIPE_ROW.match(lines[i]) or PIPE_SEP.match(lines[i]):
            start, rows = i, 0
            block_lines: List[str] = []
            while i < n and (PIPE_ROW.match(lines[i]) or PIPE_SEP.match(lines[i])):
                block_lines.append(lines[i])
                rows += 1
                i += 1
            # For single-row blocks, require at least 2 pipes (real table has columns) to avoid false positives
            if rows >= min_rows:
                if rows == 1 and _count_pipes(block_lines[0]) < 2:
                    pass  # skip single line with only one pipe
                else:
                    hits.append(TableHit(True, "pipe", start, i - 1, rows))
            continue
        i += 1
    return hits

def _detect_html_table(lines: List[str]) -> Optional[TableHit]:
    full = "\n".join(lines)
    if HTML_TABLE.search(full):
        for idx, ln in enumerate(lines):
            if HTML_TABLE.search(ln):
                return TableHit(True, "html", idx, None, rows=99999)
    return None

def _detect_space_table(lines: List[str], min_rows: int, min_cols: int) -> Optional[TableHit]:
    run_start, run_len = None, 0
    for idx, ln in enumerate(lines):
        if SPACE_COLS.search(ln):
            parts = [p for p in re.split(r"\s{2,}", ln.strip()) if p]
            if len(parts) >= min_cols:
                if run_start is None:
                    run_start = idx
                run_len += 1
                if run_len >= min_rows:
                    return TableHit(True, "space", run_start, idx, rows=run_len)
                continue
        run_start, run_len = None, 0
    return None

def detect_table_best(lines: List[str], source: str, cfg: PreFilterConfig) -> Optional[TableHit]:
    candidates: List[TableHit] = []
    candidates += _detect_pipe_blocks(lines, cfg.min_pipe_rows)
    sp = _detect_space_table(lines, cfg.min_space_rows, cfg.min_space_cols)
    if sp: candidates.append(sp)
    html = _detect_html_table(lines)
    if html: candidates.append(html)
    if not candidates:
        return None
    best = max(candidates, key=lambda t: t.rows)
    best.source = source
    return best

# Header patterns (same as before)
HEADER_IS = re.compile(r"""(?isx)
    \b
    (?:consolidated\s+|condensed\s+|interim\s+|unaudited\s+)?
    (?:
        statements?\s+of\s+
            (?: (?:consolidated\s+)? (?:operations?|income|earnings|profit\s*(?:&|and)?\s*loss|profit\s*or\s*loss) )
      | income\s+statements?
      | statement\s+of\s+earnings
      | p\s*&?\s*l
      | profit\s*(?:&|and)?\s*loss\s+statements?
      | statements?\s+of\s+comprehensive\s+income
      | statement\s+of\s+comprehensive\s+income
      | consolidated\s+results\s+of\s+operations
    )
    \b
""")

HEADER_BS = re.compile(r"""(?isx)
    \b
    (?:consolidated\s+|condensed\s+|interim\s+|unaudited\s+)?
    (?:
        balance\s+sheets?
      | statements?\s+of\s+(?: (?:consolidated\s+)? (?:financial\s+position|financial\s+condition) )
      | consolidated\s+financial\s+position
    )
    \b
""")

HEADER_CF = re.compile(r"""(?isx)
    \b
    (?:consolidated\s+|condensed\s+|interim\s+|unaudited\s+)?
    (?:
        statements?\s+of\s+(?: (?:consolidated\s+)? cash\s+flows? )
      | statement\s+of\s+(?: (?:consolidated\s+)? cash\s+flows? )
      | (?:consolidated\s+)?cash\s+flows?\s+statements?
      | (?:consolidated\s+)?cash\s*flow\s+statements?
      | (?:consolidated\s+)?cashflow\s+statements?
    )
    \b
""")

def earliest_target_type(text: str) -> Tuple[Optional[str], Optional[Tuple[int, int, str]]]:
    hits: List[Tuple[int, str, re.Match]] = []
    for typ, pat in (("balance_sheet", HEADER_BS),
                     ("income_statement", HEADER_IS),
                     ("cash_flow", HEADER_CF)):
        m = pat.search(text)
        if m:
            hits.append((m.start(), typ, m))
    if not hits:
        return None, None
    hits.sort(key=lambda x: x[0])
    best = hits[0]
    start, typ, m = best
    excerpt = text[max(0, m.start()-40): m.end()+40]
    return typ, (m.start(), m.end(), excerpt)

# Financial line item patterns (to detect continuation pages)
FINANCIAL_KEYWORDS = re.compile(r"""(?isx)
    \b(?:cash|revenue|income|expense|assets?|liabilities?|equity|debt|sales|cost|profit|loss|earnings|depreciation|amortization|accounts\s+(?:receivable|payable)|inventory|property|plant|equipment|operating|investing|financing|activities?)\b
""")

CONTINUATION_MARKERS = re.compile(r"""(?isx)
    \b(?:continued|continuation|\(continued\)|\(cont\.\))\b
""")

def check_false_positive_indicators(full_text: str, scan_text: str) -> Optional[str]:
    """CONSERVATIVE false positive detection."""
    full_lower = full_text.lower()

    percentage_phrases = [
        "expressed as a percentage of net revenue",
        "expressed as a percentage of revenue",
        "expressed as a percentage of net sales",
        "as a percentage of net revenue",
        "as a percentage of revenue"
    ]

    explicit_percentage = any(phrase in full_lower for phrase in percentage_phrases)

    derived_phrases = [
        "information derived from our consolidated statements of operations",
        "derived from our consolidated statements of operations",
        "the following table sets forth information derived from our consolidated"
    ]

    explicit_derived = any(phrase in full_lower for phrase in derived_phrases)

    context_top = full_text[:500].lower()
    md_a_section = "results of operations" in context_top

    if explicit_percentage and (explicit_derived or md_a_section):
        return "confirmed_percentage_analysis_table"

    if ("results of operations" in context_top and
    re.search(r"sets forth.*(?:consolidated\s+)?statements\s+of\s+operations", full_lower)):
        return "md_a_results_section"

    if (re.search(r"\bthe following tables?\s+sets?\s+forth", full_lower) and
        re.search(r"(?:percentage|derived|summary)", full_lower)):
        return "md&a_caption_table"

    if re.search(r"cash\s+flows?.*were\s+as\s+follows", full_lower):
        return "cashflow_summary_caption"

    return None

def prefilter_statement_page_from_rmd(
    rmd_page_text: str,
    cfg: PreFilterConfig = CFG,
    previous_page_context: Optional[str] = None
) -> Dict[str, Any]:
    """
    Enhanced prefilter that handles multi-page tables.
    - More lenient table detection (catches partial tables)
    - Checks for continuation markers
    - Uses previous page context to find headers
    - Accepts pages with tables even if header is on previous page
    """
    if not rmd_page_text or len(rmd_page_text.strip()) < 30:
        return {"pass": False, "type": "neither", "reason": "empty_or_short",
                "debug": {"version": FILTER_VERSION}}

    # Table detection
    raw_lines = rmd_page_text.splitlines()
    norm_lines = normalize_text(rmd_page_text).splitlines()

    hit = detect_table_best(raw_lines, "raw", cfg)
    if not hit:
        hit = detect_table_best(norm_lines, "norm", cfg)
    if not hit:
        # Check if this might be a continuation page (has continuation markers)
        if previous_page_context and CONTINUATION_MARKERS.search(rmd_page_text):
            # Check if previous page had a table
            prev_hit = detect_table_best(previous_page_context.splitlines(), "raw", cfg)
            if prev_hit:
                return {"pass": True, "type": "unknown", "reason": "continuation_page",
                        "debug": {"version": FILTER_VERSION, "table_kind": "continuation"}}
        return {"pass": False, "type": "neither", "reason": "no_table",
                "debug": {"version": FILTER_VERSION}}

    # Build scan context (includes previous page context if available)
    lines = raw_lines if hit.source == "raw" else norm_lines
    start = hit.start_idx or 0
    end = hit.end_idx if hit.end_idx is not None else min(len(lines)-1, start + cfg.table_scan_rows - 1)
    before_start = max(0, start - cfg.context_lines_before_table)

    heading_lines = [ln for ln in lines if ln.lstrip().startswith("#")]
    context_before = lines[before_start:start]
    table_top = lines[start: min(len(lines), start + cfg.table_scan_rows)]

    def strip_pipes(block: List[str]) -> List[str]:
        return [re.sub(r"^\s*[|\u2502\u2503\u2506]\s*", "", ln).rstrip("| ").strip() for ln in block]

    scan_text = "\n".join(strip_pipes(heading_lines) + strip_pipes(context_before) + strip_pipes(table_top))

    # Include previous page context in search if available
    full_scan_text = scan_text
    if previous_page_context:
        # Add last 200 chars of previous page to catch headers
        prev_snippet = previous_page_context[-200:] if len(previous_page_context) > 200 else previous_page_context
        full_scan_text = prev_snippet + "\n" + scan_text

    # Check for false positives
    false_positive_reason = check_false_positive_indicators(rmd_page_text, scan_text)
    if false_positive_reason:
        return {"pass": False, "type": "neither", "reason": false_positive_reason,
                "debug": {"version": FILTER_VERSION}}

    # Header detection (check both current page and previous context).
    # NOTE: From here on, ANY detected table page passes unless it is an explicit false positive above.
    stmt_type, match_info = earliest_target_type(full_scan_text)

    # Classify based on headers / financial content, but do not reject tables anymore.
    if stmt_type:
        dbg = {
            "version": FILTER_VERSION,
            "table_kind": hit.kind,
            "table_source": hit.source,
            "table_start_line": start,
            "table_end_line": end,
        }
        if match_info:
            s, e, ex = match_info
            dbg.update({"match_span": [s, e], "match_excerpt": ex[:200]})
        return {
            "pass": True,
            "type": stmt_type,
            "reason": "header_match_in_table_context",
            "debug": dbg,
        }

    # No header match – still accept the table, but annotate with financial content strength.
    financial_matches = len(FINANCIAL_KEYWORDS.findall(scan_text))
    continuation_marker = CONTINUATION_MARKERS.search(rmd_page_text)
    min_keywords_for_small = 1 if hit.rows <= 2 else 2

    if financial_matches >= min_keywords_for_small or continuation_marker:
        return {
            "pass": True,
            "type": "unknown",
            "reason": "table_with_financial_content",
            "debug": {
                "version": FILTER_VERSION,
                "table_kind": hit.kind,
                "financial_matches": financial_matches,
            },
        }

    # Fallback: generic table without strong financial signal, but still a table page.
    return {
        "pass": True,
        "type": "unknown",
        "reason": "generic_table_page",
        "debug": {
            "version": FILTER_VERSION,
            "table_kind": hit.kind,
            "table_source": hit.source,
            "table_start_line": start,
            "table_end_line": end,
            "financial_matches": financial_matches,
        },
    }
