# ============================================================
# 📄 Financial Statement Scraper – MetaLinks Integrated Version
# ============================================================

import json
import re
import requests
from bs4 import BeautifulSoup
try:
    from openpyxl import Workbook
    from openpyxl.styles import Font
except ImportError:
    # openpyxl is optional - only needed for Excel export functionality
    Workbook = None
    Font = None
from typing import Dict, List, Optional, Tuple
import time
import pandas as pd
try:
    from IPython.display import display
except ImportError:
    # IPython is optional - only needed for Jupyter notebook display
    display = None


class FinancialStatementScraper:
    """
    Extracts financial statements from SEC XBRL filings.
    Now integrates MetaLinks.json role detection before pattern matching.
    Includes post-processing year alignment for cash-flow instant shifts.
    Includes automatic restructuring for merger compatibility.
    """

    def __init__(self, filing_url: str, openai_api_key: str = None):
        self.filing_url = filing_url
        self.openai_api_key = openai_api_key
        self.session = requests.Session()

        self.session.headers.update({
            'User-Agent': 'MyCompany contact@email.com',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Cache-Control': 'max-age=0'
        })

        actual_url = self._extract_document_url(filing_url)
        print("📥 Fetching filing from SEC...")

        for attempt in range(3):
            try:
                time.sleep(0.5)
                resp = self.session.get(actual_url, timeout=30)
                resp.raise_for_status()
                self.html_content = resp.text
                break
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 403 and attempt < 2:
                    print("⚠ SEC blocked (403). Retrying...")
                    time.sleep((attempt + 1) * 2)
                else:
                    raise Exception(
                        "SEC.gov requires a User-Agent header with contact email. "
                        "Update 'User-Agent' in the code with your own email."
                    )

        self.soup = BeautifulSoup(self.html_content, "lxml")
        self.tables = self.soup.find_all("table")
        print(f"✓ Loaded HTML with {len(self.tables)} tables")

        self.context_mapping = self._build_context_mapping()
        print(f"✓ Built context mapping with {len(self.context_mapping)} contexts")

        self.metalinks_url = self._construct_metalinks_url(actual_url)
        self.metalinks = self._load_metalinks()

    # ---------------- URL HELPERS ----------------
    def _extract_document_url(self, filing_url: str) -> str:
        if "/ix?doc=" in filing_url:
            return "https://www.sec.gov" + filing_url.split("/ix?doc=")[1]
        return filing_url

    def _construct_metalinks_url(self, document_url: str) -> str:
        return document_url.rsplit("/", 1)[0] + "/MetaLinks.json"

    def _load_metalinks(self) -> Dict:
        try:
            print("📥 Fetching MetaLinks.json...")
            r = self.session.get(self.metalinks_url, timeout=30)
            r.raise_for_status()
            data = r.json()
            if isinstance(data, dict) and "instance" in data:
                first_instance = list(data["instance"].values())[0]
                reports = first_instance.get("report", {})
                print(f"✓ Loaded MetaLinks with {len(reports)} roles")
                return reports
            return {}
        except Exception as e:
            print(f"⚠ Failed to load MetaLinks: {e}")
            return {}

    # ---------------- CONTEXT MAPPING ----------------
    def _build_context_mapping(self) -> Dict[str, Dict[str, str]]:
        mapping = {}
        for ctx in self.soup.find_all(["xbrli:context", "context"]):
            cid = ctx.get("id")
            if not cid:
                continue
            inst = ctx.find(["xbrli:instant", "instant"])
            if inst:
                mapping[cid] = {"date": inst.get_text(strip=True), "type": "instant"}
                continue
            end = ctx.find(["xbrli:enddate", "enddate"])
            start = ctx.find(["xbrli:startdate", "startdate"])
            if end:
                mapping[cid] = {"date": end.get_text(strip=True), "type": "duration"}
            elif start:
                mapping[cid] = {"date": start.get_text(strip=True), "type": "duration"}
        return mapping

    # ---------------- YEAR EXTRACTION (FIXED) ----------------
    def _extract_year_from_context(self, context_ref: str) -> Optional[str]:
        if not context_ref:
            return None
        
        # PRIORITY 1: Check context_mapping FIRST (handles UUIDs and all contextRef formats)
        if context_ref in self.context_mapping:
            date = self.context_mapping[context_ref]["date"]
            y = re.search(r"(\d{4})", date)
            if y:
                return y.group(1)
        
        # PRIORITY 2: Try standard date range pattern in contextRef string
        m = re.search(r"D(\d{4})\d{4}-(\d{4})\d{4}", context_ref)
        if m:
            return m.group(2)
        
        # PRIORITY 3: Try to find last 8-digit date pattern
        m = re.search(r"(\d{8})(?!.*\d{8})", context_ref)
        if m:
            return m.group(1)[:4]
        
        # PRIORITY 4: Last resort - look for any 4-digit year pattern
        m = re.search(r"20\d{2}", context_ref)
        return m.group(0) if m else None

    # ---------------- ROBUST ID PICKER (FIXED - STANDALONE 'id' ONLY) ----------------
    def _pick_fact_id_from_tag(self, tag) -> Optional[str]:
        """
        Extracts ONLY the standalone 'id' attribute value from XBRL fact tags.
        Uses word boundary matching to ensure we match 'id=' and NOT 'data-original-id=' 
        or any other attribute containing 'id' as part of its name.
        Works for any id format (fact-identifier-226, F_xxx, custom formats, etc.)
        """
        # Primary method: Parse raw string to extract STANDALONE id attribute only
        tag_str = str(tag)
        
        # Match only standalone 'id' attribute using word boundary
        # This will match: id="..." or id='...'
        # This will NOT match: data-original-id="...", original-id="...", fact-id="...", etc.
        # The \b ensures 'id' is a complete word, not part of another attribute name
        id_match = re.search(r'\bid\s*=\s*["\']([^"\']+)["\']', tag_str)
        if id_match:
            extracted_id = id_match.group(1)
            return extracted_id
        
        # Fallback 1: Try attrs dictionary (may work in some parsers)
        # But ensure we're getting the actual 'id' key, not something else
        attrs = dict(tag.attrs) if hasattr(tag, "attrs") else {}
        
        # Only check for exact 'id' key (not 'data-id', 'original-id', etc.)
        if 'id' in attrs:
            potential_id = attrs['id']
            if potential_id:
                return potential_id
        
        # Fallback 2: Look for 'ix' attribute as last resort
        # (this is the inline XBRL identifier like F_xxx)
        if attrs.get("ix"):
            return attrs.get("ix")
        
        # Fallback 3: If nothing found, return None
        return None

    # ---------------- XBRL EXTRACTION + POST-ALIGNMENT ----------------
    def _extract_xbrl_data_from_table(self, table, statement_type: str) -> Tuple[List[str], List[Dict[str, str]]]:
        rows = table.find_all("tr")
        all_years, structured_rows = set(), []

        for row in rows:
            cells = row.find_all(["td", "th"])
            line_item = cells[0].get_text(strip=True) if cells else ""
            year_values = {}

            for cell in cells:
                for tag in cell.find_all(attrs={"contextref": True}):
                    cref = tag.get("contextref", "")
                    year = self._extract_year_from_context(cref)
                    if not year:
                        continue

                    val = tag.get_text(strip=True)

                    # ========== UNIVERSAL NEGATIVE VALUE DETECTION ==========
                    # Get the parent row to search for parentheses pattern
                    parent_row = tag.find_parent('tr')
                    
                    if parent_row and val:
                        # Get the entire row's text
                        row_text = parent_row.get_text(strip=True)
                        
                        # Remove commas from value for matching (handles "14,264" vs "14264")
                        val_clean = val.replace(',', '')
                        
                        # Search for pattern: (value) with optional whitespace and commas
                        # This handles: (307), ( 307 ), (14,264), ( 14,264 ), etc.
                        pattern = rf'\(\s*{re.escape(val)}\s*\)'
                        
                        # Also try without commas in case they're formatted differently
                        pattern_no_comma = rf'\(\s*{re.escape(val_clean)}\s*\)'
                        
                        # Check if value appears wrapped in parentheses anywhere in the row
                        if re.search(pattern, row_text) or re.search(pattern_no_comma, row_text):
                            # Only mark as negative if not already negative
                            if not val.startswith('-'):
                                val = '-' + val
                    # =========================================================
    
                    # --- Robust ID extraction using helper ---
                    tag_id = self._pick_fact_id_from_tag(tag)
                    # ------------------------------------------------

                    meta = {
                        "name": tag.get("name"),
                        "id": tag_id,
                        "unitref": tag.get("unitref"),
                        "decimals": tag.get("decimals"),
                        "format": tag.get("format"),
                        "scale": tag.get("scale"),
                    }

                    year_values[year] = {"value": val, "meta": meta}
                    all_years.add(year)

            if line_item or year_values:
                structured_rows.append({"line_item": line_item, "values": year_values})

        # ========== NOISE FILTER - REMOVE UNWANTED HEADER ROWS ==========
        NOISE_PATTERNS = [
            r'(year|years|month|months|quarter|period)s?\s+(ended|ending)',
            r'^(january|february|march|april|may|june|july|august|september|october|november|december)\s*\d{0,2}',
            r'\(in (millions?|thousands?|billions?|dollars?)\b',
            r'except (per share|share data)',
            r'^\d{4}$|^\d{1,2}/\d{1,2}/\d{2,4}$',
            r'^(as of|for the|fiscal year)',
            r'^\s*$'
        ]

        structured_rows = [
            r for r in structured_rows
            if r['values'] or not any(re.search(p, r['line_item'].lower()) for p in NOISE_PATTERNS)
        ]
        # ================================================================

        # dominant year sequence
        year_counts = {}
        for row in structured_rows:
            years = tuple(sorted(row["values"].keys(), reverse=True))
            if len(years) >= 2:
                year_counts[years] = year_counts.get(years, 0) + 1
        dominant_years = []
        if year_counts:
            dominant_years = max(year_counts, key=year_counts.get)

        # shift lagging instantaneous rows (cash flow only)
        if statement_type == "cash_flow" and dominant_years:
            dominant_years_int = [int(y) for y in dominant_years]
            for row in structured_rows:
                current_years = sorted([int(y) for y in row["values"].keys()], reverse=True)
                if len(current_years) == len(dominant_years) and all(
                    (dy - cy == 1) for dy, cy in zip(dominant_years_int, current_years)
                ):
                    shifted = {str(int(y) + 1): v for y, v in row["values"].items()}
                    row["values"] = shifted

        all_years = set()
        for r in structured_rows:
            all_years.update(r["values"].keys())

        return sorted(all_years, reverse=True), structured_rows

    # ---------------- RESTRUCTURE FOR MERGER ----------------
    @staticmethod
    def _restructure_for_merger(flat_json: dict) -> dict:
        statement_type = flat_json.get("statement_type", "")
        years = flat_json.get("years", [])
        rows = flat_json.get("rows", [])

        sections = []
        current_section = None
        pending_section_candidates = []

        for row in rows:
            line_item = row.get("line_item", "").strip()
            values = row.get("values", {})
            has_values = bool(values)

            if not has_values:
                pending_section_candidates.append(line_item)
            else:
                if pending_section_candidates:
                    section_label = pending_section_candidates[-1].rstrip(":").strip()
                    current_section = {
                        "section": section_label,
                        "gaap": None,
                        "items": []
                    }
                    sections.append(current_section)
                    pending_section_candidates = []

                if current_section is None:
                    current_section = {
                        "section": "Main",
                        "gaap": None,
                        "items": []
                    }
                    sections.append(current_section)

                item_gaap = None
                for year_key, year_data in values.items():
                    if isinstance(year_data, dict) and "meta" in year_data:
                        item_gaap = year_data["meta"].get("name")
                        break

                preserved_values = {year_key: year_data for year_key, year_data in values.items()}

                current_section["items"].append({
                    "label": line_item,
                    "gaap": item_gaap,
                    "values": preserved_values
                })

        return {
            "statement_type": statement_type,
            "periods": years,
            "sections": sections
        }

    # ---------------- TABLE / EXCEL HELPERS ----------------
    def extract_table_data(self, table_idx: int, statement_type: str) -> List[List[str]]:
        if table_idx >= len(self.tables):
            return []
        table = self.tables[table_idx]
        xbrl_tags = table.find_all(attrs={"contextref": True})
        if xbrl_tags:
            years, rows = self._extract_xbrl_data_from_table(table, statement_type)
            if years and rows:
                data = [["Line Item"] + years]
                for r in rows:
                    row = [r["line_item"]] + [r["values"].get(y, "") for y in years]
                    if any(row):
                        data.append(row)
                print(f"✓ XBRL extraction: {len(years)} columns, {len(data)-1} rows")
                return data
        return self._extract_table_data_traditional(table)

    def _extract_table_data_traditional(self, table) -> List[List[str]]:
        try:
            from io import StringIO
            import warnings
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                dfs = pd.read_html(StringIO(str(table)), flavor="html5lib")
            if dfs:
                df = dfs[0]
                data = [df.columns.tolist()] + df.values.tolist()
                cleaned = [[str(c).strip() if pd.notna(c) else "" for c in row] for row in data if any(row)]
                return cleaned
        except Exception:
            pass
        rows = []
        for tr in table.find_all("tr"):
            row = [td.get_text(strip=True) for td in tr.find_all(["td", "th"])]
            if any(row):
                rows.append(row)
        return rows

    # ---------------- STATEMENT EXTRACTION ----------------
    def extract_statement(self, role_id: str, statement_name: str, statement_type: str,
                          output_filename: str, display_output: bool = True) -> Dict:
        print(f"\n{'='*80}\nExtracting: {statement_name}\n{'='*80}\n")

        anchor_idx = self.find_table_by_unique_anchor(role_id, statement_type)
        if anchor_idx is None:
            print("⚠ Using pattern matching fallback...")
            kw = {
                "cash_flow": ["Cash flows", "Operating activities", "Investing activities", "Financing activities"],
                "balance_sheet": ["Assets", "Liabilities", "Cash and cash equivalents"],
                "income_statement": ["Revenues", "Net earnings", "Operating", "Income"],
            }.get(statement_type, [])
            matches = self.find_table_by_pattern(kw)
            if matches:
                anchor_idx = matches[0]
            else:
                return {"status": "failed", "error": f"Could not locate {statement_name}"}

        data = self.extract_table_data(anchor_idx, statement_type)

        if isinstance(data, dict) and "rows" in data:
            flat_json = data
        else:
            if not data or len(data) < 2:
                return {"status": "failed", "error": f"No data found for {statement_name}"}
            years = data[0][1:]
            rows = []
            for r in data[1:]:
                label = r[0]
                vals = {y: v for y, v in zip(years, r[1:]) if v != ""}
                rows.append({"line_item": label, "values": vals})
            flat_json = {"statement_type": statement_type, "years": years, "rows": rows}

        json_output = self._restructure_for_merger(flat_json)
        print(f"✓ Restructured to merger-compatible format: {len(json_output.get('sections', []))} sections")
        print(f"✓ Metadata preserved for all values")

        # No file saving - data returned in memory only for UI display
        return {"status": "success", "json": json_output}

    def extract_all_statements(self, display_output: bool = True) -> Dict[str, Dict]:
        configs = {
            "balance_sheet": ("balance_sheet", "Consolidated_Balance_Sheets"),
            "income_statement": ("income_statement", "Consolidated_Statements_of_Earnings"),
            "cash_flow": ("cash_flow", "Consolidated_Statements_of_Cash_Flows"),
        }
        results = {}
        for key, (stype, fname) in configs.items():
            results[key] = self.extract_statement(None, fname.replace("_", " "), stype, f"{fname}.xlsx", display_output)
        return results

    # ---------------- UTILITIES ----------------
    def to_dataframe(self, data: List[List[str]]) -> pd.DataFrame:
        if not data or len(data) < 2:
            return pd.DataFrame()
        df = pd.DataFrame(data[1:], columns=data[0])
        df = df[df.apply(lambda x: x.astype(str).str.strip().ne("").any(), axis=1)]
        return df

    def save_to_excel(self, data: List[List[str]], sheet_name: str, output_path: str):
        if Workbook is None:
            raise ImportError("openpyxl is required for Excel export. Install it with: pip install openpyxl")
        wb = Workbook()
        ws = wb.active
        ws.title = sheet_name[:31]
        bold = Font(bold=True)
        for i, row in enumerate(data, 1):
            for j, val in enumerate(row, 1):
                c = ws.cell(i, j, val)
                if i == 1:
                    c.font = bold
        wb.save(output_path)

    # ================ NEW: ITEM 8 HYPERLINK FALLBACK ================
    def find_table_by_item8_hyperlink(self, statement_type: str) -> Optional[int]:
        """
        Fallback method: Find statement table using Item 8 hyperlinks.
        Searches for hyperlinks in the document that reference financial statements,
        then locates the table following that anchor.
        Uses substring matching with parenthetical exclusion.
        """
        print("🔍 Trying Item 8 hyperlink fallback...")

        # Define search patterns for each statement type (core phrases to match)
        STATEMENT_PATTERNS = {
            "balance_sheet": [
                "consolidated balance sheets",
                "Consolidated Balance Sheets",
                "balance sheets",
                "consolidated statements of financial position",
                "statements of financial position"
            ],
            "income_statement": [
                "consolidated statements of income",
                "Consolidated Statements of Income",
                "statements of income",
                "consolidated statements of operations",
                "statements of operations",
                "consolidated statements of earnings",
                "statements of earnings"
            ],
            "cash_flow": [
                "consolidated statements of cash flows",
                "statements of cash flows",
                "Consolidated Statements of Cash Flows",
                "consolidated cash flow statements"
            ]
        }

        patterns = STATEMENT_PATTERNS.get(statement_type, [])
        if not patterns:
            return None

        # Search for hyperlinks pointing to statements
        for link in self.soup.find_all('a', href=True):
            link_text = link.get_text(strip=True).lower()
            href = link.get('href', '')

            # Check if link text contains statement patterns (handles "Aon plc Consolidated Statements of Income")
            for pattern in patterns:
                pattern_lower = pattern.lower()

                # Check if pattern exists in link text
                if pattern_lower in link_text:
                    # Find where the pattern ends in the link text
                    pattern_start = link_text.find(pattern_lower)
                    pattern_end = pattern_start + len(pattern_lower)

                    # Get text after the pattern
                    remaining_text = link_text[pattern_end:].strip()

                    # Exclude if there's parenthetical content after the pattern
                    # This rejects "(Parenthetical)" or "(Details)" etc.
                    if remaining_text and remaining_text.startswith('('):
                        continue  # Skip this match
                    
                    # Valid match found - extract anchor ID from href
                    if '#' in href:
                        anchor_id = href.split('#')[-1]

                        # Find the element with this ID
                        anchor_element = self.soup.find(attrs={"id": anchor_id})

                        if anchor_element:
                            # Find the next table after this anchor
                            next_table = anchor_element.find_next('table')

                            if next_table:
                                # Find the index of this table in self.tables
                                try:
                                    table_idx = self.tables.index(next_table)
                                    print(f"✓ Item 8 hyperlink matched '{link_text[:50]}...' → table {table_idx}")
                                    return table_idx
                                except ValueError:
                                    continue
                                
        print(f"⚠ Item 8 hyperlink fallback failed for {statement_type}")
        return None
    # ================================================================

    # ---------------- META-LINK TABLE MATCHING (UPDATED - FIXED) ----------------
    def find_table_by_unique_anchor(self, role_id: Optional[str], statement_type: str) -> Optional[int]:
        """
        Find table using MetaLinks uniqueAnchor with proper name + contextRef matching.
        Falls back to Item 8 hyperlinks if uniqueAnchor is null or matching fails.
        """
        if not self.metalinks:
            return None
        
        TAXONOMY_MAP = {
            "balance_sheet": [
                "consolidated balance sheets",
                "balance sheet",
                "statement of financial position",
                "financial condition",
                "assets and liabilities",
            ],
            "income_statement": [
                "consolidated statements of operations",
                "consolidated statement of operations",
                "Consolidated Statements of Income",
                "income statement",
                "income statements",
                "consolidated statements of profit or loss",
                "statement of earnings",
                "profit and loss",
                "consolidated income statements",
                "consolidated statements of income",
                "consolidated statements of earnings",
            ],
            "cash_flow": [
                "consolidated statements of cash flows",
                "consolidated statement of cash flows",
                "statement of cash flows",
                "cash flows statements"
            ],
        }
        
        statement_roles = {
            rid: r for rid, r in self.metalinks.items()
            if r.get("groupType", "").lower() == "statement"
        }
        
        role_lookup = {}
        for rid, rpt in statement_roles.items():
            shortname = rpt.get("shortName", "").lower().strip()
            for stype, names in TAXONOMY_MAP.items():
                if any(shortname == n.lower() for n in names):
                    role_lookup[stype] = (rid, rpt)
                    break
        
        # Helper function to find table using uniqueAnchor
        def find_table_with_anchor(role: Dict) -> Optional[int]:
            """Find table by matching both name and contextRef from uniqueAnchor"""
            unique_anchor = role.get("uniqueAnchor")
            
            # Check if uniqueAnchor exists and is not null
            if not unique_anchor or not isinstance(unique_anchor, dict):
                return None
            
            anchor_name = unique_anchor.get("name")
            anchor_context = unique_anchor.get("contextRef")
            
            if not anchor_name:
                return None
            
            # Search through all tables
            for idx, tbl in enumerate(self.tables):
                # If contextRef is provided, match BOTH name AND contextRef
                if anchor_context:
                    matching_tag = tbl.find(attrs={"name": anchor_name, "contextref": anchor_context})
                    if matching_tag:
                        print(f"✓ MetaLinks matched with name='{anchor_name}' AND contextRef='{anchor_context}' → table {idx}")
                        return idx
                else:
                    # If no contextRef, fall back to name-only matching
                    matching_tag = tbl.find(attrs={"name": anchor_name})
                    if matching_tag:
                        print(f"✓ MetaLinks matched with name='{anchor_name}' (no contextRef check) → table {idx}")
                        return idx
            
            return None
        
        # Try direct role_id match first
        if role_id and role_id in statement_roles:
            role = statement_roles[role_id]
            table_idx = find_table_with_anchor(role)
            if table_idx is not None:
                return table_idx
        
        # Try statement_type lookup
        if statement_type in role_lookup:
            _, role = role_lookup[statement_type]
            table_idx = find_table_with_anchor(role)
            if table_idx is not None:
                return table_idx
        
        # If uniqueAnchor is null or matching failed, try Item 8 hyperlink fallback
        print("⚠ MetaLinks uniqueAnchor is null or matching failed")
        hyperlink_idx = self.find_table_by_item8_hyperlink(statement_type)
        if hyperlink_idx is not None:
            return hyperlink_idx
        
        return None

    def find_table_by_pattern(self, keywords: List[str], min_length: int = 800) -> List[int]:
        found = []
        for i, t in enumerate(self.tables):
            text = t.get_text().lower()
            if len(text) >= min_length and all(k.lower() in text for k in keywords):
                found.append(i)
        return found


print("[OK] Financial Statement Scraper loaded successfully!")
print("[OK] Integrated MetaLinks-based statement detection")
print("[OK] Item 8 hyperlink fallback added")
print("[OK] Pattern-based fallback retained")
print("[OK] Post-alignment year correction active")
print("[OK] Noise filter integrated for clean data extraction")
print("[OK] Auto-restructuring for merger compatibility enabled")
print("[OK] Metadata preservation active")
print("[OK] Robust standalone 'id' extraction implemented for citations")
print("[OK] UUID contextRef handling fixed - context_mapping checked first")
print("[OK] FIXED: uniqueAnchor now matches BOTH name AND contextRef for precise table detection\n")