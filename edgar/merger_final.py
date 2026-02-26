from collections import OrderedDict
from typing import Any, Dict


def build_unified_catalog_all_statements(years_json: Dict[str, Any]) -> Dict[str, OrderedDict]:
    return {
        "income_statement": OrderedDict(),
        "balance_sheet": OrderedDict(),
        "cash_flow_statement": OrderedDict(),
    }

from collections import defaultdict, OrderedDict
import re
import logging
from difflib import SequenceMatcher
# LLM import disabled
# from helper.section_match_fallback_llm import apply_global_llm_section_consolidation

# ============================================================================
# FINANCIAL STATEMENT MERGER - FIXED VERSION
# ============================================================================
# 
# KEY FIXES IMPLEMENTED:
# 
# 1. WATERFALL MATCHING in match_line_items():
#    - GAAP match (handles GAAP changes across years)
#    - Label match (handles label variations)
#    - Value match (FIXED: compares actual values only, ignores metadata)
#      This fixes the issue where items with same values but different 
#      metadata were treated as different items.
# 
# 2. WATERFALL MATCHING in zero_out():
#    - Uses same 3-tier matching logic (GAAP → Label → Values)
#    - Prevents false zeroing of valid data
#    - Handles GAAP tag changes, label variations, and metadata differences
# 
# WHAT THIS SOLVES:
# - "Net earnings" with GAAP change (NetIncomeLoss → ProfitLoss) ✅
# - "Securities purchased" with label variations (embedded $ amounts) ✅
# - "Customer receivables" with GAAP change + label variation ✅
# - "Trading assets" with GAAP change + label variation ✅
# 
# ============================================================================

# ---------- your existing helpers (unchanged) ----------
def normalize_label(label: str) -> str:
    if not label:
        return ""
    label = label.lower()
    label = re.sub(r"[^a-z0-9 ]", " ", label)
    return re.sub(r"\s+", " ", label).strip()

def normalize_year_key(key: str) -> str:
    if not key:
        return ""
    m = re.search(r"(20\d{2}|19\d{2})", str(key))
    return m.group(1) if m else str(key)

def normalize_values(values: dict) -> dict:
    new_vals = {}
    for k, v in (values or {}).items():
        year = normalize_year_key(k)
        new_vals[year] = v
    return new_vals

def labels_clearly_different(label1: str, label2: str, threshold: float = 0.5) -> bool:
    l1, l2 = normalize_label(label1), normalize_label(label2)
    if not l1 or not l2:
        return False
    ratio = SequenceMatcher(None, l1, l2).ratio()
    return ratio < threshold

def detect_gaap_collisions(section_rows):
    counts = {}
    for r in section_rows:
        g = r.get("item_gaap")
        if g:
            counts[g] = counts.get(g, 0) + 1
    return {g for g, c in counts.items() if c > 1}

def flatten_with_positions(filing):
    """same as you shared; adds item 'position' and normalizes values/periods"""
    flat = []
    filing["periods"] = [normalize_year_key(p) for p in filing.get("periods", [])]
    for section in filing.get("sections", []):
        sec_gaap = section.get("gaap")
        sec_label = section.get("section")
        for idx, item in enumerate(section.get("items", [])):
            flat.append({
                "section_gaap": sec_gaap,
                "section_label": sec_label,
                "item_gaap": item.get("gaap"),
                "item_label": item.get("label"),
                "values": normalize_values(item.get("values", {})),
                "position": idx
            })
    return flat

def match_line_items(item1, item2, overlap_years, ignore_gaap: bool = False):
    """Waterfall matching: GAAP → Label → Values (FIXED: compares actual values only, not metadata)"""
    # Check 1: GAAP match
    if not ignore_gaap and item1["item_gaap"] and (item1["item_gaap"] == item2["item_gaap"]):
        return True
    
    # Check 2: Label match
    if normalize_label(item1["item_label"]) == normalize_label(item2["item_label"]):
        return True
    
    # Check 3: Value match (FIXED: extract actual numeric values only, ignore metadata)
    overlap_years = {normalize_year_key(y) for y in overlap_years}
    
    # Extract actual values (not the full dict with metadata)
    def extract_value(v):
        """Extract the actual numeric value from dict or return as-is"""
        if isinstance(v, dict):
            return v.get("value", v)  # Get 'value' key if dict, else return dict
        return v
    
    v1 = {}
    for y, v in item1["values"].items():
        if y in overlap_years:
            val = extract_value(v)
            if val not in (0, None, "", "0"):
                v1[y] = val
    
    v2 = {}
    for y, v in item2["values"].items():
        if y in overlap_years:
            val = extract_value(v)
            if val not in (0, None, "", "0"):
                v2[y] = val
    
    # Normalize values for comparison (remove commas, whitespace)
    def normalize_value(val):
        if val is None:
            return None
        val_str = str(val).replace(",", "").replace(" ", "").strip()
        # Handle negative values in parentheses
        if val_str.startswith("(") and val_str.endswith(")"):
            val_str = "-" + val_str[1:-1]
        return val_str
    
    # Normalize both value dicts
    v1_norm = {y: normalize_value(v) for y, v in v1.items()}
    v2_norm = {y: normalize_value(v) for y, v in v2.items()}
    
    if v1_norm and v2_norm and v1_norm == v2_norm:
        return True
    
    return False

# ---------- sets 0 to null values ---------

def _collect_all_target_years(flat_all):
    """
    Build the union of all normalized years present across all rows in all filings.
    """
    years = set()
    for _, rows in flat_all.items():
        for r in rows:
            years.update(r.get("values", {}).keys())
    # return as a sorted list (string years like "2025","2024",...)
    return sorted(years)

def _pad_missing_years_in_mapping(mapping, target_years):
    """
    For each payload in the (unified/ordered) mapping, ensure every target_year exists.
    Missing years are inserted with 0.0 (not None).
    Also converts any existing None values to 0.0.
    """
    for payload in mapping.values():
        vals = payload.setdefault("values", {})
        for y in target_years:
            if y not in vals or vals[y] is None:
                vals[y] = 0.0

# ---------- ordering-only helpers (no heuristics) ----------
def _sec_key(gaap, label):
    return gaap or normalize_label(label or "")

def _item_identity_for_positions(row, collision_gaaps):
    """
    Use the SAME identity rule you use when inserting into unified:
      - if GAAP collides inside a section → use normalized label
      - else use GAAP, else normalized label
    """
    if row.get("item_gaap") in collision_gaaps:
        return normalize_label(row.get("item_label") or "")
    return row.get("item_gaap") or normalize_label(row.get("item_label") or "")

def _parse_unified_key(orig_key):
    """
    Your unified keys are either:
      - "{itm_key}|{sec_key}"
      - "review_needed|{sec_key}|{norm_label}"
    Return (sec_key, itm_key_guess). Fall back gracefully.
    """
    if orig_key.startswith("review_needed|"):
        _, sec_key, norm_label = orig_key.split("|", 2)
        return sec_key, norm_label
    else:
        parts = orig_key.rsplit("|", 1)
        if len(parts) == 2:
            return parts[1], parts[0]
        return "", orig_key  # fallback


def _build_unified_section_index(unified):
    """
    Build a quick index of unified items by section key.
    section key = GAAP if present, else normalized section label.
    """
    idx = defaultdict(list)
    for payload in unified.values():
        sk = _sec_key(payload.get("section_gaap"), payload.get("section_label"))
        idx[sk].append(payload)
    return idx

def _sections_same_by_items(existing_sec_key,
                            unified_by_sec,
                            candidate_rows,
                            candidate_collision_gaaps,
                            ratio_threshold: float = 0.9):
    """
    Fallback: consider sections the same if ≥ ratio_threshold of the CANDIDATE section's
    items have a match (via your match_line_items waterfall) with ANY item already in the
    EXISTING unified section.

    - existing_sec_key: section key for the 'existing' payload (from unified)
    - candidate_sec_key: section key for the section we're currently processing (this filing)
    - unified_by_sec: index mapping sec_key -> list of unified payloads (items)
    - candidate_rows: list of raw rows for this candidate section in the current filing
    - candidate_collision_gaaps: set of GAAP tags colliding within candidate section
    """
    existing_items = unified_by_sec.get(existing_sec_key, [])
    if not existing_items:
        return False  # nothing to compare to

    total = len(candidate_rows)
    if total == 0:
        return False

    matched = 0
    for cand in candidate_rows:
        # use collision flag for candidate item (your rule)
        ignore_gaap = cand.get("item_gaap") in candidate_collision_gaaps

        # If ANY existing item matches, count this candidate as matched
        found = False
        for ex in existing_items:
            # overlap years per your matcher
            overlap_years = set((cand.get("values") or {}).keys()) & set((ex.get("values") or {}).keys())
            if match_line_items(cand, ex, overlap_years, ignore_gaap=ignore_gaap):
                found = True
                break
        if found:
            matched += 1

    return (matched / total) >= ratio_threshold
# ----------------------------
# GREEDY MATCHING : Matching and pinning to avoid multisectional collapsing
# ----------------------------

def _same_section_gate(g1, l1, g2, l2):
    """
    Your current gate: sections are 'same' if GAAP matches OR normalized label matches.
    Kept identical to your inline logic so behavior doesn't change, only *who* is eligible.
    """
    return (g1 and g1 == g2) or (normalize_label(l1) == normalize_label(l2))


def _list_unified_sections(unified):
    """
    Build an ordered map of unified section keys -> (gaap, label).
    Order is stable (insertion order of 'unified').
    """
    secmap = OrderedDict()
    for payload in unified.values():
        sk = _sec_key(payload.get("section_gaap"), payload.get("section_label"))
        # first payload under a section is good enough as section rep
        if sk not in secmap:
            secmap[sk] = (payload.get("section_gaap"), payload.get("section_label"))
    return secmap


def _candidate_sections_in_order(section_rows):
    """
    Given the flat 'rows' list for the CURRENT filing, return section keys
    in FIRST-APPEARANCE order (top to bottom).
    """
    seen = OrderedDict()
    for r in section_rows:
        sk = _sec_key(r["section_gaap"], r["section_label"])
        if sk not in seen:
            seen[sk] = (r["section_gaap"], r["section_label"])
    # returns: OrderedDict[sec_key] -> (gaap,label) for the candidate filing
    return seen


def _build_greedy_section_map(unified, flat_rows_for_this_filing):
    """
    Core: produce a one-to-one mapping from the CURRENT filing's section keys
    to AT MOST one unified section key.

    - Walk candidate sections in first-appearance order (top-down).
    - For each, pick the FIRST unified section that passes the same-section gate
      and has not been used already.
    - If none fits, map to None => this section must create/extend its own bucket.
    """
    unified_secs = _list_unified_sections(unified)           # sk_u -> (gaap,label)
    cand_secs = _candidate_sections_in_order(flat_rows_for_this_filing)  # sk_c -> (gaap,label)

    used_unified = set()
    greedy_map = {}

    for sk_c, (cg, cl) in cand_secs.items():
        match_sk_u = None
        for sk_u, (ug, ul) in unified_secs.items():
            if sk_u in used_unified:
                continue
            if _same_section_gate(ug, ul, cg, cl):
                match_sk_u = sk_u
                used_unified.add(sk_u)   # consume target once matched
                break
        greedy_map[sk_c] = match_sk_u  # None means "no existing target; create"
    return greedy_map  # {candidate_sec_key: matched_unified_sec_key_or_None}


# ----------------------------
# Check and flag sections with same gaap name in the same filing 
# ----------------------------

def _flag_duplicate_section_gaaps_label_only(rows):
    """
    For a single filing's flattened rows:
    - If a section GAAP appears under >1 *different* section labels,
      blank the GAAP (section_gaap="") for the 2nd+ labels so they become label-only.
    - Operates in-place on `rows`.
    """
    first_label_for_gaap = {}  # gaap -> normalized first section label
    for r in rows:
        g = (r.get("section_gaap") or "").strip()
        if not g:
            continue  # already label-only
        lbl_norm = normalize_label(r.get("section_label") or "")
        if g not in first_label_for_gaap:
            first_label_for_gaap[g] = lbl_norm  # remember the first section label that used this GAAP
            continue
        # If same GAAP but a different section label → force label-only for this section's rows
        if first_label_for_gaap[g] != lbl_norm:
            r["section_gaap"] = ""                # blank GAAP so section key becomes the label
            r["_force_label_only"] = True         # informational flag (not strictly required after blanking)

# ---
# Greedy matching for line items
# ---

def _build_greedy_item_map(unified, allowed_sec_key, section_rows, collision_gaaps):
    """
    For a given candidate section:
      - Look at existing unified items ONLY in allowed_sec_key.
      - Walk candidate rows in order and greedily assign the FIRST unused existing item
        that matches via match_line_items (GAAP→label→values).
      - Return: {row_index -> preselected_unified_key}
    """
    if allowed_sec_key is None:
        return {}

    # gather existing items ONLY from the allowed section
    existing_pool = []
    for key, ex in unified.items():
        sk = _sec_key(ex.get("section_gaap"), ex.get("section_label"))
        if sk == allowed_sec_key:
            existing_pool.append((key, ex))

    used_keys = set()
    greedy_map = {}

    for idx, cand in enumerate(section_rows):
        ignore_gaap = cand.get("item_gaap") in collision_gaaps
        for key, ex in existing_pool:
            if key in used_keys:
                continue
            overlap_years = set((cand.get("values") or {}).keys()) & set((ex.get("values") or {}).keys())
            if match_line_items(cand, ex, overlap_years, ignore_gaap=ignore_gaap):
                greedy_map[idx] = key        # pin candidate row -> this unified key
                used_keys.add(key)           # consume so it can't be matched again
                break                        # first-match-wins for this row

    return greedy_map

# ----------------------------
# FIXED: zero_out function now uses WATERFALL MATCHING (GAAP → Label → Values)
# ----------------------------

def zero_out_overlapping_years_for_new_items(unified, flat_all, years_sorted):
    """
    FIXED VERSION with WATERFALL MATCHING (mirrors match_line_items logic).
    
    Patch to zero out years where an item doesn't exist in the authoritative filing for that year.
    
    NEW: Uses waterfall matching (GAAP → Label → Values) to determine if an item exists,
    matching the same logic used during merging.
    
    Args:
        unified: The unified catalog (OrderedDict or dict)
        flat_all: Dict mapping year -> list of flattened rows
        years_sorted: List of years sorted oldest to newest (ascending)
    
    Returns:
        None (modifies unified in-place)
    """
    
    # Step 1: Build year -> authoritative_filing_year map
    year_to_authoritative_filing = {}
    
    for filing_year in reversed(years_sorted):  # Process newest to oldest (reverse for authoritative mapping)
        rows = flat_all[filing_year]
        years_in_filing = set()
        for r in rows:
            years_in_filing.update(r.get("values", {}).keys())
        
        for year in years_in_filing:
            if year not in year_to_authoritative_filing:
                year_to_authoritative_filing[year] = filing_year
    
    print(f"DEBUG: Year to authoritative filing map: {dict(sorted(year_to_authoritative_filing.items(), reverse=False))}")
    
    # Step 2: Build filing data structure (group by section for easy lookup)
    filing_sections = {}  # filing_year -> {section_key -> [items]}
    
    for filing_year in years_sorted:
        rows = flat_all[filing_year]
        sections = defaultdict(list)
        
        for r in rows:
            sec_key = _sec_key(r["section_gaap"], r["section_label"])
            sections[sec_key].append(r)
        
        filing_sections[filing_year] = sections
    
    # Step 3: WATERFALL MATCHING - Check each unified item
    items_zeroed = 0
    total_years_zeroed = 0
    
    for orig_key, payload in unified.items():
        sec_key = _sec_key(payload["section_gaap"], payload["section_label"])
        
        # Check each year this unified item has
        years_to_zero = []
        
        for year in list(payload["values"].keys()):
            if year not in year_to_authoritative_filing:
                continue
            
            authoritative_filing = year_to_authoritative_filing[year]
            
            # Get all items in this section from the authoritative filing
            auth_section_items = filing_sections.get(authoritative_filing, {}).get(sec_key, [])
            
            if not auth_section_items:
                # Section doesn't exist in authoritative filing
                if payload["values"][year] != 0:
                    years_to_zero.append(year)
                    payload["values"][year] = 0.0
                continue
            
            # WATERFALL MATCHING: Try to find this item in authoritative filing
            # Use the SAME matching logic as match_line_items()
            
            found_match = False
            
            for auth_item in auth_section_items:
                # Check 1: GAAP match (if both have GAAP tags)
                unified_gaap = payload.get("item_gaap")
                auth_gaap = auth_item.get("item_gaap")
                
                if unified_gaap and auth_gaap and unified_gaap == auth_gaap:
                    found_match = True
                    break
                
                # Check 2: Label match (normalized)
                unified_label = normalize_label(payload.get("item_label", ""))
                auth_label = normalize_label(auth_item.get("item_label", ""))
                
                if unified_label and auth_label and unified_label == auth_label:
                    found_match = True
                    break
                
                # Check 3: Value match (if this year has overlapping data)
                # Get the value for this specific year from both items
                unified_year_val = payload["values"].get(year)
                auth_year_val = auth_item.get("values", {}).get(year)
                
                # Extract actual numeric values for comparison
                if unified_year_val and auth_year_val:
                    # Handle dict format with 'value' key
                    if isinstance(unified_year_val, dict):
                        unified_year_val = unified_year_val.get("value")
                    if isinstance(auth_year_val, dict):
                        auth_year_val = auth_year_val.get("value")
                    
                    # Normalize values for comparison (remove commas, convert to string)
                    def normalize_value(v):
                        if v is None or v == 0:
                            return None
                        v_str = str(v).replace(",", "").replace(" ", "").strip()
                        # Handle negative values in parentheses
                        if v_str.startswith("(") and v_str.endswith(")"):
                            v_str = "-" + v_str[1:-1]
                        return v_str
                    
                    unified_norm = normalize_value(unified_year_val)
                    auth_norm = normalize_value(auth_year_val)
                    
                    if unified_norm and auth_norm and unified_norm == auth_norm:
                        found_match = True
                        break
            
            # If no match found, zero out this year
            if not found_match:
                if payload["values"][year] != 0:
                    years_to_zero.append(year)
                    payload["values"][year] = 0.0
        
        if years_to_zero:
            items_zeroed += 1
            total_years_zeroed += len(years_to_zero)
            print(f"DEBUG: Zeroed {years_to_zero} for: {payload.get('item_label')} | GAAP: {payload.get('item_gaap')} | Section: {payload.get('section_label')}")
    
    print(f"DEBUG: Total items with zeroed years: {items_zeroed}")
    print(f"DEBUG: Total year-values zeroed: {total_years_zeroed}")
    print("="*80)

# ----------------------------
# Check the line items fallback for unmatched sections
# ----------------------------
def _apply_fallback_section_matching(unified, flat_rows_for_this_filing, greedy_sec_map, 
                                     ratio_threshold=0.5):
    """
    Fallback for unmatched sections: if ≥80% of candidate section's items match 
    ANY item in an existing unified section, treat them as the same section.
    
    This runs AFTER greedy matching, so it only affects sections where greedy_map[sec_key] is None.
    
    Args:
        unified: Current unified catalog
        flat_rows_for_this_filing: All rows from current filing
        greedy_sec_map: Dict from _build_greedy_section_map {candidate_sec_key: matched_unified_sec_key_or_None}
        ratio_threshold: Minimum match ratio to consider sections the same (default 0.8 = 80%)
    
    Returns:
        Updated greedy_sec_map with fallback matches filled in
    """
    print("\n" + "="*80)
    print("🔍 FALLBACK SECTION MATCHING - DEBUG TRACE")
    print("="*80)
    
    # Build index of unified items by section for quick lookup
    unified_by_sec = _build_unified_section_index(unified)
    
    # Group candidate rows by section
    candidate_sections = defaultdict(list)
    for r in flat_rows_for_this_filing:
        sk = _sec_key(r["section_gaap"], r["section_label"])
        candidate_sections[sk].append(r)
    
    # Detect collisions for each candidate section
    collision_gaaps_per_section = {}
    for sk, rows in candidate_sections.items():
        collision_gaaps_per_section[sk] = detect_gaap_collisions(rows)
    
    # Process only unmatched sections (where greedy_map returned None)
    updated_map = dict(greedy_sec_map)  # Copy to avoid modifying original
    
    # Track statistics
    unmatched_sections = []
    total_unmatched = 0
    total_matched_by_fallback = 0
    
    print(f"\n📊 Initial Status:")
    print(f"   • Total candidate sections: {len(greedy_sec_map)}")
    print(f"   • Already matched by greedy: {sum(1 for v in greedy_sec_map.values() if v is not None)}")
    print(f"   • Unmatched (need fallback): {sum(1 for v in greedy_sec_map.values() if v is None)}")
    print(f"   • Total unified sections available: {len(unified_by_sec)}")
    print(f"   • Match threshold: {ratio_threshold:.0%}")
    
    for candidate_sk, matched_unified_sk in greedy_sec_map.items():
        if matched_unified_sk is not None:
            # Already matched by greedy - skip
            continue
        
        total_unmatched += 1
        candidate_rows = candidate_sections[candidate_sk]
        collision_gaaps = collision_gaaps_per_section[candidate_sk]
        
        print(f"\n{'─'*80}")
        print(f"🔍 Analyzing UNMATCHED section: '{candidate_sk}'")
        print(f"   📝 Items in this section: {len(candidate_rows)}")
        if collision_gaaps:
            print(f"   ⚠️  GAAP collisions detected: {collision_gaaps}")
        
        # Try matching against each existing unified section
        best_match_sk = None
        best_match_ratio = 0.0
        all_match_attempts = []
        
        for existing_sk in unified_by_sec.keys():
            # Check if this unified section is already claimed by greedy matching
            if existing_sk in updated_map.values():
                # Skip - this unified section already matched to another candidate
                continue
            
            # Calculate detailed match statistics
            matched_items = []
            unmatched_items = []
            existing_items = unified_by_sec.get(existing_sk, [])
            
            for idx, cand in enumerate(candidate_rows):
                ignore_gaap = cand.get("item_gaap") in collision_gaaps
                matched_this_item = False
                matched_to = None
                
                for ex in existing_items:
                    overlap_years = set((cand.get("values") or {}).keys()) & \
                                  set((ex.get("values") or {}).keys())
                    if match_line_items(cand, ex, overlap_years, ignore_gaap=ignore_gaap):
                        matched_this_item = True
                        matched_to = ex.get("item_label", "Unknown")
                        break
                
                if matched_this_item:
                    matched_items.append({
                        'candidate_label': cand.get("item_label", "Unknown"),
                        'matched_to': matched_to,
                        'candidate_gaap': cand.get("item_gaap", "N/A")
                    })
                else:
                    unmatched_items.append({
                        'label': cand.get("item_label", "Unknown"),
                        'gaap': cand.get("item_gaap", "N/A")
                    })
            
            ratio = len(matched_items) / len(candidate_rows) if candidate_rows else 0
            
            # Store attempt info
            all_match_attempts.append({
                'existing_sk': existing_sk,
                'ratio': ratio,
                'matched_count': len(matched_items),
                'total_count': len(candidate_rows),
                'passes_threshold': ratio >= ratio_threshold,
                'matched_items': matched_items,
                'unmatched_items': unmatched_items
            })
            
            # Update best match if this is better
            if ratio >= ratio_threshold and ratio > best_match_ratio:
                best_match_ratio = ratio
                best_match_sk = existing_sk
        
        # Sort attempts by ratio (highest first) for display
        all_match_attempts.sort(key=lambda x: x['ratio'], reverse=True)
        
        # Display top 3 candidates (or all if fewer)
        print(f"\n   📈 Top matching candidates:")
        for i, attempt in enumerate(all_match_attempts[:3], 1):
            status = "✅ PASS" if attempt['passes_threshold'] else "❌ FAIL"
            print(f"      #{i}. '{attempt['existing_sk']}'")
            print(f"          Match ratio: {attempt['ratio']:.1%} ({attempt['matched_count']}/{attempt['total_count']}) {status}")
            
            if i == 1 and attempt['ratio'] > 0:  # Show details for best candidate
                if attempt['matched_items']:
                    print(f"          ✓ Matched items (showing first 3):")
                    for item in attempt['matched_items'][:3]:
                        print(f"             • {item['candidate_label'][:50]} → {item['matched_to'][:50]}")
                    if len(attempt['matched_items']) > 3:
                        print(f"             ... and {len(attempt['matched_items']) - 3} more")
                
                if attempt['unmatched_items'] and len(attempt['unmatched_items']) <= 5:
                    print(f"          ✗ Unmatched items:")
                    for item in attempt['unmatched_items']:
                        print(f"             • {item['label'][:60]}")
        
        if len(all_match_attempts) == 0:
            print(f"   ⚠️  No available unified sections to match against (all already claimed)")
        
        # Apply best match if found
        if best_match_sk is not None:
            updated_map[candidate_sk] = best_match_sk
            total_matched_by_fallback += 1
            print(f"\n   ✅ FALLBACK MATCH SUCCESSFUL!")
            print(f"      Matched '{candidate_sk}' → '{best_match_sk}'")
            print(f"      Final ratio: {best_match_ratio:.1%}")
        else:
            unmatched_sections.append({
                'section': candidate_sk,
                'item_count': len(candidate_rows),
                'best_ratio': all_match_attempts[0]['ratio'] if all_match_attempts else 0.0,
                'reason': 'No candidates available' if len(all_match_attempts) == 0 else 'Below threshold'
            })
            print(f"\n   ❌ NO MATCH FOUND")
            if all_match_attempts:
                print(f"      Best ratio was {all_match_attempts[0]['ratio']:.1%} (threshold: {ratio_threshold:.0%})")
            else:
                print(f"      No available unified sections to match")
    
    # Final summary
    print(f"\n{'='*80}")
    print(f"📊 FALLBACK MATCHING SUMMARY")
    print(f"{'='*80}")
    print(f"   • Sections analyzed: {total_unmatched}")
    print(f"   • Successfully matched by fallback: {total_matched_by_fallback}")
    print(f"   • Still unmatched: {len(unmatched_sections)}")
    
    if unmatched_sections:
        print(f"\n   ⚠️  SECTIONS REMAINING UNMAPPED:")
        for i, info in enumerate(unmatched_sections, 1):
            print(f"      {i}. '{info['section']}' ({info['item_count']} items)")
            print(f"         Best match ratio: {info['best_ratio']:.1%}")
            print(f"         Reason: {info['reason']}")
    
    print(f"\n{'='*80}\n")
    
    return updated_map

# ----------------------------
# DROP-IN: same name/signature, matching unchanged; index-based ordering added
# ----------------------------
def build_unified_catalog(years_json, statement_type):
    """
    EXACT same behavior as your original for matching/merging.
    ONLY change: the returned mapping is an OrderedDict with items ordered by:
      1) Sections in the order from the latest year automatically,
      2) Items within each section using:
         - If an item exists in the latest year → its latest position (latest wins),
         - Else → its position from the most recent year it appears,
         - Older-only items are inserted around the latest-year spine without reordering
           any latest-year items. Ties are resolved stably using the relative anchor
           and label as a final tie-breaker.
    """
    # ---- PREP: flatten each year + collisions per section/year ----
    flat_all = {}   # year -> flat rows (with 'position')
    years_sorted = sorted(years_json.keys(), reverse=False)  # oldest -> newest (ascending)
    latest_year = years_sorted[-1]  # Latest year is now the last one


    # collisions map and positions ledger
    collisions_per_year_section = {}  # (year, sec_key) -> set(gaap)
    positions_map = defaultdict(dict) # (sec_key, item_key) -> {year: position}
    latest_section_order = OrderedDict()  # sec_key -> index in latest year
    

    for yr, filing in years_json.items():
        stmt_key = list(filing.keys())[0]  # Gets 'income_statement', 'balance_sheet', or 'cash_flow_statement'
        stmt = filing[stmt_key]
        rows = flatten_with_positions(stmt)
        _flag_duplicate_section_gaaps_label_only(rows)
        flat_all[yr] = rows

        # detect collisions by section (same as your code path)
        section_groups = defaultdict(list)
        for r in rows:
            sec_key = _sec_key(r["section_gaap"], r["section_label"])
            section_groups[sec_key].append(r)
        for sec_key, section_rows in section_groups.items():
            collisions_per_year_section[(yr, sec_key)] = detect_gaap_collisions(section_rows)

    # build positions_map using the SAME identity rule you use to create unified keys
    for yr in years_sorted:
        for r in flat_all[yr]:
            sk = _sec_key(r["section_gaap"], r["section_label"])
            collision_gaaps = collisions_per_year_section[(yr, sk)]
            ik = _item_identity_for_positions(r, collision_gaaps)
            positions_map[(sk, ik)][yr] = r["position"]

    # build latest-year section order spine (appearance order in latest)
    for r in flat_all[latest_year]:
        sk = _sec_key(r["section_gaap"], r["section_label"])
        if sk not in latest_section_order:
            latest_section_order[sk] = len(latest_section_order)

    # ---- YOUR ORIGINAL MERGE (UNCHANGED) ----
    unified = {}

    for _, rows in flat_all.items():
        # Group by section
        section_groups = defaultdict(list)
        for r in rows:
            sec = r["section_gaap"] or normalize_label(r["section_label"])
            section_groups[sec].append(r)
        
        # NEW: build greedy map for THIS filing using its flat rows
        greedy_sec_map = _build_greedy_section_map(unified, rows)

## update the section data after fallback matching
        unified_by_sec = _build_unified_section_index(unified)
        for candidate_sk, target_sk in greedy_sec_map.items():
            if target_sk is not None and candidate_sk != target_sk:
                # This section was matched (either by greedy or fallback)
                # Get the target section's metadata
                target_items = unified_by_sec.get(target_sk, [])
                if target_items:
                    target_section_gaap = target_items[0]["section_gaap"]
                    target_section_label = target_items[0]["section_label"]

                    # Update ALL rows in the candidate section to use target's metadata
                    for r in rows:
                        row_sk = _sec_key(r["section_gaap"], r["section_label"])
                        if row_sk == candidate_sk:
                            # Overwrite section metadata
                            r["section_gaap"] = target_section_gaap
                            r["section_label"] = target_section_label
                    print(f"   🔄 Updated section metadata: '{candidate_sk}' → '{target_sk}'")



        for sec, section_rows in section_groups.items():
            
            unified_by_sec = _build_unified_section_index(unified)
            # Detect GAAP collisions
            collision_gaaps = detect_gaap_collisions(section_rows)

            # NEW: pin this candidate section to one unified section (greedy + fallback)
            allowed_unified_sk = greedy_sec_map.get(sec)

            

            # NEW: preselect one-to-one item matches within this section
            greedy_item_map = _build_greedy_item_map(unified, allowed_unified_sk, section_rows, collision_gaaps)

            for row_idx, row in enumerate(section_rows):
                ignore_gaap = row.get("item_gaap") in collision_gaaps
                matched_key = None
            
                # SECTION SCOPE: only this section may match
                allowed_unified_sk_local = greedy_sec_map.get(sec)
            
                # ITEM-LEVEL GREEDY: use preselected unified key (if any)
                pre_key = greedy_item_map.get(row_idx) if allowed_unified_sk_local is not None else None
            
                if pre_key is not None:
                    existing = unified[pre_key]
            
                    # Safety: ensure same-section gate still holds
                    same_section = (
                        (existing["section_gaap"] and existing["section_gaap"] == row["section_gaap"]) or
                        (normalize_label(existing["section_label"]) == normalize_label(row["section_label"]))
                    )
                    if same_section:
                        overlap_years = set(existing["values"].keys()) & set(row["values"].keys())
                        if match_line_items(row, existing, overlap_years, ignore_gaap=ignore_gaap):
                            matched_key = pre_key
            
                # If there was no preselected match, create new (unchanged behavior)
                if matched_key and not matched_key.startswith("review_needed"):
                    for y, v in row["values"].items():
                        if y not in unified[matched_key]["values"]:
                            unified[matched_key]["values"][y] = v
                        else:
                            if int(normalize_year_key(yr[:4])) > int(normalize_year_key(list(unified[matched_key]["values"].keys())[0])):
                                unified[matched_key]["values"][y] = v
                elif not matched_key:
                    itm_key = (normalize_label(row["item_label"]) if ignore_gaap
                               else (row.get("item_gaap") or normalize_label(row["item_label"])))
                    key = f"{itm_key}|{sec}"
                    
                    # Clean values to ensure no None values
                    cleaned_values = {}
                    for k, v in row["values"].items():
                        cleaned_values[k] = v if v is not None else 0.0
                    
                    unified[key] = {
                        "section_gaap": row["section_gaap"],
                        "section_label": row["section_label"],
                        "item_gaap": row["item_gaap"],
                        "item_label": row["item_label"],
                        "values": cleaned_values
                    }


                if matched_key and not matched_key.startswith("review_needed"):
                    for y, v in row["values"].items():
                        # Clean the value to ensure no None
                        clean_v = v if v is not None else 0.0
                        # If this period hasn't been set yet, take it
                        if y not in unified[matched_key]["values"]:
                            unified[matched_key]["values"][y] = clean_v
                        else:
                            # Otherwise, keep the value from the newer filing (later year in years_sorted)
                            if int(normalize_year_key(yr[:4])) > int(normalize_year_key(list(unified[matched_key]["values"].keys())[0])):
                                unified[matched_key]["values"][y] = clean_v

                elif not matched_key:
                    # Build safe key (identical to your original)
                    itm_key = (
                        normalize_label(row["item_label"])
                        if ignore_gaap else (row.get("item_gaap") or normalize_label(row["item_label"]))
                    )
                    key = f"{itm_key}|{sec}"
                    
                    # Clean values to ensure no None values
                    cleaned_values = {}
                    for k, v in row["values"].items():
                        cleaned_values[k] = v if v is not None else 0.0
                    
                    unified[key] = {
                        "section_gaap": row["section_gaap"],
                        "section_label": row["section_label"],
                        "item_gaap": row["item_gaap"],
                        "item_label": row["item_label"],
                        "values": cleaned_values
                    }

    # FIXED: Call the corrected zero_out function
    zero_out_overlapping_years_for_new_items(unified, flat_all, years_sorted)
    
    # ---- PATCH: normalize section labels using latest year ----
    # Build a lookup of section_key -> latest label from the latest year
    latest_section_labels = {}
    for r in flat_all[latest_year]:
        sk = _sec_key(r["section_gaap"], r["section_label"])
        if sk not in latest_section_labels:
            latest_section_labels[sk] = (r["section_label"], r["section_gaap"])

    # Update all unified items to use that label consistently
    for payload in unified.values():
        sk = _sec_key(payload["section_gaap"], payload["section_label"])
        if sk in latest_section_labels:
            latest_label, latest_gaap = latest_section_labels[sk]
            payload["section_label"] = latest_label
            payload["section_gaap"] = latest_gaap

    # ---- ORDERING ONLY (latest wins; older-only insert around spine) ----
    # group unified items by section
    by_section = defaultdict(list)  # sec_key -> list of (orig_key, payload)
    for orig_key, payload in unified.items():
        sk = (payload["section_gaap"] or normalize_label(payload["section_label"]))
        by_section[sk].append((orig_key, payload))

    # final ordered dict
    ordered = OrderedDict()

    # iterate sections in latest-year order; any section absent in latest goes to the end
    section_keys_sorted = sorted(by_section.keys(), key=lambda sk: latest_section_order.get(sk, 10**9))

    for sk in section_keys_sorted:
        items = by_section[sk]

        # Build latest spine for this section (list of (orig_key, payload, latest_pos, latest_index))
        latest_items = []
        older_only_items = []

        # For mapping unified keys back to the item identity used in positions_map
        def _item_identity_from_unified_key(key, payload):
            sec_from_key, itm_guess = _parse_unified_key(key)
            if sec_from_key and sec_from_key != sk:
                # fall back (rare) – reconstruct identity like insertion uses
                g = payload.get("item_gaap")
                return g or normalize_label(payload.get("item_label") or "")
            return itm_guess

        # Collect and separate
        for orig_key, payload in items:
            ik = _item_identity_from_unified_key(orig_key, payload)
            pos_by_year = positions_map.get((sk, ik), {})
            if latest_year in pos_by_year:
                latest_items.append((orig_key, payload, pos_by_year[latest_year]))
            else:
                # take position from most recent year the item exists
                pos = None
                for y in years_sorted:
                    if y in pos_by_year:
                        pos = pos_by_year[y]
                        break
                # If truly missing (shouldn't happen), push to the end
                if pos is None:
                    pos = 10**9
                older_only_items.append((orig_key, payload, pos))

        # Sort latest items by their latest position (this defines immutable spine)
        latest_items.sort(key=lambda t: (t[2], normalize_label(t[1]["item_label"])))
        spine = latest_items  # list with stable order

        # Build an index map for spine positions to spine indices
        spine_positions = [p for (_, _, p) in spine]

        def anchor_index_for_pos(p):
            """Map an older-only position p to the insert anchor in the spine (before first spine item with pos>=p)."""
            for idx, sp in enumerate(spine_positions):
                if p <= sp:
                    return idx
            return len(spine_positions)  # append at end if larger than all

        # Prepare sortable list: give each item a (anchor_idx, priority_flag, tie) key
        # priority_flag: 0 for older-only (so they insert BEFORE the spine item at the same anchor),
        #                1 for latest items (spine itself)
        sortable = []
        for (orig_key, payload, p) in older_only_items:
            anchor_idx = anchor_index_for_pos(p)
            sortable.append((anchor_idx, 0, normalize_label(payload["item_label"]), orig_key, payload))
        for (orig_key, payload, p) in spine:
            anchor_idx = spine_positions.index(p)  # its own slot
            sortable.append((anchor_idx, 1, normalize_label(payload["item_label"]), orig_key, payload))

        # Final sort: by anchor index; older-only (0) before latest (1) at the same anchor; then label for stability
        sortable.sort(key=lambda x: (x[0], x[1], x[2]))

        # Emit into ordered dict in this section's sequence
        for _, _, _, orig_key, payload in sortable:
            ordered[orig_key] = payload

        # Build the complete set of years we want to show as columns
        target_years = _collect_all_target_years(flat_all)

        # Pad all items so missing years become 0.0 (no empty cells)
        _pad_missing_years_in_mapping(ordered, target_years)

    # LLM section consolidation disabled
    # ordered = apply_global_llm_section_consolidation(ordered, statement_type)

    return ordered




#### wrapper function 

def build_unified_catalog_all_statements(years_json):
    """
    Processes all financial statements (income_statement, balance_sheet, cash_flow_statement)
    from the provided JSON and returns unified catalogs for each.
    
    Args:
        years_json: Dictionary with structure:
            {
                "ticker": "MSFT",
                "years": {
                    "2025-06-30": {
                        "income_statement": {...},
                        "balance_sheet": {...},
                        "cash_flow_statement": {...}
                    },
                    ...
                }
            }
    
    Returns:
        Dictionary with unified catalogs for each statement:
        {
            "income_statement": OrderedDict(...),
            "balance_sheet": OrderedDict(...),
            "cash_flow_statement": OrderedDict(...)
        }
    """
    try:
        # Extract just the years data
        years_data = years_json.get("years", {})
        
        if not years_data:
            logging.warning("No years data found in years_json")
            return {
                "income_statement": OrderedDict(),
                "balance_sheet": OrderedDict(),
                "cash_flow_statement": OrderedDict()
            }
        
        # Statement types to process
        statement_types = ["income_statement", "balance_sheet", "cash_flow_statement"]
        
        results = {}
        
        for stmt_type in statement_types:
            try:
                # Build a statement-specific years dictionary
                stmt_years = {}
                source_urls = []  # Track source URLs
                
                for year_key, year_data in years_data.items():
                    if stmt_type in year_data:
                        # Check if the statement data has an error
                        if isinstance(year_data[stmt_type], dict) and 'error' in year_data[stmt_type]:
                            logging.warning(f"Skipping {stmt_type} for {year_key} due to error: {year_data[stmt_type]['error']}")
                            continue
                        
                        # Extract source URL if available
                        if isinstance(year_data[stmt_type], dict) and 'source_url' in year_data[stmt_type]:
                            source_url = year_data[stmt_type]['source_url']
                            if source_url and source_url not in source_urls:
                                source_urls.append(source_url)
                        
                        stmt_years[year_key] = {stmt_type: year_data[stmt_type]}
            
                # Only process if we have data for this statement
                if stmt_years:
                    unified_result = build_unified_catalog(stmt_years, stmt_type)
                    # Store source URLs list in the results dict so it can be accessed
                    results[stmt_type] = unified_result
                    # Return all URLs (one for each year)
                    results[stmt_type + "_url"] = source_urls if source_urls else []
                else:
                    results[stmt_type] = OrderedDict()
                    results[stmt_type + "_url"] = []
            except Exception as e:
                logging.error(f"Error processing {stmt_type}: {e}")
                results[stmt_type] = OrderedDict()
        
        return results
    except Exception as e:
        logging.error(f"Critical error in build_unified_catalog_all_statements: {e}")
        return {
            "income_statement": OrderedDict(),
            "balance_sheet": OrderedDict(),
            "cash_flow_statement": OrderedDict()
        }