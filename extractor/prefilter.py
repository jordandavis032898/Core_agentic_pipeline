from typing import Dict, Any, Optional


FILTER_VERSION = "dummy_prefilter_reference_only"


def prefilter_statement_page_from_rmd(
    rmd_page_text: str,
    cfg: Optional[object] = None,
    previous_page_context: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Dummy prefilter used only to illustrate the expected return shape.

    - Treats any non-empty input as a generic "candidate" page.
    - Does not analyze content, detect tables, or use configuration/context.
    """
    text = (rmd_page_text or "").strip()
    if not text:
        return {
            "pass": False,
            "type": "none",
            "reason": "empty_page",
            "debug": {"version": FILTER_VERSION},
        }

    return {
        "pass": True,
        "type": "dummy",
        "reason": "dummy_prefilter_for_structure_only",
        "debug": {"version": FILTER_VERSION},
    }

from typing import Dict, Any, Optional

"""
Dummy prefilter module.

This exists only to keep the folder / import structure intact.
It does not implement any real table or financial detection logic.
"""

FILTER_VERSION = "dummy_prefilter_reference_only"


def prefilter_statement_page_from_rmd(
    rmd_page_text: str,
    cfg: Optional[object] = None,
    previous_page_context: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Dummy prefilter used only for structural reference.

    It treats any non-empty page as a generic "candidate" page and
    provides a minimal, predictable dictionary shape.
    """
    text = (rmd_page_text or "").strip()
    if not text:
        return {
            "pass": False,
            "type": "none",
            "reason": "empty_page",
            "debug": {"version": FILTER_VERSION},
        }

    return {
        "pass": True,
        "type": "dummy",
        "reason": "dummy_prefilter_for_structure_only",
        "debug": {"version": FILTER_VERSION},
    }

from typing import Dict, Any, Optional


FILTER_VERSION = "stub_prefilter_v1"


def prefilter_statement_page_from_rmd(
    rmd_page_text: str,
    cfg: Optional[object] = None,
    previous_page_context: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Extremely simplified placeholder prefilter.

    Keeps the original function signature but does not attempt
    to detect or classify tables in any meaningful way.
    """
    text = (rmd_page_text or "").strip()
    if not text:
        return {
            "pass": False,
            "type": "neither",
            "reason": "empty",
            "debug": {"version": FILTER_VERSION},
        }

    return {
        "pass": True,
        "type": "unknown",
        "reason": "stub_prefilter",
        "debug": {"version": FILTER_VERSION},
    }

