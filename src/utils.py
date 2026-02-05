import json
import re
from typing import Any, Dict, List, Optional


def join_list(items: Optional[List[str]], sep: str = ", ") -> str:
    if not items:
        return ""
    return sep.join([str(x) for x in items if x is not None])


def join_lines(items: Any) -> str:
    """
    Convert a list into newline-delimited text.
    If items is not a list, return a safe string.
    """
    if items is None:
        return ""
    if isinstance(items, list):
        return "\n".join([str(x) for x in items if x is not None and str(x).strip() != ""])
    return safe_str(items)


def join_commas(items: Any) -> str:
    """
    Convert a list into comma-delimited text.
    If items is not a list, return a safe string.
    """
    if items is None:
        return ""
    if isinstance(items, list):
        return ", ".join([str(x) for x in items if x is not None and str(x).strip() != ""])
    return safe_str(items)


def merge_notes(a: str, b: str) -> str:
    """
    Merge two text blocks cleanly, avoiding extra blank lines.
    """
    a = (a or "").strip()
    b = (b or "").strip()
    if a and b:
        return f"{a}\n{b}"
    return a or b


def safe_parse_json(text: str) -> Optional[Dict[str, Any]]:
    """
    Best-effort JSON parser. If the model returns extra text, tries to extract
    the first JSON object/array found.
    """
    try:
        return json.loads(text)
    except Exception:
        m = re.search(r"(\{.*\}|\[.*\])", text, re.S)
        if not m:
            return None
        try:
            parsed = json.loads(m.group(1))
            # If it's a list, wrap it to keep our return type stable
            if isinstance(parsed, list):
                return {"_list": parsed}
            if isinstance(parsed, dict):
                return parsed
            return None
        except Exception:
            return None


def safe_str(obj: Any) -> str:
    try:
        return str(obj)
    except Exception:
        return ""


def normalize_strategy(raw: str) -> str:
    """
    Normalize OpenAI strategy response to valid Airtable Strategy options.
    Valid options: "Apply", "Network First", "Skip"
    """
    if not raw:
        return "Network First"  # Safe default

    normalized = raw.strip().lower()

    # Map variants to canonical forms
    if normalized in ("apply", "yes", "should apply"):
        return "Apply"
    elif normalized in ("network first", "network", "reach out", "reach out first", "networking"):
        return "Network First"
    elif normalized in ("skip", "pass", "no", "dont apply"):
        return "Skip"
    else:
        # Fallback: try to detect key words
        if "apply" in normalized:
            return "Apply"
        elif "network" in normalized or "reach" in normalized:
            return "Network First"
        elif "skip" in normalized or "pass" in normalized:
            return "Skip"
        else:
            return "Network First"  # Safe default

