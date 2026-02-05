import requests
import xml.etree.ElementTree as ET
from typing import List, Dict


def fetch_rss_items_raw(url: str, timeout: int = 20) -> List[ET.Element]:
    """Fetch RSS XML and return raw ElementTree items for debugging.
    Raises RuntimeError on XML parse errors or request errors.
    """
    resp = requests.get(url, timeout=timeout)
    resp.raise_for_status()
    text = resp.text
    try:
        root = ET.fromstring(text)
    except ET.ParseError as e:
        raise RuntimeError(f"XML parse error: {e}") from e

    # Return raw item elements
    return root.findall(".//item")


def fetch_rss_items(url: str, timeout: int = 20) -> List[Dict[str, str]]:
    """Fetch RSS XML from `url` and return a list of items.

    Raises RuntimeError on XML parse errors or request errors.
    """
    resp = requests.get(url, timeout=timeout)
    resp.raise_for_status()
    text = resp.text
    try:
        root = ET.fromstring(text)
    except ET.ParseError as e:
        raise RuntimeError(f"XML parse error: {e}") from e

    items = []
    # Standard RSS uses <item>
    for item in root.findall(".//item"):
        title = (item.findtext("title") or "").strip()
        link = (item.findtext("link") or "").strip()
        pub = (item.findtext("pubDate") or "").strip()
        desc = (item.findtext("description") or "").strip()
        
        # Extract company and location from RSS metadata
        company = _extract_company_from_item(item)
        location = _extract_location_from_item(item)
        
        items.append({
            "title": title,
            "link": link,
            "published": pub,
            "description": desc,
            "company": company,
            "location": location,
        })

    return items


def _extract_company_from_item(item: ET.Element) -> str:
    """Extract company name from RSS item metadata.
    
    Checks in order:
    1. Custom job namespace: <job:company>
    2. description-based extraction (patterns like "Company: X")
    """
    # Check for job namespace tags (common in some RSS feeds)
    namespaces = {
        "job": "http://example.com/job",
        "": ""
    }
    
    # Try common namespaces and unprefixed tags
    for company_tag in ["job:company", "company", "{http://example.com/job}company"]:
        elem = item.find(company_tag)
        if elem is not None and elem.text:
            return elem.text.strip()
    
    # Fallback: try to extract from description if it contains "Company:" pattern
    desc = (item.findtext("description") or "").strip()
    if "Company:" in desc or "company:" in desc:
        # Simple extraction: look for "Company: <name>" pattern
        import re
        match = re.search(r"(?:Company|company):\s*([^\n,]+)", desc)
        if match:
            return match.group(1).strip()
    
    return ""


def _extract_location_from_item(item: ET.Element) -> str:
    """Extract location from RSS item metadata.
    
    Checks in order:
    1. Custom job namespace: <job:location>
    2. Category tags with location pattern
    3. description-based extraction (patterns like "Location: X")
    """
    # Try common namespaces and unprefixed tags
    for location_tag in ["job:location", "location", "{http://example.com/job}location"]:
        elem = item.find(location_tag)
        if elem is not None and elem.text:
            return elem.text.strip()
    
    # Try to extract from categories
    categories = item.findall("category")
    location_candidates = []
    for cat in categories:
        text = (cat.text or "").strip()
    
    # Fallback: try to extract from description if it contains "Location:" pattern
    desc = (item.findtext("description") or "").strip()

    # Try to extract from categories
