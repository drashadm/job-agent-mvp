#!/usr/bin/env python3
import sys
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

import config
from rss_client import fetch_rss_items
from airtable_client import AirtableClient


def _today_date() -> str:
    return datetime.now().date().isoformat()


def run_ingest(settings, airtable, max_items=None):
    """Run RSS ingest and return dict with results.
    Returns: {"total": int, "created": int, "skipped": int, "first_created_id": str|None}
    """
    url = getattr(settings, "RSS_FEED_URL", None)
    if not url:
        raise RuntimeError("RSS_FEED_URL not configured")

    items = fetch_rss_items(url)
    if max_items:
        items = items[:max_items]
    
    total = len(items)
    created = 0
    skipped = 0
    first_created_id = None

    for item in items:
        try:
            job_url = (item.get("link") or "").strip()
            if not job_url:
                # skip malformed items
                skipped += 1
                continue

            # Check for existing record with same JobURL
            try:
                existing = airtable.find_one(
                    table=settings.AIRTABLE_TABLE_JOBS,
                    id_field="JobURL",
                    id_value=job_url,
                )
            except Exception as e:
                # Airtable API error: print and skip this item
                print("AIRTABLE_LOOKUP_FAIL", str(e))
                skipped += 1
                continue

            if existing:
                skipped += 1
                continue

            # Prepare fields to create. Write both `Title` and `JobTitle` to be safe.
            # Include JobDescriptionRaw from RSS description field
            fields = {
                "JobURL": job_url,
                "Title": item.get("title", ""),
                "JobTitle": item.get("title", ""),
                "JobDescriptionRaw": item.get("description", ""),
                "Source": getattr(settings, "RSS_SOURCE_FEED_NAME", "RSS"),
                "Status": "New",
                "DateFound": _today_date(),
            }

            try:
                created_rec = airtable.create_record(settings.AIRTABLE_TABLE_JOBS, fields)
                created += 1
                if not first_created_id:
                    first_created_id = created_rec.get("id")
            except RuntimeError as e:
                # Check for INVALID_MULTIPLE_CHOICE_OPTIONS error (invalid select option)
                if "INVALID_MULTIPLE_CHOICE_OPTIONS" in str(e):
                    print("SOURCE_FALLBACK_USED")
                    # Retry without Source field
                    fields_fallback = {
                        "JobURL": job_url,
                        "Title": item.get("title", ""),
                        "JobTitle": item.get("title", ""),
                        "JobDescriptionRaw": item.get("description", ""),
                        "Status": "New",
                        "DateFound": _today_date(),
                    }
                    try:
                        created_rec = airtable.create_record(settings.AIRTABLE_TABLE_JOBS, fields_fallback)
                        created += 1
                        if not first_created_id:
                            first_created_id = created_rec.get("id")
                    except Exception as e_retry:
                        print("AIRTABLE_CREATE_FAIL", str(e_retry))
                        skipped += 1
                else:
                    print("AIRTABLE_CREATE_FAIL", str(e))
                    skipped += 1
            except Exception as e:
                print("AIRTABLE_CREATE_FAIL", str(e))
                skipped += 1
        except Exception as e:
            # Per-item safety: don't let one bad item stop the run
            print("ITEM_PROCESS_FAIL", str(e))
            skipped += 1
            continue

    return {
        "total": total,
        "created": created,
        "skipped": skipped,
        "first_created_id": first_created_id,
    }


def debug_sample_mode():
    """Debug mode: print structure of first 3 RSS items without writing to Airtable."""
    try:
        from rss_client import fetch_rss_items_raw
        
        settings = config.settings
        url = getattr(settings, "RSS_FEED_URL", None)
        if not url:
            print("RSS_FEED_URL not configured")
            return 1
        
        print(f"Fetching RSS from: {url}")
        raw_items = fetch_rss_items_raw(url)
        print(f"Total items fetched: {len(raw_items)}\n")
        
        for i, item in enumerate(raw_items[:3], 1):
            print(f"=== ITEM {i} ===")
            
            # Get basic fields
            title = (item.findtext("title") or "").strip()
            link = (item.findtext("link") or "").strip()
            
            print(f"Title: {title}")
            print(f"Link: {link}")
            
            # Get all available child element tags
            available_keys = sorted(set(child.tag for child in item))
            print(f"Available keys: {available_keys}")
            
            # Check candidate text fields
            print(f"\nCandidate text field lengths:")
            
            summary = (item.findtext("summary") or "")
            print(f"  summary: {len(summary)} chars")
            
            description = (item.findtext("description") or "")
            print(f"  description: {len(description)} chars")
            
            subtitle = (item.findtext("subtitle") or "")
            print(f"  subtitle: {len(subtitle)} chars")
            
            # Check for content element (might be nested)
            content_elem = item.find("content")
            if content_elem is not None:
                content_text = (content_elem.text or "")
                print(f"  content (direct): {len(content_text)} chars")
            else:
                print(f"  content (direct): 0 chars (element not found)")
            
            # Check for encoded content (common in RSS)
            encoded = item.findtext("{http://purl.org/rss/1.0/modules/content/}encoded") or ""
            print(f"  content:encoded: {len(encoded)} chars")
            
            print()
        
        return 0
        
    except Exception as e:
        print(f"DEBUG_SAMPLE_FAIL {e}")
        return 1


def debug_write_one_mode():
    """Debug mode: write exactly one RSS item to Airtable to verify JobDescriptionRaw is populated."""
    try:
        settings = config.settings
        settings.validate()
        
        url = getattr(settings, "RSS_FEED_URL", None)
        if not url:
            print("RSS_FEED_URL not configured")
            return 1
        
        airtable = AirtableClient(settings.AIRTABLE_TOKEN, settings.AIRTABLE_BASE_ID)
        
        print(f"Fetching RSS from: {url}")
        items = fetch_rss_items(url)
        print(f"Total items fetched: {len(items)}")
        
        if not items:
            print("No items to write")
            return 1
        
        # Take first item
        item = items[0]
        job_url = item.get("link", "").strip()
        job_title = item.get("title", "").strip()
        job_description = item.get("description", "").strip()
        
        if not job_url:
            print("First item has no URL")
            return 1
        
        print(f"\nAttempting to create record for:")
        print(f"  URL: {job_url[:80]}...")
        print(f"  Title: {job_title[:80]}...")
        print(f"  Description length: {len(job_description)} chars")
        
        # Prepare fields
        fields = {
            "JobURL": job_url,
            "JobTitle": job_title,
            "Title": job_title,
            "JobDescriptionRaw": job_description,
            "Source": getattr(settings, "RSS_SOURCE_FEED_NAME", "RSS"),
            "Status": "New",
            "DateFound": _today_date(),
        }
        
        try:
            created_rec = airtable.create_record(settings.AIRTABLE_TABLE_JOBS, fields)
            record_id = created_rec.get("id")
            print(f"\n✅ WRITE_ONE created_record_id={record_id}")
            print(f"WRITE_ONE desc_len={len(job_description)}")
            print(f"WRITE_ONE url={job_url}")
            return 0
        except RuntimeError as e:
            # Try fallback without Source if needed
            if "INVALID_MULTIPLE_CHOICE_OPTIONS" in str(e):
                print("SOURCE_FALLBACK_USED - retrying without Source field")
                fields_fallback = {
                    "JobURL": job_url,
                    "JobTitle": job_title,
                    "Title": job_title,
                    "JobDescriptionRaw": job_description,
                    "Status": "New",
                    "DateFound": _today_date(),
                }
                created_rec = airtable.create_record(settings.AIRTABLE_TABLE_JOBS, fields_fallback)
                record_id = created_rec.get("id")
                print(f"\n✅ WRITE_ONE created_record_id={record_id}")
                print(f"WRITE_ONE desc_len={len(job_description)}")
                print(f"WRITE_ONE url={job_url}")
                return 0
            else:
                print(f"AIRTABLE_CREATE_FAIL {e}")
                return 1
        
    except Exception as e:
        print(f"DEBUG_WRITE_ONE_FAIL {e}")
        return 1


def main() -> int:
    import argparse
    
    parser = argparse.ArgumentParser(description="RSS ingest script")
    parser.add_argument("--debug-sample", action="store_true", help="Debug mode: print first 3 RSS items and exit")
    parser.add_argument("--debug-write-one", action="store_true", help="Debug mode: write exactly one RSS item to Airtable and exit")
    args = parser.parse_args()
    
    if args.debug_sample:
        return debug_sample_mode()
    
    if args.debug_write_one:
        return debug_write_one_mode()
    
    try:
        settings = config.settings
        settings.validate()
        airtable = AirtableClient(settings.AIRTABLE_TOKEN, settings.AIRTABLE_BASE_ID)
        
        result = run_ingest(settings, airtable)
        
        print(f"RSS_INGEST_OK total={result['total']} created={result['created']} skipped={result['skipped']}")
        if result['first_created_id']:
            print(f"FIRST_CREATED_ID {result['first_created_id']}")
        
        
    except Exception as e:
        print("RSS_INGEST_FAIL", e)
        return 1


if __name__ == "__main__":
    sys.exit(main())
