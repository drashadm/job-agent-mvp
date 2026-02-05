#!/usr/bin/env python3
import sys
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

from config import settings
from rss_client import fetch_rss_items
from airtable_client import AirtableClient
from openai_client import OpenAIClient
import utils


RSS_NORMALIZE_PROMPT = """\
SYSTEM
You extract structured job data. Return JSON only. No markdown. No commentary.

USER
Given this RSS item, normalize into JSON with this exact schema:
{
"job_url": "",
"job_title": "",
"company": "",
"location": "",
"remote_status": "Remote|Hybrid|Onsite|Unknown",
"job_description_raw": "",
"keywords": [],
"needs_human_input": []
}

Rules:

job_description_raw must be >= 1200 characters if the RSS text allows it.

Do NOT invent requirements not present. You may reorganize and rewrite the provided content for clarity.

If you cannot reach 1200 without inventing, keep best-effort and include "insufficient_description" in needs_human_input.

keywords should be a list of up to 20 strings.

Return valid JSON only.

RSS_TITLE: <title>
RSS_LINK: <link>
RSS_DESCRIPTION: <description>
"""


RSS_EXPAND_PROMPT = """\
SYSTEM
You expand a job description using only the provided RSS content. Return JSON only. No markdown. No commentary.

USER
EXPAND

Given this RSS item, return JSON with this exact schema:
{
"job_url": "",
"job_title": "",
"company": "",
"location": "",
"remote_status": "Remote|Hybrid|Onsite|Unknown",
"job_description_raw": "",
"keywords": [],
"needs_human_input": []
}

Rules:

job_description_raw must be >= 1200 characters if the RSS text allows it.

Do NOT invent requirements not present. You may reorganize and rewrite the provided content for clarity.

If you cannot reach 1200 without inventing, keep best-effort and include "insufficient_description" in needs_human_input.

keywords should be a list of up to 20 strings.

Return valid JSON only.

RSS_TITLE: <title>
RSS_LINK: <link>
RSS_DESCRIPTION: <description>
"""


def _today_date() -> str:
    return datetime.now().date().isoformat()


def _build_prompt(prompt_template: str, title: str, link: str, description: str) -> str:
    prompt = prompt_template.replace("<title>", title)
    prompt = prompt.replace("<link>", link)
    prompt = prompt.replace("<description>", description)
    return prompt


def normalize_rss_item(openai_client, item, model, rss_company="", rss_location=""):
    """
    Normalize a single RSS item using OpenAI.
    Returns normalized dict or None if normalization fails.
    """
    title = item.get("title", "").strip()
    link = item.get("link", "").strip()
    description = item.get("description", "").strip()
    
    prompt = _build_prompt(RSS_NORMALIZE_PROMPT, title, link, description)
    
    try:
        response_text = openai_client.request_json(
            model=model,
            prompt=prompt,
            max_tokens=2000,
            temperature=0.0
        )
        normalized = utils.safe_parse_json(response_text)
        return normalized
    except Exception as e:
        return None


def expand_rss_item(openai_client, item, model, rss_company="", rss_location=""):
    title = item.get("title", "").strip()
    link = item.get("link", "").strip()
    description = item.get("description", "").strip()

    prompt = _build_prompt(RSS_EXPAND_PROMPT, title, link, description)

    try:
        response_text = openai_client.request_json(
            model=model,
            prompt=prompt,
            max_tokens=2000,
            temperature=0.0,
        )
        expanded = utils.safe_parse_json(response_text)
        return expanded
    except Exception:
        return None


def _ensure_insufficient_flag(needs_human_input):
    if not isinstance(needs_human_input, list):
        needs_human_input = []
    if "insufficient_description" not in needs_human_input:
        needs_human_input.append("insufficient_description")
    return needs_human_input


def _create_with_fallback(airtable, table_name, fields, job_url, job_title, job_description_raw):
    try:
        return airtable.create_record(table_name, fields)
    except Exception as e:
        error_msg = str(e)
        print(f"AIRTABLE_CREATE_FAIL reason={error_msg}")

    # Drop fields progressively on any error
    optional_fields = ["Company", "Location", "RemoteStatus", "Keywords", "NeedsHumanInput", "DateFound"]
    base_fields = {
        "JobURL": job_url,
        "JobTitle": job_title,
        "JobDescriptionRaw": job_description_raw,
        "Status": "New",
    }

    for field_name in optional_fields:
        if field_name in fields:
            fields = {k: v for k, v in fields.items() if k != field_name}
            try:
                return airtable.create_record(table_name, fields)
            except Exception as e:
                print(f"AIRTABLE_CREATE_FAIL reason={e}")
                continue

    try:
        return airtable.create_record(table_name, base_fields)
    except Exception as e:
        print(f"AIRTABLE_CREATE_FAIL reason={e}")
        return None


def run_rss_normalize_ingest():
    """
    Main function: fetch RSS items, normalize with OpenAI, create Airtable records.
    """
    # Validate settings
    try:
        settings.validate()
    except Exception as e:
        print(f"CONFIG_ERROR {e}")
        sys.exit(1)
    
    # Initialize clients
    airtable = AirtableClient(
        token=settings.AIRTABLE_TOKEN,
        base_id=settings.AIRTABLE_BASE_ID,
    )
    openai_client = OpenAIClient(api_key=settings.OPENAI_API_KEY)
    
    # Fetch RSS items
    url = settings.RSS_FEED_URL
    if not url:
        print("RSS_FEED_URL not configured")
        sys.exit(1)
    
    try:
        items = fetch_rss_items(url)
    except Exception as e:
        print(f"RSS_FETCH_FAIL reason={e}")
        sys.exit(1)
    
    # Limit to 10 items
    items = items[:10]
    total = len(items)
    print(f"RSS_FETCH_OK count={total}")
    
    created = 0
    skipped = 0
    failed = 0
    created_record_ids = []
    
    for idx, item in enumerate(items, start=1):
        job_url = (item.get("link") or "").strip()
        
        if not job_url:
            print(f"SKIP_NO_URL idx={idx}")
            skipped += 1
            continue
        
        # Deduplication: check if record with same JobURL exists
        try:
            existing = airtable.find_one(
                table=settings.AIRTABLE_TABLE_JOBS,
                id_field="JobURL",
                id_value=job_url,
            )
            if existing:
                print(f"DUPLICATE_SKIP job_url={job_url}")
                skipped += 1
                continue
        except Exception as e:
            print(f"AIRTABLE_LOOKUP_FAIL idx={idx} reason={e}")
            failed += 1
            continue
        
        # Normalize with OpenAI, passing RSS metadata to skip LLM parsing if available
        normalized = normalize_rss_item(openai_client, item, settings.OPENAI_MODEL_PARSE)
        
        if not normalized:
            print(f"NORMALIZE_FAIL idx={idx} reason=json_parse_error")
            failed += 1
            continue
        
        # Extract fields from normalized data
        job_title = normalized.get("job_title") or ""
        company = normalized.get("company") or ""
        location = normalized.get("location") or ""
        remote_status = normalized.get("remote_status") or "Unknown"
        job_description_raw = normalized.get("job_description_raw") or ""
        keywords = normalized.get("keywords") or []
        needs_human_input = normalized.get("needs_human_input") or []

        if not isinstance(keywords, list):
            keywords = []
        if not isinstance(needs_human_input, list):
            needs_human_input = []

        desc_len = len(job_description_raw)
        if desc_len < 1200:
            print(f"DESC_TOO_SHORT idx={idx} len={desc_len}")
            expanded = expand_rss_item(openai_client, item, settings.OPENAI_MODEL_PARSE, rss_company, rss_location)
            expanded = expand_rss_item(openai_client, item, settings.OPENAI_MODEL_PARSE)
            if expanded and isinstance(expanded, dict):
                expanded_desc = expanded.get("job_description_raw") or ""
                expanded_len = len(expanded_desc)
                if expanded_len > desc_len:
                    job_description_raw = expanded_desc
                    job_title = expanded.get("job_title") or job_title
                    company = expanded.get("company") or company
                    location = expanded.get("location") or location
                    remote_status = expanded.get("remote_status") or remote_status
                    keywords = expanded.get("keywords") or keywords
                    needs_human_input = expanded.get("needs_human_input") or needs_human_input
                if not isinstance(keywords, list):
                    keywords = []
                if not isinstance(needs_human_input, list):
                    needs_human_input = []
                new_len = len(job_description_raw)
                print(f"EXPAND_OK idx={idx} new_len={new_len}")
                if new_len < 1200:
                    needs_human_input = _ensure_insufficient_flag(needs_human_input)
                    print(f"EXPAND_STILL_SHORT idx={idx} final_len={new_len}")
            else:
                needs_human_input = _ensure_insufficient_flag(needs_human_input)
                print(f"EXPAND_STILL_SHORT idx={idx} final_len={desc_len}")

        desc_len = len(job_description_raw)
        if desc_len < 1200:
            needs_human_input = _ensure_insufficient_flag(needs_human_input)
        
        desc_len = len(job_description_raw)
        print(f"NORMALIZE_OK idx={idx} desc_len={desc_len}")
        
        # Prepare Airtable fields
        fields = {
            "JobURL": job_url,
            "JobTitle": job_title,
            "Company": company,
            "Location": location,
            "RemoteStatus": remote_status,
            "JobDescriptionRaw": job_description_raw,
            "Keywords": utils.join_commas(keywords),
            "NeedsHumanInput": utils.join_lines(needs_human_input),
            "Status": "New",
            "DateFound": _today_date(),
        }
        
        created_rec = _create_with_fallback(
            airtable,
            settings.AIRTABLE_TABLE_JOBS,
            fields,
            job_url,
            job_title,
            job_description_raw,
        )
        if created_rec:
            record_id = created_rec.get("id", "")
            print(f"AIRTABLE_CREATE_OK record_id={record_id}")
            created += 1
            if record_id:
                created_record_ids.append(record_id)
        else:
            failed += 1
    
    # Post-run verification for first 3 created records
    for record_id in created_record_ids[:3]:
        try:
            rec = airtable.get_record(settings.AIRTABLE_TABLE_JOBS, record_id)
            fields = rec.get("fields", {}) if isinstance(rec, dict) else {}
            job_url = fields.get("JobURL", "")
            jd_raw = fields.get("JobDescriptionRaw", "") or ""
            status = fields.get("Status", "")
            jd_len = len(jd_raw)
            print(f"VERIFY_OK record_id={record_id} jd_len={jd_len} status={status}")
        except Exception as e:
            print(f"VERIFY_FAIL record_id={record_id} reason={e}")

    # Summary
    print(f"RSS_NORMALIZE_SUMMARY total={total} created={created} skipped={skipped} failed={failed}")


if __name__ == "__main__":
    run_rss_normalize_ingest()
