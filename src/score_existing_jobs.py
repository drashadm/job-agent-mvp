#!/usr/bin/env python3
"""
Score existing jobs that have JobDescriptionRaw but no FitScore yet.
Queries Airtable for records where JobDescriptionRaw is not empty and FitScore is empty,
then sends ONLY the JobDescriptionRaw to OpenAI for scoring.

Usage:
  export AIRTABLE_TOKEN=<key>
  export AIRTABLE_BASE_ID=<id>
  export AIRTABLE_TABLE_JOBS=<name>
  export OPENAI_API_KEY=<key>
  python src/score_existing_jobs.py [--max 10] [--dry-run]
"""
import argparse
import sys
import time
import json
import os
import re
import html as html_module
from dotenv import load_dotenv

load_dotenv()

from airtable_client import AirtableClient
from openai_client import OpenAIClient
import utils
import config
import score_new_jobs


CANDIDATE_PROFILE_SUMMARY = "Python/SQL automation engineer with experience building ingestion pipelines, API integrations, ETL workflows, logging/monitoring, and data processing. Background in finance and cybersecurity. Interested in AI engineering, AI integration specialist, or forward deployed engineer roles. Prefers remote US or hybrid in Maryland/DC/NOVA. No security clearance."

SYSTEM_PROMPT = """\
You are a strict job-fit scoring function.
Return ONLY valid JSON. No markdown. No extra keys. No commentary.
Use the full 1–5 scale; avoid defaulting to 3.
Allowed NextAction values: "Apply Now", "Apply", "Network First", "Skip".
"""

USER_PROMPT_TEMPLATE = """\
Score this job description for fit using ONLY the text below.

Candidate profile summary (treat as truth):
{candidate_profile}

Output JSON schema (no extra keys):
{{
  "FitScore": 1|2|3|4|5,
  "NextAction": "Apply Now"|"Apply"|"Network First"|"Skip",
  "FitReasons": "short evidence-based match reasons (max 300 chars)",
  "GapsRisks": "short missing must-haves / blockers (max 300 chars)"
}}

Method (follow internally, but output only JSON):
1) Extract 3-6 MUST-HAVES from the job description (skills/experience/constraints).
2) Extract 2-5 NICE-TO-HAVES.
3) Compare MUST-HAVES to the candidate profile summary:
   - Count how many MUST-HAVES clearly match.
   - Identify the top missing MUST-HAVES.
4) Apply scoring rules:

Hard blockers: if clearly required, FitScore must be 1 or 2 and NextAction must be "Skip":
- Security clearance required
- Strict on-site outside candidate regions with no remote option
- Mandatory unrelated license/certification

Score mapping:
- FitScore=5 if MOST (>=80%) must-haves match and no major risks.
- FitScore=4 if MANY (>=60%) must-haves match and gaps are manageable.
- FitScore=3 only if match is truly mixed/unclear (around half match or JD is vague).
- FitScore=2 if FEW (<40%) must-haves match or major must-have gaps.
- FitScore=1 if clearly irrelevant or blocked.

NextAction rules:
- Apply Now: FitScore=5 or strong 4 with low risks
- Apply: FitScore=4 or high-confidence 3 with manageable gaps
- Network First: FitScore=3 with notable gaps OR competitive role where referral likely helps
- Skip: FitScore=1–2 OR any hard blocker

Evidence rule:
FitReasons must cite 2-3 concrete items from the job description that match the candidate profile.
GapsRisks must cite 1-3 concrete missing must-haves or blockers from the job description.
Do not invent candidate details beyond the provided profile summary.

JobDescriptionText:
{job_description}
"""


def load_env_vars():
    """Load and validate required environment variables."""
    api_key = os.getenv("AIRTABLE_TOKEN")
    base_id = os.getenv("AIRTABLE_BASE_ID")
    table_name = os.getenv("AIRTABLE_TABLE_JOBS")
    openai_key = os.getenv("OPENAI_API_KEY")
    
    if not all([api_key, base_id, table_name, openai_key]):
        missing = []
        if not api_key:
            missing.append("AIRTABLE_TOKEN")
        if not base_id:
            missing.append("AIRTABLE_BASE_ID")
        if not table_name:
            missing.append("AIRTABLE_TABLE_JOBS")
        if not openai_key:
            missing.append("OPENAI_API_KEY")
        raise RuntimeError(f"Missing env vars: {', '.join(missing)}")
    
    return api_key, base_id, table_name, openai_key


def clean_job_text(raw: str) -> str:
    """
    Clean HTML-encoded job description text.
    Remove script/style blocks, HTML tags, unescape entities, collapse whitespace.
    """
    if not raw:
        return ""
    
    text = raw
    text = re.sub(r'<script[^>]*>.*?</script>', '', text, flags=re.IGNORECASE | re.DOTALL)
    text = re.sub(r'<style[^>]*>.*?</style>', '', text, flags=re.IGNORECASE | re.DOTALL)
    text = re.sub(r'<[^>]+>', '', text)
    text = html_module.unescape(text)
    text = re.sub(r'\s+', ' ', text)
    return text.strip()


def airtable_list_unscored(airtable, table_name, batch_size=15):
    """
    Fetch unscored job records from Airtable.
    Only reads JobDescriptionRaw and FitScore fields.
    Filter: JobDescriptionRaw present (LEN > 0) AND FitScore blank.
    Returns list of records, max batch_size items.
    """
    filter_formula = "AND(LEN({JobDescriptionRaw})>0, {FitScore}='')"
    records = airtable.list_records(
        table=table_name,
        max_records=batch_size,
        filter_by_formula=filter_formula
    )
    return records


def score_job_description(job_description, openai_client):
    """
    Score a job description using OpenAI.
    Cleans HTML from description and sends ONLY clean text to OpenAI.
    Returns normalized dict: fit_score (1-5), next_action (validated), fit_reasons, gaps_risks.
    """
    cleaned = clean_job_text(job_description)
    user_prompt = USER_PROMPT_TEMPLATE.format(
        candidate_profile=CANDIDATE_PROFILE_SUMMARY,
        job_description=cleaned
    )
    response_text = openai_client.request_json(
        model="gpt-4o-mini",
        prompt=user_prompt,
        max_tokens=300,
        temperature=0.0
    )
    
    score_data = utils.safe_parse_json(response_text)
    if not score_data:
        raise ValueError("Failed to parse OpenAI response as JSON")
    
    fit_score = score_data.get("FitScore")
    try:
        fit_score = int(fit_score) if fit_score is not None else 3
        fit_score = max(1, min(5, fit_score))
    except (ValueError, TypeError):
        fit_score = 3
    
    next_action = score_data.get("NextAction", "").strip()
    valid_actions = {"Apply Now", "Apply", "Network First", "Skip"}
    if next_action not in valid_actions:
        next_action = "Skip"
    
    fit_reasons = score_data.get("FitReasons", "")
    fit_reasons_str = str(fit_reasons)[:300] if fit_reasons else ""
    
    gaps_risks = score_data.get("GapsRisks", "")
    gaps_risks_str = str(gaps_risks)[:300] if gaps_risks else ""
    
    return {
        "fit_score": fit_score,
        "next_action": next_action,
        "fit_reasons": fit_reasons_str,
        "gaps_risks": gaps_risks_str,
    }


def score_record(record, openai_client):
    """
    Score a single job record.
    Returns dict with status, fit_score, next_action, fit_reasons, gaps_risks.
    """
    record_id = record.get("id")
    fields = record.get("fields", {})
    
    job_description = fields.get("JobDescriptionRaw", "").strip()
    
    if not job_description:
        return {"status": "skipped", "reason": "no_description"}
    
    if fields.get("FitScore"):
        return {"status": "skipped", "reason": "already_scored"}
    
    try:
        score_result = score_job_description(job_description, openai_client)
        return {
            "status": "scored",
            "fit_score": score_result["fit_score"],
            "next_action": score_result["next_action"],
            "fit_reasons": score_result["fit_reasons"],
            "gaps_risks": score_result["gaps_risks"],
        }
    except Exception as e:
        return {"status": "error", "reason": str(e)}


def airtable_update_record(airtable, table_name, record_id, score_result):
    """
    Update Airtable record with scored fields.
    PATCH only: FitScore, NextAction, FitReasons, GapsRisks.
    Raises exception on failure.
    """
    update_fields = {
        "FitScore": score_result["fit_score"],
        "NextAction": score_result["next_action"],
    }
    if score_result["fit_reasons"]:
        update_fields["FitReasons"] = score_result["fit_reasons"]
    if score_result["gaps_risks"]:
        update_fields["GapsRisks"] = score_result["gaps_risks"]
    
    airtable.update_record(table_name, record_id, update_fields)


def self_test():
    """
    Run deterministic self-test on three hardcoded samples.
    Print results and exit cleanly.
    """
    samples = [
        {
            "name": "strong_fit",
            "text": "We need a Python Automation Engineer to build ETL pipelines, integrate APIs, write scripts for data ingestion, maintain logging, and improve reliability of batch jobs. Experience with requests, JSON, and basic SQL required.",
        },
        {
            "name": "weak_fit",
            "text": "Seeking a Senior iOS Engineer to build SwiftUI apps, manage Apple Store releases, and optimize UIKit performance. Must have 5+ years Swift and Xcode.",
        },
        {
            "name": "hard_blocker",
            "text": "Data Engineer position supporting government systems. Active TS/SCI clearance required. On-site daily in Denver, CO. No remote.",
        },
    ]
    
    try:
        openai_key = os.getenv("OPENAI_API_KEY")
        if not openai_key:
            raise RuntimeError("OPENAI_API_KEY not set")
        
        openai = OpenAIClient(openai_key)
        
        for sample in samples:
            result = score_job_description(sample["text"], openai)
            print(f"SampleName={sample['name']} FitScore={result['fit_score']} NextAction={result['next_action']}")
        
        return 0
    except Exception as e:
        print(f"FAIL {e}")
        return 1


def peek_records(peek_count):
    """
    Query Airtable for unscored records and print first N.
    Print record ID and first 120 chars of JobDescriptionRaw.
    Do not call OpenAI or update Airtable.
    """
    try:
        api_key, base_id, table_name, openai_key = load_env_vars()
        
        airtable = AirtableClient(api_key, base_id)
        records = airtable_list_unscored(airtable, table_name, batch_size=peek_count)
        
        for record in records:
            record_id = record.get("id")
            fields = record.get("fields", {})
            job_description = fields.get("JobDescriptionRaw", "")
            preview = job_description[:120] if job_description else "(empty)"
            print(f"{record_id}: {preview}")
        
        return 0
    except Exception as e:
        print(f"FAIL {e}")
        return 1


def run_scoring(max_records: int = 10, dry_run: bool = False, shortlist_min_score: int = 4, scorer_engine: str = "v1") -> dict:
    """
    Runs scoring for Airtable records where JobDescriptionRaw exists and FitScore is empty.
    Delegates to score_new_jobs.run_scoring() for the actual scoring logic.
    Returns {"scored": <int>, "errors": <int>}.
    """
    print("SCORER_WRAPPER=score_existing_jobs -> score_new_jobs")
    
    if shortlist_min_score != 4:
        print(f"WARN shortlist_min_score_ignored={shortlist_min_score}")
    
    # Load environment and create required dependencies
    api_key, base_id, table_name, openai_key = load_env_vars()
    
    settings = config.settings
    settings.validate()
    
    airtable = AirtableClient(api_key, base_id)
    openai = OpenAIClient(openai_key)
    
    # Delegate to score_new_jobs.run_scoring
    result = score_new_jobs.run_scoring(
        settings=settings,
        airtable=airtable,
        openai=openai,
        max_jobs=max_records,
        dry_run=dry_run,
        scorer_engine=scorer_engine
    )
    
    # Return format expected by daily_run.py
    return {"scored": result["scored"], "errors": result["errors"]}


def main():
    parser = argparse.ArgumentParser(description="Score existing jobs with JobDescriptionRaw")
    parser.add_argument("--max", type=int, default=10, help="Maximum number of jobs to score")
    parser.add_argument("--dry-run", action="store_true", help="Run without Airtable updates")
    parser.add_argument("--self-test", action="store_true", help="Run deterministic self-test and exit")
    parser.add_argument("--peek", type=int, metavar="N", help="Query and preview first N unscored records, then exit")
    parser.add_argument("--shortlist-min-score", type=int, default=4, help="Minimum FitScore for shortlisting (default 4)")
    args = parser.parse_args()
    
    if args.self_test:
        return self_test()
    
    if args.peek is not None:
        return peek_records(args.peek)
    
    try:
        api_key, base_id, table_name, openai_key = load_env_vars()
        batch_size = int(os.getenv("BATCH_SIZE", 15))
        
        airtable = AirtableClient(api_key, base_id)
        openai = OpenAIClient(openai_key)
        
        model = "gpt-4o-mini"
        print(f"model={model} batch_size={batch_size}")
        
        max_records = min(args.max, batch_size) if args.max else batch_size
        records = airtable_list_unscored(airtable, table_name, batch_size=max_records)
        
        total = len(records)
        scored = 0
        skipped = 0
        failed = 0
        shortlisted = 0
        
        print(f"Found {total}")
        
        for i, record in enumerate(records, 1):
            record_id = record.get("id")
            fields = record.get("fields", {})
            
            job_description = fields.get("JobDescriptionRaw", "").strip()
            
            if not job_description:
                skipped += 1
                continue
            
            if fields.get("FitScore"):
                skipped += 1
                continue
            
            result = score_record(record, openai)
            
            if result["status"] == "scored":
                if not args.dry_run:
                    try:
                        airtable_update_record(airtable, table_name, record_id, result)
                    except Exception as e:
                        failed += 1
                        continue
                
                is_shortlisted = result["fit_score"] >= args.shortlist_min_score
                if is_shortlisted:
                    shortlisted += 1
                
                scored += 1
                print(f"Record {record_id}: FitScore={result['fit_score']} NextAction={result['next_action']} shortlisted={str(is_shortlisted).lower()}")
            elif result["status"] == "skipped":
                skipped += 1
            else:
                failed += 1
            
            if i < total:
                time.sleep(1)
        
        print(f"scored={scored} skipped={skipped} failed={failed} shortlisted={shortlisted}")
        return 0
    except Exception as e:
        print(f"FAIL {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
