#!/usr/bin/env python3
"""
Daily run script that orchestrates RSS ingestion and job scoring.
Runs both stages and reports combined summary.
"""
import argparse
import sys
from dotenv import load_dotenv

load_dotenv()

import config
from airtable_client import AirtableClient
from openai_client import OpenAIClient
from rss_ingest import run_ingest
from score_existing_jobs import run_scoring


def main():
    parser = argparse.ArgumentParser(description="Daily RSS ingest + scoring run")
    parser.add_argument("--max-ingest", type=int, default=50, help="Maximum RSS items to ingest")
    parser.add_argument("--max-score", type=int, default=20, help="Maximum jobs to score")
    parser.add_argument("--scorer-engine", type=str, default="v1", help="Scorer engine to use (v1, perfecter_v1)")
    parser.add_argument("--dry-run", action="store_true", help="Run without Airtable updates")
    args = parser.parse_args()
    
    try:
        settings = config.settings
        settings.validate()
        
        airtable = AirtableClient(settings.AIRTABLE_TOKEN, settings.AIRTABLE_BASE_ID)
        openai = OpenAIClient(settings.OPENAI_API_KEY)
        
        print("=" * 60)
        print("DAILY RUN: Stage 1 - RSS Ingest")
        print("=" * 60)
        
        # Stage 1: RSS Ingest
        ingest_failed = False
        try:
            ingest_result = run_ingest(settings, airtable, max_items=args.max_ingest)
            print(f"\nRSS_INGEST_OK total={ingest_result['total']} created={ingest_result['created']} skipped={ingest_result['skipped']}")
            if ingest_result['first_created_id']:
                print(f"FIRST_CREATED_ID {ingest_result['first_created_id']}")
        except Exception as e:
            print(f"\n[FAIL] RSS_INGEST_HARD_FAIL {e}")
            # Exit 2 for RSS hard failure (feed unreachable, Airtable down, etc.)
            print(f"DAILY_RUN_SUMMARY RSS_TOTAL=0 RSS_CREATED=0 RSS_SKIPPED=0 SCORED_OK=0 SCORED_FAIL=0")
            return 2
        
        print("\n" + "=" * 60)
        print("DAILY RUN: Stage 2 - Score New Jobs")
        print("=" * 60)
        
        # Stage 2: Score new jobs
        try:
            score_result = run_scoring(max_records=args.max_score, dry_run=args.dry_run, shortlist_min_score=4, scorer_engine=args.scorer_engine)
            print(f"\nSCORE_NEW_JOBS_OK scored={score_result['scored']} errors={score_result['errors']}")
        except Exception as e:
            print(f"\n[FAIL] SCORE_STAGE_HARD_FAIL {e}")
            # Exit 3 for scoring hard failure (OpenAI unreachable, critical Airtable failure)
            print(f"DAILY_RUN_SUMMARY RSS_TOTAL={ingest_result['total']} RSS_CREATED={ingest_result['created']} RSS_SKIPPED={ingest_result['skipped']} SCORED_OK=0 SCORED_FAIL=0")
            return 3
        
        # Final summary - single line format
        print(f"\nDAILY_RUN_SUMMARY RSS_TOTAL={ingest_result['total']} RSS_CREATED={ingest_result['created']} RSS_SKIPPED={ingest_result['skipped']} SCORED_OK={score_result['scored']} SCORED_FAIL={score_result['errors']}")
        
        # Determine exit code
        # Exit 0 when: rss stage ran + score stage ran, and SCORED_FAIL == 0
        if score_result['errors'] == 0:
            print("[OK] DAILY_RUN_OK")
            return 0
        else:
            print(f"[WARN] DAILY_RUN_COMPLETED_WITH_ERRORS (SCORED_FAIL={score_result['errors']})")
            return 0  # Still exit 0 since per-job failures are not hard failures
        
    except Exception as e:
        print(f"\n[FAIL] DAILY_RUN_FAIL {e}")
        print(f"DAILY_RUN_SUMMARY RSS_TOTAL=0 RSS_CREATED=0 RSS_SKIPPED=0 SCORED_OK=0 SCORED_FAIL=0")
        return 1


if __name__ == "__main__":
    sys.exit(main())
