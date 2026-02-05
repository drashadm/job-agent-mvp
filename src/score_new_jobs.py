#!/usr/bin/env python3
"""
Score new RSS-ingested jobs that don't have FitScore yet.
Queries Jobs table for Status="New" (or blank) and FitScore empty,
then runs parse+score logic reusing pipeline functions.
"""
import argparse
import sys
import time
import json
import re
import html
import os
import hashlib
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv()

import config
from airtable_client import AirtableClient
from openai_client import OpenAIClient
import prompts
import utils

# Minimum cleaned text length required for scoring
CLEAN_MIN_LEN = 600


def generate_run_id() -> str:
    """Generate unique RunID in format RUN_<UTC_ISO_TIMESTAMP>."""
    now_utc = datetime.now(timezone.utc)
    timestamp_str = now_utc.strftime("%Y-%m-%dT%H-%M-%SZ")
    return f"RUN_{timestamp_str}"


def load_scorer_prompt(engine_name: str) -> str:
    """Load prompt from file for given scorer engine. Fail fast if missing."""
    if engine_name == "v1":
        return prompts.JOB_SCORER_PROMPT_V1
    elif engine_name == "perfecter_v1":
        prompt_file = os.path.join(os.path.dirname(__file__), "..", "prompts", "job_scorer_perfecter_v1.txt")
        if not os.path.exists(prompt_file):
            print(f"PROMPT_FILE_MISSING engine={engine_name} path={prompt_file}")
            raise FileNotFoundError(f"Prompt file not found: {prompt_file}")
        try:
            with open(prompt_file, "r", encoding="utf-8") as f:
                return f.read()
        except UnicodeDecodeError as e:
            print(f"PROMPT_DECODE_ERROR engine={engine_name} path={prompt_file} error={str(e)[:100]} suggestion='Resave file as UTF-8'")
            raise
    else:
        raise ValueError(f"Unknown scorer engine: {engine_name}")


def get_prompt_hash(prompt_text: str) -> str:
    """Compute SHA256 hash of prompt text."""
    return hashlib.sha256(prompt_text.encode("utf-8")).hexdigest()[:16]


def is_hard_gate_failure(result: dict, engine_name: str) -> bool:
    """
    Detect if a scorer result indicates a hard-gate failure (should output fit_score=1).
    
    For perfecter_v1:
      - debug.hard_gates_passed == false, OR
      - fit_score == 1 with debug.hard_gate_fail_reasons non-empty
    
    For v1 (fallback heuristic):
      - fit_score == 1 AND (gaps_risks or needs_human_input) contains clearance/travel keywords
    
    Args:
        result: scorer result dict with raw_score, fit_score, gaps_risks, needs_human_input
        engine_name: name of the engine (v1, perfecter_v1, etc.)
    
    Returns:
        True if hard-gate failure detected, False otherwise
    """
    fit_score = result.get("fit_score", 0)
    raw_score = result.get("raw_score", {})
    debug = raw_score.get("debug", {})
    
    if engine_name == "perfecter_v1":
        # perfecter_v1 explicit hard-gate detection
        hard_gates_passed = debug.get("hard_gates_passed", True)
        hard_gate_fail_reasons = debug.get("hard_gate_fail_reasons", [])
        
        if not hard_gates_passed:
            return True
        if fit_score == 1 and hard_gate_fail_reasons:
            return True
    elif engine_name == "v1":
        # v1 fallback heuristic: fit_score=1 + clearance/travel keywords
        if fit_score == 1:
            gaps_risks = result.get("gaps_risks", [])
            needs_human_input = result.get("needs_human_input", [])
            combined_text = " ".join(gaps_risks + needs_human_input).lower()
            
            clearance_keywords = ["clearance", "security clearance", "government"]
            travel_keywords = ["travel", "relocation", "on-site"]
            gate_keywords = clearance_keywords + travel_keywords
            
            if any(kw in combined_text for kw in gate_keywords):
                return True
    
    return False


def select_ab_winner(results: dict, ab_test_engines: list, selected_ab_engine: str = None) -> str:
    """
    Deterministic A/B winner selection based on hard-gate failure detection.
    
    Rule:
    - If either output indicates hard-gate failure, winner must be the one with fit_score=1 ("Skip")
    - If both skip, choose perfecter_v1
    - Else winner = higher fit_score
    - If tie, winner = perfecter_v1
    
    Args:
        results: dict mapping engine_name -> scorer result
        ab_test_engines: list of engine names that were run
        selected_ab_engine: optional override (legacy, ignored for deterministic selection)
    
    Returns:
        Name of winning engine
    """
    # Detect hard-gate failures
    hard_gate_failures = {}
    for engine in ab_test_engines:
        if engine in results:
            hard_gate_failures[engine] = is_hard_gate_failure(results[engine], engine)
    
    # Count how many hard-gate failures
    skip_engines = [e for e, is_fail in hard_gate_failures.items() if is_fail]
    
    if skip_engines:
        # At least one engine detected hard-gate failure
        if len(skip_engines) == len(hard_gate_failures):
            # Both skip: choose perfecter_v1 if available, else first
            return "perfecter_v1" if "perfecter_v1" in skip_engines else skip_engines[0]
        else:
            # Only one skips: winner is the one with fit_score=1
            skip_engine = skip_engines[0]
            if results[skip_engine]["fit_score"] == 1:
                return skip_engine
            # Fallback: shouldn't happen, but pick first skip
            return skip_engine
    
    # No hard-gate failures: winner by highest fit_score
    max_score = -1
    winner = None
    for engine in ab_test_engines:
        if engine in results:
            fit_score = results[engine]["fit_score"]
            if fit_score > max_score:
                max_score = fit_score
                winner = engine
            elif fit_score == max_score and engine == "perfecter_v1":
                # Tie-breaker: prefer perfecter_v1
                winner = engine
    
    return winner if winner else list(results.keys())[0]


def build_scoring_ab_json(results: dict, ab_test_engines: list, winner_engine: str, scorer_input: dict) -> str:
    """
    Build and serialize A/B scoring results as JSON string.
    
    Args:
        results: dict mapping engine_name -> scorer result dict with full 'raw_score'
        ab_test_engines: list of engine names that were run
        winner_engine: name of winning engine
        scorer_input: dict containing runtime info (model, temperature, timestamp)
    
    Returns:
        Serialized JSON string, potentially truncated if > 90000 chars
    """
    # Build output object with engines, winner, hashes, runtime
    winner_result = results.get(winner_engine, {})
    output = {
        "engines": {},
        "winner": winner_engine,
        "winner_fit_score": winner_result.get("fit_score"),
        "winner_next_action": winner_result.get("next_action"),
        "hashes": {},
        "runtime": {
            "model": scorer_input["runtime"].get("model", "unknown"),
            "temperature": scorer_input["runtime"].get("temperature", 0.0),
            "timestamp_utc": scorer_input["runtime"].get("timestamp_utc", datetime.now(timezone.utc).isoformat()),
        }
    }
    
    # Collect engines and hashes
    for engine_name in ab_test_engines:
        if engine_name in results:
            result = results[engine_name]
            # Store the full raw_score output
            output["engines"][engine_name] = result.get("raw_score", {})
            # Compute and store hash of the prompt for this engine
            try:
                prompt = load_scorer_prompt(engine_name)
                output["hashes"][engine_name] = get_prompt_hash(prompt)
            except Exception as e:
                output["hashes"][engine_name] = f"error:{str(e)[:50]}"
    
    # Serialize with compact format
    json_string = json.dumps(output, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
    
    # Handle truncation if too large
    max_len = 90000
    if len(json_string) > max_len:
        # Add truncation metadata
        output["TRUNCATED"] = True
        output["TRUNCATED_REASON"] = f"JSON size {len(json_string)} exceeded max {max_len}"
        # Remove some engine details to reduce size
        for engine_name in list(output["engines"].keys()):
            if engine_name != winner_engine and "debug" in output["engines"][engine_name]:
                del output["engines"][engine_name]["debug"]
        json_string = json.dumps(output, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
        
        # If still too large, keep only winner
        if len(json_string) > max_len:
            output_minimal = {
                "engines": {winner_engine: output["engines"][winner_engine]},
                "winner": winner_engine,
                "hashes": output["hashes"],
                "runtime": output["runtime"],
                "TRUNCATED": True,
                "TRUNCATED_REASON": f"Aggressive truncation: kept only winner engine",
            }
            json_string = json.dumps(output_minimal, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
    
    return json_string


def run_single_scorer(scorer_engine: str, scorer_input: dict, openai_client, job_record_id: str) -> dict:
    """Run a single scorer and return parsed result with metadata."""
    prompt = load_scorer_prompt(scorer_engine)
    full_prompt = prompt + "\n\nINPUT_JSON:\n" + json.dumps(scorer_input, ensure_ascii=False)
    score_text = openai_client.request_json(model=scorer_input["runtime"]["model"], prompt=full_prompt, temperature=scorer_input["runtime"]["temperature"])
    score = utils.safe_parse_json(score_text)
    
    # Validate schema and retry once if invalid
    required_keys = {
        "fit_score", "next_action", "fit_reasons", "gaps_risks",
        "non_obvious_matches", "keywords_to_tailor_resume",
        "questions_to_verify", "confidence", "needs_human_input", "debug"
    }
    
    if not score or not required_keys.issubset(score.keys()):
        # Retry with strict instruction
        retry_prompt = full_prompt + "\n\nYou produced invalid JSON or missing keys. Output STRICT JSON ONLY matching the exact schema."
        score_text = openai_client.request_json(model=scorer_input["runtime"]["model"], prompt=retry_prompt, temperature=scorer_input["runtime"]["temperature"])
        score = utils.safe_parse_json(score_text)
        
        # If still invalid, fail
        if not score or not required_keys.issubset(score.keys()):
            print(f"FAIL_BAD_MODEL_OUTPUT record_id={job_record_id} engine={scorer_engine}")
            return {"status": "error", "reason": "invalid_model_output", "engine": scorer_engine}
    
    # Extract and normalize fit_score
    fit_score_int = score.get("fit_score")
    if fit_score_int is None:
        fit_score_int = score.get("score") or score.get("FitScore") or score.get("fitScore")
    try:
        fit_score_int = int(fit_score_int) if fit_score_int is not None else 3
    except (ValueError, TypeError):
        fit_score_int = 3
    fit_score_int = max(1, min(5, fit_score_int))
    
    # Extract and normalize next_action
    next_action_raw = score.get("next_action", "").strip() if isinstance(score.get("next_action"), str) else None
    valid_actions = {"Apply Now", "Apply", "Network First", "Skip"}
    if next_action_raw not in valid_actions:
        # Map deterministically by fit_score
        if fit_score_int == 5:
            next_action_target = "Apply Now"
        elif fit_score_int == 4:
            next_action_target = "Apply"
        elif fit_score_int in (3, 2):
            next_action_target = "Network First"
        else:
            next_action_target = "Skip"
    else:
        next_action_target = next_action_raw
    
    confidence = score.get("confidence", "N/A")
    try:
        if confidence != "N/A":
            confidence = float(confidence)
    except (ValueError, TypeError):
        confidence = "N/A"
    
    print(f"SCORER_RESULT engine={scorer_engine} fit={fit_score_int} action={next_action_target} conf={confidence}")
    
    return {
        "engine": scorer_engine,
        "status": "scored",
        "fit_score": fit_score_int,
        "next_action": next_action_target,
        "confidence": confidence,
        "raw_score": score,
        "fit_reasons": score.get("fit_reasons", []),
        "gaps_risks": score.get("gaps_risks", []),
        "needs_human_input": score.get("needs_human_input", []),
    }


def _today_date() -> str:
    return datetime.now().date().isoformat()


def _candidate_fields(candidate_record):
    if not candidate_record:
        return {}
    return candidate_record.get("fields", {}) or {}


def filter_fields_to_table(payload, table_field_names):
    """Filter payload to only include fields that exist in the table schema.
    Prints FIELD_DROP for any dropped fields.
    """
    filtered = {}
    for key, value in payload.items():
        if key in table_field_names:
            filtered[key] = value
        else:
            print(f"FIELD_DROP {key}")
    return filtered


def sample_valid_fields(airtable_client, table_name: str) -> set:
    """Sample 3 records (no filter) and union their field keys to get VALID_FIELDS.
    Returns: set of field names present in sampled records
    """
    records = airtable_client.list_records(table=table_name, max_records=3)
    valid_fields = set()
    for record in records:
        valid_fields.update(record.get("fields", {}).keys())
    return valid_fields


def clean_html_to_text(raw: str) -> str:
    """Convert HTML into clean plain text with bullet and newline preservation."""
    if not raw:
        return ""

    text = raw
    # Remove script/style blocks
    text = re.sub(r'<script[^>]*>.*?</script>', '', text, flags=re.IGNORECASE | re.DOTALL)
    text = re.sub(r'<style[^>]*>.*?</style>', '', text, flags=re.IGNORECASE | re.DOTALL)
    # Remove images
    text = re.sub(r'<img[^>]*>', '', text, flags=re.IGNORECASE)
    # Convert line breaks to newlines
    text = re.sub(r'<br\s*/?>', '\n', text, flags=re.IGNORECASE)
    # Convert list items to bullet lines
    text = re.sub(r'<li[^>]*>', '- ', text, flags=re.IGNORECASE)
    text = re.sub(r'</li>', '\n', text, flags=re.IGNORECASE)
    # Strip remaining tags
    text = re.sub(r'<[^>]+>', '', text)
    # HTML unescape
    text = html.unescape(text)
    # Normalize newlines
    text = text.replace('\r\n', '\n').replace('\r', '\n')
    # Trim line whitespace
    lines = [line.strip() for line in text.split('\n')]
    text = '\n'.join(lines)
    # Collapse excessive blank lines
    text = re.sub(r'\n\s*\n+', '\n\n', text)
    return text.strip()


def clean_job_text(raw: str) -> str:
    """Backward-compatible alias for HTML cleaning."""
    return clean_html_to_text(raw)


def build_flags_from_jd(clean_text: str) -> dict:
    """Build JobJSON.flags from cleaned job description text."""
    flags = {
        "requires_clearance": False,
        "clearance_type": "",
        "requires_us_citizenship": False,
        "requires_travel": False,
        "travel_percent": None,
        "gov_or_defense": False,
        "people_management": False,
        "phd_required": False,
        "research_only": False,
    }

    if not clean_text:
        return flags

    t = clean_text

    if re.search(r"ts\s*/\s*sci|ts\s*sci|top secret|secret clearance|security clearance|active clearance|must have (?:a )?clearance|clearance required", t, flags=re.IGNORECASE):
        flags["requires_clearance"] = True

    if re.search(r"ts\s*/\s*sci|ts\s*sci|top secret", t, flags=re.IGNORECASE):
        flags["clearance_type"] = "TS/SCI"
    elif re.search(r"secret clearance", t, flags=re.IGNORECASE):
        flags["clearance_type"] = "Secret"
    elif re.search(r"security clearance", t, flags=re.IGNORECASE):
        flags["clearance_type"] = "Clearance"

    if re.search(r"\b(u\.s\.|us)\s*citizen(ship)?\b|citizenship required|must be a\s+us\s+citizen", t, flags=re.IGNORECASE):
        flags["requires_us_citizenship"] = True

    travel_percent_match = re.search(r"(?:up\s*to\s*)?(\d{1,3})\s*%\s*travel", t, flags=re.IGNORECASE)
    if not travel_percent_match:
        travel_percent_match = re.search(r"travel\s*up\s*to\s*(\d{1,3})\s*%", t, flags=re.IGNORECASE)
    if travel_percent_match:
        try:
            flags["travel_percent"] = int(travel_percent_match.group(1))
        except (ValueError, TypeError):
            flags["travel_percent"] = None

    no_travel_match = re.search(r"\b(no travel|travel not required)\b", t, flags=re.IGNORECASE)
    travel_required_match = False
    if travel_percent_match:
        travel_required_match = True
    elif re.search(r"travel required|travel as needed|%\s*travel|up\s*to\s*(?:\d{1,3}\s*%)?\s*travel", t, flags=re.IGNORECASE):
        travel_required_match = True

    if no_travel_match:
        flags["requires_travel"] = False
    elif travel_required_match:
        flags["requires_travel"] = True

    if re.search(r"\b(dod|department of defense|defense|federal|government|public sector)\b", t, flags=re.IGNORECASE):
        flags["gov_or_defense"] = True

    if re.search(r"manage\s+a\s+team|people\s+manager|direct\s+reports|management\s+experience|team\s+lead", t, flags=re.IGNORECASE):
        flags["people_management"] = True

    if re.search(r"phd\s+required|doctorate\s+required|ph\.d\.?\s+required", t, flags=re.IGNORECASE):
        flags["phd_required"] = True

    if re.search(r"\bresearch\b", t, flags=re.IGNORECASE) and not re.search(
        r"\b(deploy|production|ship|implementation|integration)\b",
        t,
        flags=re.IGNORECASE,
    ):
        flags["research_only"] = True

    return flags


def score_job_record(job_record, settings, airtable_client, openai_client, candidate_for_scoring, table_field_names, valid_fields, dry_run=False, scorer_engine="perfecter_v1", ab_test=None, selected_ab_engine=None, run_id=None):
    """Parse and score a single job record. Returns dict with status."""
    job_record_id = job_record.get("id")
    fields = job_record.get("fields", {})
    
    job_url = fields.get("JobURL", "")
    
    # Helper to update fields only if they exist in valid_fields
    def safe_update(fields_dict: dict, step: str = "", fit_score: int = None, next_action: str = None) -> bool:
        """Filter and update fields. Return True if update succeeded."""
        filtered = filter_fields_to_table(fields_dict, table_field_names)
        if not filtered:
            return False
        if not dry_run:
            try:
                airtable_client.update_record(settings.AIRTABLE_TABLE_JOBS, job_record_id, filtered)
                wrote_keys = ','.join(sorted(filtered.keys()))
                # Special log for FitScore update with fit_score and next_action metadata
                if fit_score is not None and next_action is not None:
                    print(f"UPDATE_OK record_id={job_record_id} wrote={wrote_keys} FitScore={fit_score} NextAction={next_action}")
                elif step:
                    print(f"UPDATE_OK record_id={job_record_id} step={step} wrote={wrote_keys}")
                else:
                    print(f"UPDATE_OK record_id={job_record_id} wrote={wrote_keys}")
            except Exception as e:
                print(f"WARN: Update failed for step {step} record_id={job_record_id} error={str(e)[:200]}")
                retry_fields = {k: v for k, v in filtered.items() if k not in ("ScoringStatus", "SkipReason")}
                if retry_fields and retry_fields != filtered:
                    try:
                        airtable_client.update_record(settings.AIRTABLE_TABLE_JOBS, job_record_id, retry_fields)
                        wrote_keys = ','.join(sorted(retry_fields.keys()))
                        print(f"UPDATE_OK record_id={job_record_id} step={step} wrote={wrote_keys} retry=1")
                        return True
                    except Exception as e2:
                        print(f"WARN: Update retry failed for step {step} record_id={job_record_id} error={str(e2)[:200]}")
                return False
        return True
    
    # ONLY use JobDescriptionRaw - no other field names
    job_description_raw = fields.get("JobDescriptionRaw", "").strip()
    
    if not job_description_raw:
        # Mark as needing JD and prevent reprocessing
        print(f"SKIP_NO_JD record_id={job_record_id}")
        if not dry_run:
            try:
                update_fields = {
                    "Status": "Needs JD",
                    "ScoringStatus": "SKIPPED",
                    "SkipReason": "JobDescriptionRaw empty after stripping. Needs full JD."
                }
                update_fields = filter_fields_to_table(update_fields, table_field_names)
                if update_fields:
                    safe_update(update_fields, "skip_no_jd")
            except Exception as e:
                print(f"WARN: Failed to update skip_no_jd record_id={job_record_id} error={str(e)[:200]}")
        return {"status": "skipped", "reason": "no_description"}
    
    # ===== MANDATORY: Generate and store JobDescriptionText ====="
    job_description_text = fields.get("JobDescriptionText", "").strip()
    if not job_description_text:
        # Generate from raw
        job_description_cleaned = clean_html_to_text(job_description_raw)
        job_description_text = job_description_cleaned
        
        # Eligibility check: cleaned text must meet minimum length
        if len(job_description_text) < CLEAN_MIN_LEN:
            print(f"WARN_SHORT_JD record_id={job_record_id} len={len(job_description_text)}")
            if not dry_run:
                update_fields = {
                    "NeedsHumanInput": f"SHORT_JD_SCORING len={len(job_description_text)}"
                }
                update_fields = filter_fields_to_table(update_fields, table_field_names)
                if update_fields:
                    safe_update(update_fields, "short_jd_note")
        
        # Write JobDescriptionText to Airtable (with retry on failure)
        if not dry_run:
            try:
                update_fields = {"JobDescriptionText": job_description_text}
                update_fields = filter_fields_to_table(update_fields, table_field_names)
                if update_fields:
                    safe_update(update_fields, "jobtext")
                    print(f"JOBTEXT_OK record_id={job_record_id} len={len(job_description_text)}")
            except Exception as e:
                print(f"WARN: Failed to write JobDescriptionText record_id={job_record_id} error={str(e)[:200]}")
                # Retry once
                try:
                    update_fields = {"JobDescriptionText": job_description_text}
                    update_fields = filter_fields_to_table(update_fields, table_field_names)
                    if update_fields:
                        safe_update(update_fields, "jobtext")
                        print(f"JOBTEXT_OK record_id={job_record_id} len={len(job_description_text)} retry=1")
                except Exception as e2:
                    print(f"WARN: Retry failed to write JobDescriptionText record_id={job_record_id} error={str(e2)[:200]}")
        else:
            print(f"JOBTEXT_OK record_id={job_record_id} len={len(job_description_text)}")
    else:
        # Already has JobDescriptionText
        print(f"JOBTEXT_OK record_id={job_record_id} len={len(job_description_text)}")
    
    # Log raw vs clean
    job_description_cleaned = clean_html_to_text(job_description_raw)
    print(f"JD_CLEAN record_id={job_record_id} raw_len={len(job_description_raw)} clean_len={len(job_description_cleaned)}")
    
    # ===== MANDATORY: Ensure JobJSON exists ====="
    
    parsed = {}
    jobjson_raw = fields.get("JobJSON", "").strip()
    
    if jobjson_raw:
        # JobJSON already exists, use it
        parsed = utils.safe_parse_json(jobjson_raw) or {}
        if not isinstance(parsed, dict):
            parsed = {}
    
    if not parsed:
        # JobJSON doesn't exist or is invalid - build it
        source_label = "text"
        parse_prompt = prompts.JOB_PARSE_PROMPT.replace("<<JOB_DESCRIPTION>>", job_description_text)
        parsed_text = openai_client.request_json(model=settings.OPENAI_MODEL_PARSE, prompt=parse_prompt)
        parsed = utils.safe_parse_json(parsed_text) or {}
        
        required_keys = {
            "company", "job_title", "location", "remote_status", "seniority",
            "apply_type", "requirements", "responsibilities", "keywords",
            "tech_stack", "needs_human_input"
        }
        
        if not isinstance(parsed, dict) or not required_keys.issubset(parsed.keys()):
            # Parse failed: build minimal fallback JobJSON
            print(f"JOBJSON_PARSE_FAILED record_id={job_record_id}")
            parsed = {
                "job_title": fields.get("JobTitle") or fields.get("Title") or None,
                "company": None,
                "location": None,
                "remote_status": "Unknown",
                "seniority": "Unknown",
                "apply_type": "Unknown",
                "requirements": [],
                "responsibilities": [],
                "keywords": [],
                "tech_stack": [],
                "needs_human_input": ["JOBJSON_PARSE_FAILED"]
            }
            # Add to existing NeedsHumanInput
            if not dry_run:
                try:
                    update_fields = {"NeedsHumanInput": "JOBJSON_PARSE_FAILED"}
                    update_fields = filter_fields_to_table(update_fields, table_field_names)
                    if update_fields:
                        safe_update(update_fields, "jobjson_fail")
                except Exception as e:
                    print(f"WARN: Failed to update NeedsHumanInput record_id={job_record_id} error={str(e)[:200]}")
        
        # Write JobJSON to Airtable (mandatory)
        jobjson_string = json.dumps(parsed, ensure_ascii=False, separators=(",", ":"))
        print(f"JOBJSON_BUILD record_id={job_record_id} source={source_label} bytes={len(jobjson_string)}")
        if not dry_run:
            try:
                update_fields = {"JobJSON": jobjson_string}
                update_fields = filter_fields_to_table(update_fields, table_field_names)
                if update_fields:
                    safe_update(update_fields, "jobjson")
                    print(f"JOBJSON_OK record_id={job_record_id} bytes={len(jobjson_string)}")
            except Exception as e:
                print(f"WARN: Failed to write JobJSON record_id={job_record_id} error={str(e)[:200]}")
                # Retry once
                try:
                    update_fields = {"JobJSON": jobjson_string}
                    update_fields = filter_fields_to_table(update_fields, table_field_names)
                    if update_fields:
                        safe_update(update_fields, "jobjson")
                        print(f"JOBJSON_OK record_id={job_record_id} bytes={len(jobjson_string)} retry=1")
                except Exception as e2:
                    print(f"WARN: Retry failed to write JobJSON record_id={job_record_id} error={str(e2)[:200]}")
        else:
            print(f"JOBJSON_OK record_id={job_record_id} bytes={len(jobjson_string)}")
    else:
        print(f"JOBJSON_OK record_id={job_record_id} bytes={len(json.dumps(parsed, ensure_ascii=False, separators=(',', ':')))}")  
    try:
        requirements_text = utils.join_lines(parsed.get("requirements", []))
        responsibilities_text = utils.join_lines(parsed.get("responsibilities", []))
        keywords_text = utils.join_commas(parsed.get("keywords", []))
        tech_stack_text = utils.join_commas(parsed.get("tech_stack", []))
        needs_human_input_text = utils.join_lines(parsed.get("needs_human_input", []))
        
        # Update parsed fields - batch into single update if possible
        parsed_fields_to_update = {}
        parsed_field_names = [
            ("Company", parsed.get("company")),
            ("JobTitle", parsed.get("job_title")),
            ("Location", parsed.get("location")),
            ("RemoteStatus", parsed.get("remote_status")),
            ("Requirements", requirements_text),
            ("Responsibilities", responsibilities_text),
            ("Keywords", keywords_text),
            ("TechStack", tech_stack_text),
            ("NeedsHumanInput", needs_human_input_text),
        ]
        
        for field_name, field_value in parsed_field_names:
            if field_value:
                parsed_fields_to_update[field_name] = field_value
        
        # Filter to valid fields before updating
        if parsed_fields_to_update:
            parsed_fields_to_update = filter_fields_to_table(parsed_fields_to_update, table_field_names)
            if parsed_fields_to_update:
                safe_update(parsed_fields_to_update, "parse")
        
        # === NEW PROPRIETARY SCORER V1 ===
        # Build job_profile from parsed data
        flags = build_flags_from_jd(job_description_cleaned)
        print(f"FLAGS_OK record_id={job_record_id} flags={json.dumps(flags, ensure_ascii=False)}")
        job_profile = {
            "title": parsed.get("job_title") or "",
            "company": parsed.get("company") or "",
            "location": parsed.get("location") or "",
            "employment_type": parsed.get("remote_status") or "Unknown",
            "role_family_guess": parsed.get("seniority") or "Unknown",
            "responsibilities": parsed.get("responsibilities", []),
            "required_qualifications": parsed.get("requirements", []),
            "preferred_qualifications": [],
            "tech_stack": parsed.get("tech_stack", []),
            "industry": "",
            "flags": flags,
            "raw_text": job_description_cleaned
        }
        
        # Build scorer input dict (contract)
        scorer_input = {
            "candidate": {
                "candidate_profile": candidate_for_scoring
            },
            "job": {
                "job_profile": job_profile,
                "job_json": parsed
            },
            "runtime": {
                "creativity_dial": 0.7,
                "model": settings.OPENAI_MODEL_SCORE,
                "temperature": 0.7,
                "run_id": run_id or "",
            }
        }
        
        # Call scorer(s) based on mode
        if ab_test:
            # A/B testing mode: run multiple scorers
            results = {}
            for engine in ab_test:
                result = run_single_scorer(engine, scorer_input, openai_client, job_record_id)
                if result.get("status") == "scored":
                    results[engine] = result
            
            if not results:
                print(f"FAIL_BAD_MODEL_OUTPUT record_id={job_record_id}")
                return {"status": "error", "reason": "invalid_model_output"}
            
            # Deterministic winner selection based on hard-gate failures
            winner_engine = select_ab_winner(results, ab_test, selected_ab_engine)
            winner_result = results[winner_engine]
            print(f"AB_COMPARE record_id={job_record_id} engines={','.join(ab_test)} winner={winner_engine}")
            
            fit_score_int = winner_result["fit_score"]
            next_action_target = winner_result["next_action"]
            fit_reasons_text = utils.join_lines(winner_result["fit_reasons"])
            gaps_risks_text = utils.join_lines(winner_result["gaps_risks"])
            needs_human_2_text = utils.join_lines(winner_result["needs_human_input"])
        else:
            # Single scorer mode
            result = run_single_scorer(scorer_engine, scorer_input, openai_client, job_record_id)
            
            if result.get("status") != "scored":
                print(f"FAIL_BAD_MODEL_OUTPUT record_id={job_record_id}")
                return {"status": "error", "reason": "invalid_model_output"}
            
            fit_score_int = result["fit_score"]
            next_action_target = result["next_action"]
            fit_reasons_text = utils.join_lines(result["fit_reasons"])
            gaps_risks_text = utils.join_lines(result["gaps_risks"])
            needs_human_2_text = utils.join_lines(result["needs_human_input"])
        
        merged_needs = utils.merge_notes(needs_human_input_text, needs_human_2_text)
        
        # Track if any core update succeeds
        any_core_update_ok = False
        
        # Update FitScore and Status
        if not dry_run:
            update_dict = {"FitScore": fit_score_int, "Status": "Scored", "ScoringStatus": "SCORED"}
            if run_id:
                update_dict["RunID"] = run_id
            if safe_update(update_dict, fit_score=fit_score_int, next_action=next_action_target):
                any_core_update_ok = True
            
            # Update NextAction
            if safe_update({"NextAction": next_action_target}):
                any_core_update_ok = True
            else:
                # Fallback for NextAction if enum is invalid
                try:
                    fallback = {"NextAction": "Review"}
                    fallback = filter_fields_to_table(fallback, table_field_names)
                    if safe_update(fallback):
                        any_core_update_ok = True
                except Exception:
                    pass
            
            # Update optional fields
            optional_fields_c = {}
            if fit_reasons_text:
                optional_fields_c["FitReasons"] = fit_reasons_text
            if gaps_risks_text:
                optional_fields_c["GapsRisks"] = gaps_risks_text
            if merged_needs:
                optional_fields_c["NeedsHumanInput"] = merged_needs
            
            if optional_fields_c:
                optional_fields_c = filter_fields_to_table(optional_fields_c, table_field_names)
                if safe_update(optional_fields_c, "optional"):
                    any_core_update_ok = True
            
            # Update ScoringAB in A/B mode
            if ab_test:
                try:
                    ab_json_string = build_scoring_ab_json(results, ab_test, winner_engine, scorer_input)
                    ab_update = {"ScoringAB": ab_json_string}
                    ab_update = filter_fields_to_table(ab_update, table_field_names)
                    if ab_update:
                        safe_update(ab_update, "ab_scoring")
                        print(f"SCORING_AB_WRITE bytes={len(ab_json_string)} record_id={job_record_id} engines={','.join(ab_test)} winner={winner_engine}")
                except Exception as e:
                    print(f"WARN: Failed to write ScoringAB record_id={job_record_id} error={str(e)[:200]}")
        else:
            # Dry-run: print ScoringAB JSON metadata
            if ab_test:
                try:
                    ab_json_string = build_scoring_ab_json(results, ab_test, winner_engine, scorer_input)
                    print(f"SCORING_AB_WRITE bytes={len(ab_json_string)} record_id={job_record_id} engines={','.join(ab_test)} winner={winner_engine}")
                except Exception as e:
                    print(f"WARN: Failed to build ScoringAB record_id={job_record_id} error={str(e)[:200]}")
            
            # Only print SKIP if no core fields were written
            if not any_core_update_ok:
                print(f"SKIP_NO_VALID_FIELDS record_id={job_record_id}")
        
        return {
            "status": "scored",
            "fit_score": fit_score_int,
            "next_action": next_action_target,
        }
        
    except Exception as e:
        if not dry_run:
            try:
                update_fields = {
                    "ScoringStatus": "FAILED",
                    "SkipReason": f"Scoring error: {str(e)[:200]}"
                }
                update_fields = filter_fields_to_table(update_fields, table_field_names)
                missing_fields = [f for f in ("ScoringStatus", "SkipReason") if f not in table_field_names]
                if missing_fields:
                    print(f"WARN: schema_mismatch record_id={job_record_id} missing_fields={missing_fields} available_fields_count={len(valid_fields)}")
                if update_fields:
                    safe_update(update_fields, "score_error")
            except Exception as e:
                print(f"WARN: Failed to update score_error record_id={job_record_id} error={str(e)[:200]}")
        return {"status": "error", "reason": str(e)}


def run_scoring(settings, airtable, openai, max_jobs=10, dry_run=False, diag=False, diag_runid=False, test_filter=None, filter_step=None, scorer_engine="perfecter_v1", ab_test=None, selected_ab_engine=None):
    """Run scoring for new jobs and return dict with results.
    Returns: {"total": int, "scored": int, "skipped": int, "errors": int}
    """
    # Generate unique RunID for this execution
    run_id = generate_run_id()
    print(f"RUN_START id={run_id}")
    print("SCORER_ENGINE=score_new_jobs")
    
    # Log active scorer engine and prompt hash
    active_engines = ab_test if ab_test else [scorer_engine]
    for engine in active_engines:
        try:
            prompt = load_scorer_prompt(engine)
            prompt_hash = get_prompt_hash(prompt)
            print(f"ACTIVE_SCORER_ENGINE={engine}")
            print(f"ACTIVE_PROMPT_HASH_{engine}={prompt_hash}")
        except Exception as e:
            print(f"WARN: Failed to load prompt for engine={engine} error={str(e)[:100]}")
    
    # Sample VALID_FIELDS from first 3 records (no filter)
    valid_fields = sample_valid_fields(airtable, settings.AIRTABLE_TABLE_JOBS)
    valid_fields_sorted = sorted(valid_fields)
    print(f"FIELDS_OK count={len(valid_fields_sorted)} fields={','.join(valid_fields_sorted)}")
    
    # Static allowlist for fallback (not from Meta API)
    table_field_names = {
        "FitScore", "NextAction", "FitReasons", "GapsRisks", "NeedsHumanInput", "Status",
        "ScoringStatus", "SkipReason", "ProcessedAt",
        "Company", "Location", "RemoteStatus", "Requirements", "Responsibilities",
        "Keywords", "TechStack", "JobTitle", "JobURL", "JobDescriptionRaw", "JobDescriptionText", "JobJSON",
        "DateFound", "Source", "Strategy", "ScoringAB", "RunID",
    }
    
    if dry_run:
        print("DRY_RUN mode enabled - no Airtable updates will be made\n")
    
    # Warn if FitScore field missing
    if "FitScore" not in valid_fields:
        print("WARN_NO_FITSCORE_FIELD")
    
    # Query Jobs table: JobDescriptionRaw present AND FitScore empty
    # Filter formula includes length check: LEN({JobDescriptionRaw})>0
    filter_formula = "AND(LEN({JobDescriptionRaw})>0, {FitScore}=BLANK())"
    fallback_filter_formula = "AND(LEN({JobDescriptionRaw})>0, {FitScore}=BLANK())"

    if diag:
        base_id = settings.AIRTABLE_BASE_ID or ""
        if len(base_id) > 10:
            masked_base_id = f"{base_id[:6]}…{base_id[-4:]}"
        else:
            masked_base_id = base_id
        print(f"DIAG_BASE_ID_PRESENT={bool(settings.AIRTABLE_BASE_ID)} DIAG_BASE_ID_MASKED={masked_base_id}")
        print(f"DIAG_JOBS_TABLE={settings.AIRTABLE_TABLE_JOBS}")
        print(f"DIAG_FILTER_FORMULA={filter_formula}")
        print(f"DIAG_FIELDS={','.join(valid_fields_sorted)}")
        print(f"DIAG_HAS_ScoringStatus={'ScoringStatus' in valid_fields}")
        print(f"DIAG_HAS_SkipReason={'SkipReason' in valid_fields}")

    if diag_runid:
        # Print existing env diagnostics
        print(f"CWD: {os.getcwd()}")
        print(f".env exists: {os.path.exists('.env')}")
        print(f"AIRTABLE_BASE_ID loaded?: {bool(settings.AIRTABLE_BASE_ID)}")
        print(f"AIRTABLE_TOKEN loaded?: {bool(settings.AIRTABLE_TOKEN)}")
        
        # Mask base ID
        base_id = settings.AIRTABLE_BASE_ID or ""
        if len(base_id) > 10:
            masked_base_id = f"{base_id[:6]}…{base_id[-4:]}"
        else:
            masked_base_id = base_id
        print(f"DIAG_MASKED_BASE_ID={masked_base_id}")
        print(f"DIAG_TABLE_NAME={settings.AIRTABLE_TABLE_JOBS}")
        
        # Fetch up to 5 records to collect all field names
        try:
            sample_records = airtable.list_records(
                table=settings.AIRTABLE_TABLE_JOBS,
                max_records=5
            )
            
            # Collect union of all field keys
            all_fields = set()
            for record in sample_records:
                fields = record.get("fields", {})
                all_fields.update(fields.keys())
            
            all_fields_sorted = sorted(all_fields)
            print(f"DIAG_FIELD_COUNT={len(all_fields_sorted)}")
            print(f"DIAG_FIELDS={','.join(all_fields_sorted)}")
            
            # Check for exact RunID match
            has_runid_exact = "RunID" in all_fields
            print(f"DIAG_RUNID_EXACT_MATCH={has_runid_exact}")
            
            # Check for case-insensitive/normalized matches
            def normalize_field_name(name: str) -> str:
                # Remove all whitespace (including non-breaking spaces \xa0)
                name = ''.join(name.split())
                return name.lower()
            
            runid_like = []
            target_normalized = normalize_field_name("RunID")
            for field in all_fields:
                if normalize_field_name(field) == target_normalized and field != "RunID":
                    runid_like.append(field)
            
            print(f"DIAG_RUNID_LIKE={runid_like if runid_like else '[]'}")
            
        except Exception as e:
            print(f"DIAG_ERROR: {str(e)[:200]}")
        
        return {"total": 0, "scored": 0, "skipped": 0, "errors": 0}

    if test_filter:
        print(f"TEST_FILTER={test_filter}")
        records = airtable.list_records(
            table=settings.AIRTABLE_TABLE_JOBS,
            max_records=max_jobs,
            filter_by_formula=test_filter
        )
        record_ids = [r.get("id") for r in records]
        print(f"TEST_FILTER_RECORDS count={len(record_ids)} ids={','.join([rid for rid in record_ids if rid])}")
        return {"total": 0, "scored": 0, "skipped": 0, "errors": 0}

    if filter_step is not None:
        filters = {
            1: "AND(LEN({JobDescriptionRaw})>0)",
            2: "AND(LEN({JobDescriptionRaw})>0, {FitScore}=BLANK())",
            22: "AND(LEN({JobDescriptionRaw})>0, OR({FitScore}=BLANK(), {FitScore}=0))",
            3: 'AND(LEN({JobDescriptionRaw})>0, OR({FitScore}=BLANK(), {FitScore}=0), OR(LEN({ScoringStatus}&"")=0, {ScoringStatus}="NEW"))',
        }
        filter_formula_step = filters.get(filter_step)
        if not filter_formula_step:
            print(f"FILTER_STEP_INVALID step={filter_step}")
            return {"total": 0, "scored": 0, "skipped": 0, "errors": 1}
        records = airtable.list_records(
            table=settings.AIRTABLE_TABLE_JOBS,
            max_records=max_jobs,
            filter_by_formula=filter_formula_step
        )
        record_ids = [r.get("id") for r in records if r.get("id")]
        print(f"FILTER_STEP={filter_step} count={len(record_ids)} ids={','.join(record_ids[:3])}")
        return {"total": 0, "scored": 0, "skipped": 0, "errors": 0}

    # Load candidate profile
    candidate_record = airtable.find_one(
        table=settings.AIRTABLE_TABLE_CANDIDATE,
        id_field=settings.CANDIDATE_PROFILE_ID_FIELD,
        id_value=settings.CANDIDATE_PROFILE_ID_VALUE,
    )
    if not candidate_record:
        raise RuntimeError("CandidateProfile not found")
    
    candidate = _candidate_fields(candidate_record)
    candidate_json_str = candidate.get("CandidateJSON")
    
    # STRICT: Require CandidateJSON for proprietary scorer
    if not candidate_json_str:
        print("FAIL_NO_CANDIDATE_JSON")
        return {"total": 0, "scored": 0, "skipped": 0, "errors": 1}
    
    try:
        candidate_for_scoring = json.loads(candidate_json_str)
    except json.JSONDecodeError:
        print("FAIL_NO_CANDIDATE_JSON")
        return {"total": 0, "scored": 0, "skipped": 0, "errors": 1}

    print(f"=== QUERY FILTER ===")
    print(f"Filter formula: {filter_formula}")
    print(f"Max records: {max_jobs}")
    print(f"===================\n")
    
    try:
        records = airtable.list_records(
            table=settings.AIRTABLE_TABLE_JOBS,
            max_records=max_jobs,
            filter_by_formula=filter_formula
        )
    except Exception as e:
        print(f"WARN: Filter formula failed, using fallback filter error={str(e)[:200]}")
        records = airtable.list_records(
            table=settings.AIRTABLE_TABLE_JOBS,
            max_records=max_jobs,
            filter_by_formula=fallback_filter_formula
        )

    if diag:
        print(f"DIAG_RECORD_COUNT={len(records)}")
        return {"total": 0, "scored": 0, "skipped": 0, "errors": 0}
    
    for record in records:
        scoring_status = record.get("fields", {}).get("ScoringStatus")
        if scoring_status not in (None, "", "NEW"):
            print(f"WARN: filter_not_applied record_id={record.get('id')} scoringstatus={scoring_status}")
    
    total = len(records)
    scored = 0
    skipped = 0
    errors = 0
    
    print(f"Found {total} jobs to score (max={max_jobs})")
    
    for i, record in enumerate(records, 1):
        job_id = record.get("id")
        job_url = record.get("fields", {}).get("JobURL", "unknown")
        print(f"[{i}/{total}] Processing {job_id} ({job_url[:50]}...)")
        
        result = score_job_record(record, settings, airtable, openai, candidate_for_scoring, table_field_names, valid_fields, dry_run=dry_run, scorer_engine=scorer_engine, ab_test=ab_test, selected_ab_engine=selected_ab_engine, run_id=run_id)
        
        if result["status"] == "scored":
            scored += 1
            print(f"SCORED_OK record_id={job_id} fit={result['fit_score']} action={result.get('next_action')}")
        elif result["status"] == "skipped":
            skipped += 1
            print(f"  [SKIPPED] {result['reason']}")
        else:
            errors += 1
            print(f"  [ERROR] {result.get('reason', 'unknown')}")
        
        # Rate limiting: sleep 1s between jobs
        if i < total:
            time.sleep(1)
    
    return {
        "total": total,
        "scored": scored,
        "skipped": skipped,
        "errors": errors,
    }


def main():
    parser = argparse.ArgumentParser(description="Score new RSS jobs")
    parser.add_argument("--max", type=int, default=10, help="Maximum number of jobs to score")
    parser.add_argument("--dry-run", action="store_true", help="Run without making any Airtable updates")
    parser.add_argument("--schema-source", action="store_true", help="Print schema source (META or FALLBACK)")
    parser.add_argument("--diag", action="store_true", help="Print Airtable schema/runtime diagnostics and exit")
    parser.add_argument("--diag-runid", action="store_true", help="Print RunID diagnostics: base/table/fields and exit")
    parser.add_argument("--test-filter", type=str, default=None, help="Test filter formula and exit after listing record ids")
    parser.add_argument("--filter-step", type=int, default=None, help="Run progressive filter checks and exit")
    parser.add_argument("--scorer-engine", type=str, default="perfecter_v1", choices=["v1", "perfecter_v1"], help="Scorer engine to use (default: perfecter_v1)")
    parser.add_argument("--ab-test", type=str, default=None, help="A/B test mode: comma-separated list of engines (e.g., perfecter_v1,v1)")
    parser.add_argument("--ab-write-both", action="store_true", help="In A/B test mode, write both scorers' output (not yet implemented)")
    parser.add_argument("--print-prompt-hash", action="store_true", help="Print prompt hash and exit")
    args = parser.parse_args()
    
    # Parse A/B test engines if provided
    ab_test = None
    selected_ab_engine = None
    if args.ab_test:
        ab_test = [e.strip() for e in args.ab_test.split(",")]
        # Use first engine as the selected one for writing
        selected_ab_engine = ab_test[0]
    
    try:
        # Handle --print-prompt-hash early
        if args.print_prompt_hash:
            try:
                for engine in ["v1", "perfecter_v1"]:
                    prompt = load_scorer_prompt(engine)
                    full_hash = hashlib.sha256(prompt.encode("utf-8")).hexdigest()
                    print(f"PROMPT_HASH engine={engine} hash={full_hash}")
                return 0
            except Exception as e:
                print(f"PROMPT_HASH_ERROR error={str(e)[:100]}")
                return 1
        
        settings = config.settings
        settings.validate()
        
        airtable = AirtableClient(settings.AIRTABLE_TOKEN, settings.AIRTABLE_BASE_ID)
        openai = OpenAIClient(settings.OPENAI_API_KEY)
        
        result = run_scoring(settings, airtable, openai, max_jobs=args.max, dry_run=args.dry_run, diag=args.diag, diag_runid=args.diag_runid, test_filter=args.test_filter, filter_step=args.filter_step, scorer_engine=args.scorer_engine, ab_test=ab_test, selected_ab_engine=selected_ab_engine)
        
        print(f"\nSCORE_NEW_JOBS_OK total={result['total']} scored={result['scored']} skipped={result['skipped']} errors={result['errors']}")
        return 0
        
    except Exception as e:
        print(f"SCORE_NEW_JOBS_FAIL {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
