from __future__ import annotations

from typing import Any, Dict, Optional
from datetime import datetime, timezone
import json

import prompts
import utils


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _today_date() -> str:
    """Return today's date in YYYY-MM-DD format for Airtable date fields."""
    return datetime.now().date().isoformat()


def _airtable_link(record_id: str) -> list[str]:
    # Airtable linked-record fields expect a list of record IDs
    return [record_id]


def _candidate_fields(candidate_record: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    if not candidate_record:
        return {}
    return candidate_record.get("fields", {}) or {}


def _normalize_strategy(s: str) -> str:
    """
    Normalize strategy string to one of: Apply Now, Apply, Network First, Skip
    """
    if not s:
        return "Skip"
    s_lower = s.lower()
    if "apply now" in s_lower:
        return "Apply Now"
    elif "apply" in s_lower:
        return "Apply"
    elif "network" in s_lower:
        return "Network First"
    elif "skip" in s_lower or "reject" in s_lower:
        return "Skip"
    else:
        return "Skip"


def _make_event(
    *,
    airtable_client,
    events_table: str,
    job_record_id: str,
    event_type: str,
    actor: str,
    details: str,
) -> None:
    airtable_client.create_record(
        events_table,
        {
            "Job": _airtable_link(job_record_id),
            "EventType": event_type,
            "Actor": actor,
            "Timestamp": _now_iso(),
            "Details": details,
        },
    )


def run_pipeline(
    job_url: str,
    job_description_raw: str,
    settings,
    airtable_client,
    openai_client,
) -> Dict[str, Any]:
    """
    End-to-end MVP:
    1) Load CandidateProfile truth record
    2) Create Jobs record
    3) Parse JobDescriptionRaw -> structured fields
    4) Update Jobs -> Parsed
    5) Score fit -> update Jobs -> Scored
    6) Append Events log (INGESTED, PARSED, SCORED)
    """
    print("PIPELINE: start")

    # 1) Load CandidateProfile (single record by ProfileID=ME or your config)
    candidate_record = airtable_client.find_one(
        table=settings.AIRTABLE_TABLE_CANDIDATE,
        id_field=settings.CANDIDATE_PROFILE_ID_FIELD,
        id_value=settings.CANDIDATE_PROFILE_ID_VALUE,
    )
    if not candidate_record:
        raise RuntimeError(
            f"CandidateProfile not found. Ensure there is a record where "
            f"{settings.CANDIDATE_PROFILE_ID_FIELD} = {settings.CANDIDATE_PROFILE_ID_VALUE}"
        )
    candidate = _candidate_fields(candidate_record)
    print("PIPELINE: candidate loaded:", bool(candidate))

    # Check if CandidateJSON is available (enriched profile)
    candidate_json_str = candidate.get("CandidateJSON")
    if candidate_json_str:
        try:
            candidate_for_scoring = json.loads(candidate_json_str)
            print("PIPELINE: using CandidateJSON: True")
        except json.JSONDecodeError:
            candidate_for_scoring = candidate
            print("PIPELINE: using CandidateJSON: False (parse error)")
    else:
        candidate_for_scoring = candidate
        print("PIPELINE: using CandidateJSON: False")

    # 2) Create Job record in Airtable (Jobs table)
    print("PIPELINE: creating job record in Airtable...")
    # IMPORTANT: field names must match your Airtable Jobs schema.
    job_fields = {
        "JobURL": job_url,
        "JobDescriptionRaw": job_description_raw,
        "Source": "LinkedIn",
        "Status": "New",
        "NextAction": "Review",
        "DateFound": _today_date(),
    }
    created = airtable_client.create_record(settings.AIRTABLE_TABLE_JOBS, job_fields)
    job_record_id = created.get("id")
    if not job_record_id:
        raise RuntimeError("Airtable did not return a Job record id on create.")
    print("PIPELINE: job record created:", job_record_id)

    _make_event(
        airtable_client=airtable_client,
        events_table=settings.AIRTABLE_TABLE_EVENTS,
        job_record_id=job_record_id,
        event_type="INGESTED",
        actor="You",
        details="Created job record from intake.",
    )

    # 3) Parse job via OpenAI (JSON only)
    print("PIPELINE: calling OpenAI parse...")
    parse_prompt = prompts.JOB_PARSE_PROMPT.replace("<<JOB_DESCRIPTION>>", job_description_raw)
    parsed_text = openai_client.request_json(model=settings.OPENAI_MODEL_PARSE, prompt=parse_prompt)
    print("PIPELINE: OpenAI parse returned (chars):", len(parsed_text))
    parsed = utils.safe_parse_json(parsed_text) or {}

    # Normalize + flatten into Airtable-friendly formats
    requirements_text = utils.join_lines(parsed.get("requirements", []))
    responsibilities_text = utils.join_lines(parsed.get("responsibilities", []))
    keywords_text = utils.join_commas(parsed.get("keywords", []))
    tech_stack_text = utils.join_commas(parsed.get("tech_stack", []))
    needs_human_input_text = utils.join_lines(parsed.get("needs_human_input", []))

    # 4) Update Job with structured fields (Status -> Parsed)
    print("PIPELINE: updating Airtable with parsed fields...")
    update_fields = {
        "Company": parsed.get("company") or "",
        "JobTitle": parsed.get("job_title") or "",  # if your field is Title instead, change this key to "Title"
        "Location": parsed.get("location") or "",
        "RemoteStatus": parsed.get("remote_status") or "Unknown",
        "Seniority": parsed.get("seniority") or "Unknown",
        "ApplyType": parsed.get("apply_type") or "Unknown",
        "Requirements": requirements_text,
        "Responsibilities": responsibilities_text,
        "Keywords": keywords_text,
        "TechStack": tech_stack_text,
        "NeedsHumanInput": needs_human_input_text,
        "Status": "Parsed",
        "NextAction": "Review",
    }
    airtable_client.update_record(settings.AIRTABLE_TABLE_JOBS, job_record_id, update_fields)

    _make_event(
        airtable_client=airtable_client,
        events_table=settings.AIRTABLE_TABLE_EVENTS,
        job_record_id=job_record_id,
        event_type="PARSED",
        actor="AI_Agent",
        details="Parsed JobDescriptionRaw into structured fields.",
    )

    # 5) Score job fit via OpenAI (JSON only)
    print("PIPELINE: calling OpenAI scoring...")
    score_prompt = prompts.FIT_SCORING_PROMPT.replace("<<CANDIDATE_JSON>>", json.dumps(candidate_for_scoring, ensure_ascii=False)).replace("<<JOB_JSON>>", json.dumps(
            {
                "JobURL": job_url,
                "JobDescriptionRaw": job_description_raw,
                "Parsed": parsed,
            },
            ensure_ascii=False,
        ))
    score_text = openai_client.request_json(model=settings.OPENAI_MODEL_SCORE, prompt=score_prompt)
    print("PIPELINE: OpenAI scoring returned (chars):", len(score_text))
    score = utils.safe_parse_json(score_text) or {}

    # 6) Compute fit_score, strategy, next_action
    print("PIPELINE: computing fit score and strategy...")
    # Extract fit_score from response, enforce 1-5 range
    fit_score_int = score.get("fit_score")
    if fit_score_int is None:
        fit_score_int = score.get("score") or score.get("FitScore") or score.get("fitScore")
    try:
        fit_score_int = int(fit_score_int) if fit_score_int is not None else 3
    except (ValueError, TypeError):
        fit_score_int = 3
    fit_score_int = max(1, min(5, fit_score_int))
    
    # Deterministic mapping: FitScore -> Strategy + NextAction
    # (overrides any model output for consistency)
    if fit_score_int == 5:
        strategy_norm = "Apply Now"
        next_action_target = "Apply Now"
    elif fit_score_int == 4:
        strategy_norm = "Apply"
        next_action_target = "Apply"
    elif fit_score_int in (3, 2):
        strategy_norm = "Network First"
        next_action_target = "Network First"
    else:  # fit_score_int == 1
        strategy_norm = "Skip"
        next_action_target = "Skip"
    
    # Prepare optional field content
    fit_reasons_text = utils.join_lines(score.get("fit_reasons", []))
    gaps_risks_text = utils.join_lines(score.get("gaps_risks", []))
    needs_human_2_text = utils.join_lines(score.get("needs_human_input", []))
    merged_needs = utils.merge_notes(needs_human_input_text, needs_human_2_text)
    
    # Track actual next_action written (may differ if fallback needed)
    actual_next_action = next_action_target
    
    # STEP A1: Update FitScore (critical - must succeed)
    print("PIPELINE: updating Airtable step A1 (FitScore only)...")
    step_a1_update = {"FitScore": fit_score_int, "Status": "Scored"}
    try:
        airtable_client.update_record(settings.AIRTABLE_TABLE_JOBS, job_record_id, step_a1_update)
        print(f"A1_OK wrote FitScore={fit_score_int}")
    except Exception as e:
        print(f"AIRTABLE_UPDATE_FAIL step=A1 field=FitScore error={str(e)[:80]}")
    
    # STEP A2: Update Strategy (non-critical, silent fail)
    print(f"PIPELINE: updating Airtable step A2 (Strategy='{strategy_norm}')...")
    step_a2_update = {"Strategy": strategy_norm}
    try:
        airtable_client.update_record(settings.AIRTABLE_TABLE_JOBS, job_record_id, step_a2_update)
        print("PIPELINE: step A2 update successful")
    except Exception as e:
        error_msg = str(e)
        print(f"AIRTABLE_UPDATE_FAIL step=A2 field=Strategy error={error_msg[:80]}")
    
    # STEP B: Update NextAction (with fallback to "Review")
    print(f"PIPELINE: updating Airtable step B (NextAction='{next_action_target}')...")
    step_b_update = {"NextAction": next_action_target}
    try:
        airtable_client.update_record(settings.AIRTABLE_TABLE_JOBS, job_record_id, step_b_update)
        print("PIPELINE: step B update successful")
    except Exception as e:
        error_msg = str(e)
        if "INVALID_MULTIPLE_CHOICE_OPTIONS" in error_msg:
            print(f"AIRTABLE_UPDATE_FAIL step=B field=NextAction error=INVALID_OPTION_fallback_to_Review")
            step_b_update_fallback = {"NextAction": "Review"}
            try:
                airtable_client.update_record(settings.AIRTABLE_TABLE_JOBS, job_record_id, step_b_update_fallback)
                actual_next_action = "Review"
                print("PIPELINE: step B fallback update successful")
            except Exception as e2:
                print(f"AIRTABLE_UPDATE_FAIL step=B field=NextAction error=fallback_failed_{str(e2)[:40]}")
        else:
            print(f"AIRTABLE_UPDATE_FAIL step=B field=NextAction error={error_msg[:80]}")
    
    # STEP C: Update optional fields (only if present)
    optional_fields_c = {}
    if fit_reasons_text:
        optional_fields_c["FitReasons"] = fit_reasons_text
    if gaps_risks_text:
        optional_fields_c["GapsRisks"] = gaps_risks_text
    if merged_needs:
        optional_fields_c["NeedsHumanInput"] = merged_needs
    
    if optional_fields_c:
        print("PIPELINE: updating Airtable step C (optional fields)...")
        try:
            airtable_client.update_record(settings.AIRTABLE_TABLE_JOBS, job_record_id, optional_fields_c)
            print("PIPELINE: step C update successful")
        except Exception as e:
            print(f"AIRTABLE_UPDATE_FAIL step=C field=optional error={str(e)[:80]}")

    _make_event(
        airtable_client=airtable_client,
        events_table=settings.AIRTABLE_TABLE_EVENTS,
        job_record_id=job_record_id,
        event_type="SCORED",
        actor="AI_Agent",
        details=f"FitScore={fit_score_int} Strategy={strategy_norm}",
    )

    print("PIPELINE: done")
    print("PIPELINE: completed successfully")
    print(f"PIPELINE RESULT: JobRecordID={job_record_id} FitScore={fit_score_int} Strategy={strategy_norm} NextAction={actual_next_action}")
    return {"job_record": created, "parsed": parsed, "score": score, "job_record_id": job_record_id, "fit_score": fit_score_int, "strategy": strategy_norm, "next_action": actual_next_action}
