#!/usr/bin/env python
"""
Build and enrich CandidateProfile with AI-generated JSON and narrative.
"""

from typing import Dict, Any, Optional
import json
import prompts
from airtable_client import AirtableClient
from openai_client import OpenAIClient
import utils


def build_profile(
    settings,
    airtable_client: AirtableClient,
    openai_client: OpenAIClient,
) -> Dict[str, Any]:
    """
    Load CandidateProfile, generate CandidateJSON and CandidateProfilePackAI,
    write back to Airtable.
    """
    print("PROFILE_BUILDER: start")

    # 1) Load CandidateProfile record
    print("PROFILE_BUILDER: loading CandidateProfile...")
    candidate_record = airtable_client.find_one(
        table=settings.AIRTABLE_TABLE_CANDIDATE,
        id_field=settings.CANDIDATE_PROFILE_ID_FIELD,
        id_value=settings.CANDIDATE_PROFILE_ID_VALUE,
    )
    if not candidate_record:
        raise RuntimeError(
            f"CandidateProfile not found: {settings.CANDIDATE_PROFILE_ID_FIELD}={settings.CANDIDATE_PROFILE_ID_VALUE}"
        )

    candidate_id = candidate_record.get("id")
    fields = candidate_record.get("fields", {}) or {}

    # 2) Extract candidate truth fields
    print("PROFILE_BUILDER: extracting candidate truth fields...")
    truth_fields = [
        "ProfessionalSummary",
        "ResumeMasterText",
        "Skills",
        "Certifications",
        "TargetRoles",
        "TargetLocations",
        "Achievements",
        "PreferencesConstraints",
        "PortfolioLinks",
        "OutreachTone",
    ]

    truth_blob = f"ProfileID: {settings.CANDIDATE_PROFILE_ID_VALUE}\n\n"
    for field_name in truth_fields:
        value = fields.get(field_name)
        if value:
            truth_blob += f"## {field_name}\n{value}\n\n"

    print(f"PROFILE_BUILDER: truth blob ({len(truth_blob)} chars)")

    # 3) OpenAI Call #1 — Generate CandidateJSON
    print("PROFILE: calling JSON builder...")
    candidate_json_prompt = prompts.CANDIDATE_JSON_PROMPT.replace(
        "<<CANDIDATE_TRUTH>>", truth_blob
    )
    candidate_json_text = openai_client.request_json(
        model=settings.OPENAI_MODEL_PARSE,
        prompt=candidate_json_prompt,
        max_tokens=1500,
    )
    print("PROFILE: JSON builder done...")
    print(f"PROFILE_BUILDER: CandidateJSON returned ({len(candidate_json_text)} chars)")

    # Validate JSON
    try:
        candidate_json_obj = json.loads(candidate_json_text)
        candidate_json_obj["profile_id"] = settings.CANDIDATE_PROFILE_ID_VALUE
        candidate_json_str = json.dumps(candidate_json_obj, ensure_ascii=False)
    except json.JSONDecodeError as e:
        raise RuntimeError(f"CandidateJSON parse failed: {str(e)}\n{candidate_json_text[:200]}")

    # 4) OpenAI Call #2 — Generate CandidateProfilePackAI
    print("PROFILE: calling ProfilePack builder...")
    profile_pack_prompt = prompts.CANDIDATE_PROFILE_PACK_PROMPT.replace(
        "<<CANDIDATE_TRUTH>>", truth_blob
    ).replace("<<CANDIDATE_JSON>>", candidate_json_str)
    profile_pack_text = openai_client.request_json(
        model=settings.OPENAI_MODEL_PARSE,
        prompt=profile_pack_prompt,
        max_tokens=1200,
    )
    print("PROFILE: ProfilePack builder done...")
    print(f"PROFILE_BUILDER: ProfilePackAI returned ({len(profile_pack_text)} chars)")

    # 5) Write back to Airtable
    print("PROFILE_BUILDER: writing results to Airtable...")
    update_fields = {
        "CandidateJSON": candidate_json_str,
        "CandidateProfilePackAI": profile_pack_text,
    }
    airtable_client.update_record(
        settings.AIRTABLE_TABLE_CANDIDATE,
        candidate_id,
        update_fields,
    )
    print("PROFILE_BUILDER: Airtable updated")

    # 6) Return summary
    print("PROFILE_BUILDER: done")
    return {
        "profile_id": settings.CANDIDATE_PROFILE_ID_VALUE,
        "candidate_json_chars": len(candidate_json_str),
        "profile_pack_chars": len(profile_pack_text),
    }
