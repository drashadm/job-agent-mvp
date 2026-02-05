#!/usr/bin/env python
"""
Verify that CandidateJSON in Airtable is valid and properly populated.
"""

import sys
import json

sys.path.insert(0, '.')
import config
from airtable_client import AirtableClient


def verify_candidate_json():
    """Verify CandidateJSON is valid."""
    settings = config.settings
    settings.validate()

    airtable = AirtableClient(settings.AIRTABLE_TOKEN, settings.AIRTABLE_BASE_ID)

    # Fetch the ME profile
    me_record = airtable.find_one(
        table=settings.AIRTABLE_TABLE_CANDIDATE,
        id_field=settings.CANDIDATE_PROFILE_ID_FIELD,
        id_value=settings.CANDIDATE_PROFILE_ID_VALUE,
    )

    if not me_record:
        print(f"Profile not found: {settings.CANDIDATE_PROFILE_ID_FIELD}={settings.CANDIDATE_PROFILE_ID_VALUE}")
        return False

    fields = me_record.get("fields", {}) or {}
    candidate_json_str = fields.get("CandidateJSON")
    profile_pack_str = fields.get("CandidateProfilePackAI")

    print("CandidateJSON exists:", bool(candidate_json_str))
    print("CandidateProfilePackAI exists:", bool(profile_pack_str))

    candidate_json_valid = False
    if candidate_json_str:
        try:
            parsed = json.loads(candidate_json_str)
            candidate_json_valid = True
            print("CandidateJSON valid JSON: True")
            print("CandidateJSON keys:", list(parsed.keys()))
        except json.JSONDecodeError as e:
            print("CandidateJSON valid JSON: False")
            print("Error:", str(e))
    else:
        print("CandidateJSON valid JSON: False")

    if profile_pack_str:
        print("CandidateProfilePackAI length:", len(profile_pack_str))
    else:
        print("CandidateProfilePackAI length: 0")

    return candidate_json_valid


if __name__ == "__main__":
    success = verify_candidate_json()
    sys.exit(0 if success else 1)
