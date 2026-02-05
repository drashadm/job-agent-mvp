#!/usr/bin/env python
"""
Smoke test: CandidateProfile integrity check.
Verify the configured candidate record exists and is readable.
"""

import config
from airtable_client import AirtableClient


def test_candidate_profile():
    settings = config.settings
    airtable = AirtableClient(settings.AIRTABLE_TOKEN, settings.AIRTABLE_BASE_ID)

    print("👤 Testing CandidateProfile integrity...")

    # Query for the candidate
    print(f"  1. Looking for ProfileID = '{settings.CANDIDATE_PROFILE_ID_VALUE}'...")
    try:
        candidate = airtable.find_one(
            table=settings.AIRTABLE_TABLE_CANDIDATE,
            id_field=settings.CANDIDATE_PROFILE_ID_FIELD,
            id_value=settings.CANDIDATE_PROFILE_ID_VALUE,
        )
        if not candidate:
            print(f"     ❌ CandidateProfile not found")
            return False
        candidate_id = candidate.get("id")
        print(f"     ✅ CandidateProfile found: {candidate_id}")
    except Exception as e:
        print(f"     ❌ Query failed: {e}")
        return False

    # Verify fields are readable
    print("  2. Checking field structure...")
    try:
        fields = candidate.get("fields", {})
        if not isinstance(fields, dict):
            print(f"     ❌ Fields are not a dict: {type(fields)}")
            return False
        print(f"     ✅ Fields readable as flat dict ({len(fields)} fields)")
        
        # Show non-sensitive field names
        field_names = list(fields.keys())
        print(f"     Fields: {', '.join(field_names[:5])}{'...' if len(field_names) > 5 else ''}")
    except Exception as e:
        print(f"     ❌ Failed to read fields: {e}")
        return False

    print("✅ CandidateProfile integrity test PASSED")
    return True


if __name__ == "__main__":
    config.settings.validate()
    success = test_candidate_profile()
    exit(0 if success else 1)
