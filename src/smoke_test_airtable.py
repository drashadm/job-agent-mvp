#!/usr/bin/env python
"""
Smoke test: Airtable connectivity and basic CRUD.
Does NOT require OpenAI.
"""

import config
from airtable_client import AirtableClient


def test_airtable_connectivity():
    settings = config.settings
    airtable = AirtableClient(settings.AIRTABLE_TOKEN, settings.AIRTABLE_BASE_ID)

    print("📝 Testing Airtable connectivity...")

    # Test 1: Create a test job record
    print("  1. Creating a test job record...")
    test_fields = {
        "JobURL": "https://smoke-test.example.com",
        "JobDescriptionRaw": "This is a smoke test job description.",
        "Source": "LinkedIn",
    }
    try:
        created = airtable.create_record(settings.AIRTABLE_TABLE_JOBS, test_fields)
        job_id = created.get("id")
        print(f"     ✅ Record created: {job_id}")
    except Exception as e:
        print(f"     ❌ Failed to create record: {e}")
        return False

    # Test 2: Verify the record was created
    print("  2. Verifying record was created...")
    try:
        fetched = airtable.get_record(settings.AIRTABLE_TABLE_JOBS, job_id)
        if fetched.get("id") == job_id:
            print(f"     ✅ Record verified: {fetched.get('id')}")
        else:
            print(f"     ❌ Record not found")
            return False
    except Exception as e:
        print(f"     ❌ Failed to fetch record: {e}")
        return False

    # Test 3: Update the record
    print("  3. Updating the record...")
    update_fields = {"JobURL": "https://smoke-test-updated.example.com"}
    try:
        updated = airtable.update_record(settings.AIRTABLE_TABLE_JOBS, job_id, update_fields)
        print(f"     ✅ Record updated: {updated.get('id')}")
    except Exception as e:
        print(f"     ❌ Failed to update record: {e}")
        return False

    print("✅ Airtable connectivity test PASSED")
    return True


if __name__ == "__main__":
    config.settings.validate()
    success = test_airtable_connectivity()
    exit(0 if success else 1)
