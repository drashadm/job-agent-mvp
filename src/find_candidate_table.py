#!/usr/bin/env python
"""Debug: Try to find the correct CandidateProfile table name"""

import config
from airtable_client import AirtableClient

settings = config.settings
airtable = AirtableClient(settings.AIRTABLE_TOKEN, settings.AIRTABLE_BASE_ID)

# List of possible table names to try
possible_names = [
    "CandidateProfile",
    "Candidate",
    "Candidates",
    "Profile",
    "Me",
    "User",
    "Users",
]

print("Attempting to find the correct CandidateProfile table...")
for table_name in possible_names:
    try:
        records = airtable.list_records(table_name, max_records=1)
        print(f"✅ Found table: {table_name}")
        if records:
            print(f"   First record has fields: {list(records[0].get('fields', {}).keys())[:5]}")
    except Exception as e:
        error_msg = str(e)
        if "403" in error_msg or "not found" in error_msg.lower():
            pass  # Expected for non-existent tables
        else:
            print(f"⚠️  {table_name}: {error_msg[:80]}")
