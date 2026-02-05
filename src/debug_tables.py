#!/usr/bin/env python
"""Debug: List all tables and test read access"""

import config
from airtable_client import AirtableClient

settings = config.settings
airtable = AirtableClient(settings.AIRTABLE_TOKEN, settings.AIRTABLE_BASE_ID)

# Try to list some records from the candidate table
print(f"Attempting to read from table: {settings.AIRTABLE_TABLE_CANDIDATE}")
try:
    records = airtable.list_records(settings.AIRTABLE_TABLE_CANDIDATE, max_records=5)
    print(f"✅ Retrieved {len(records)} records")
    if records:
        print(f"First record ID: {records[0].get('id')}")
        fields = records[0].get('fields', {})
        print(f"Field names: {list(fields.keys())[:10]}")
except Exception as e:
    print(f"❌ Error: {e}")
