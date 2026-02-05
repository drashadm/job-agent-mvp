#!/usr/bin/env python
"""
Probe Airtable to verify access to Jobs, Events, and CandidateProfile tables.
"""

import sys
import config
from airtable_client import AirtableClient


def probe_airtable():
    """Check access to required Airtable tables."""
    print("Starting probe...")
    
    print("Validating settings...")
    settings = config.settings
    settings.validate()
    
    print("Creating AirtableClient...")
    airtable = AirtableClient(settings.AIRTABLE_TOKEN, settings.AIRTABLE_BASE_ID)
    
    tables_to_check = ["Jobs", "Events", "CandidateProfile"]
    all_ok = True
    
    for table_name in tables_to_check:
        print(f"Probing table: {table_name}...")
        try:
            # Try to fetch one record from each table to verify access
            records = airtable.list_records(table_name)
            print(f"✅ OK: {table_name}")
        except Exception as e:
            print(f"❌ FAIL: {table_name}")
            print(f"   Error: {str(e)}")
            all_ok = False
    
    return all_ok


if __name__ == "__main__":
    success = probe_airtable()
    sys.exit(0 if success else 1)
