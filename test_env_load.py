#!/usr/bin/env python
"""Test .env loading"""

from dotenv import load_dotenv
import os

# Explicitly load .env
result = load_dotenv('.env', override=True)
print(f'load_dotenv result: {result}')

# Check if variables loaded
vars_to_check = [
    'OPENAI_API_KEY',
    'AIRTABLE_TOKEN', 
    'AIRTABLE_BASE_ID',
    'AIRTABLE_TABLE_JOBS',
    'AIRTABLE_TABLE_EVENTS',
    'AIRTABLE_TABLE_CANDIDATE',
    'CANDIDATE_PROFILE_ID_FIELD',
    'CANDIDATE_PROFILE_ID_VALUE'
]

print('\nEnvironment Variables Status:')
for var in vars_to_check:
    loaded = bool(os.getenv(var))
    status = '✅' if loaded else '❌'
    print(f'{status} {var}')

print('\nAll required vars present?', all(os.getenv(v) for v in vars_to_check))
