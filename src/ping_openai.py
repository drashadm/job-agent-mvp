#!/usr/bin/env python
"""
Ping OpenAI to verify connectivity independent of Airtable.
"""

import sys
import config
from openai_client import OpenAIClient


def ping_openai():
    """Test OpenAI API connectivity."""
    print("Validating settings...")
    settings = config.settings
    settings.validate()
    
    print("Instantiating OpenAIClient...")
    openai_client = OpenAIClient(settings.OPENAI_API_KEY)
    
    print("Calling OpenAI...")
    result = openai_client.request_json(
        model=settings.OPENAI_MODEL_PARSE,
        prompt='Return JSON only: {"ok": true}'
    )
    
    print("Response:")
    print(result)
    return True


if __name__ == "__main__":
    try:
        success = ping_openai()
        sys.exit(0 if success else 1)
    except Exception as e:
        print(f"ERROR: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
