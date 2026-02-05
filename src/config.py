import os
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv

import os
from pathlib import Path
from dotenv import load_dotenv

print("CWD:", os.getcwd())
print(".env exists:", Path(".env").exists())
load_dotenv()
print("AIRTABLE_BASE_ID loaded?:", bool(os.getenv("AIRTABLE_BASE_ID")))
print("AIRTABLE_TOKEN loaded?:", bool(os.getenv("AIRTABLE_TOKEN")))


# Load .env if present (safe no-op if missing)
load_dotenv()


class Settings:
    def __init__(
        self,
        openai_api_key: Optional[str] = None,
        openai_model_parse: Optional[str] = None,
        openai_model_score: Optional[str] = None,
        airtable_token: Optional[str] = None,
        airtable_base_id: Optional[str] = None,
        airtable_table_jobs: Optional[str] = None,
        airtable_table_events: Optional[str] = None,
        airtable_table_candidate: Optional[str] = None,
        candidate_profile_id_field: Optional[str] = None,
        candidate_profile_id_value: Optional[str] = None,
    ):
        # OpenAI
        self.OPENAI_API_KEY = openai_api_key or os.getenv("OPENAI_API_KEY")
        # Default to a cost-effective model; override via env if desired
        self.OPENAI_MODEL_PARSE = openai_model_parse or os.getenv("OPENAI_MODEL_PARSE", "gpt-4o-mini")
        self.OPENAI_MODEL_SCORE = openai_model_score or os.getenv("OPENAI_MODEL_SCORE", "gpt-4o-mini")

        # Airtable
        self.AIRTABLE_TOKEN = airtable_token or os.getenv("AIRTABLE_TOKEN")
        self.AIRTABLE_BASE_ID = airtable_base_id or os.getenv("AIRTABLE_BASE_ID")

        # Table names (safe defaults)
        self.AIRTABLE_TABLE_JOBS = airtable_table_jobs or os.getenv("AIRTABLE_TABLE_JOBS", "Jobs")
        self.AIRTABLE_TABLE_EVENTS = airtable_table_events or os.getenv("AIRTABLE_TABLE_EVENTS", "Events")
        self.AIRTABLE_TABLE_CANDIDATE = airtable_table_candidate or os.getenv("AIRTABLE_TABLE_CANDIDATE", "CandidateProfile")

        # CandidateProfile lookup
        self.CANDIDATE_PROFILE_ID_FIELD = candidate_profile_id_field or os.getenv("CANDIDATE_PROFILE_ID_FIELD", "ProfileID")
        self.CANDIDATE_PROFILE_ID_VALUE = candidate_profile_id_value or os.getenv("CANDIDATE_PROFILE_ID_VALUE", "ME")

        # RSS feed settings
        self.RSS_FEED_URL = os.getenv("RSS_FEED_URL")
        self.RSS_SOURCE_FEED_NAME = os.getenv("RSS_SOURCE_FEED_NAME", "LinkedIn_AI_Integration_DMV")

    def validate(self) -> None:
        missing = [
            name
            for name in (
                "OPENAI_API_KEY",
                "AIRTABLE_TOKEN",
                "AIRTABLE_BASE_ID",
                "AIRTABLE_TABLE_JOBS",
                "AIRTABLE_TABLE_EVENTS",
                "AIRTABLE_TABLE_CANDIDATE",
                "CANDIDATE_PROFILE_ID_FIELD",
                "CANDIDATE_PROFILE_ID_VALUE",
                    "RSS_FEED_URL",
            )
            if not getattr(self, name)
        ]
        if missing:
            raise RuntimeError(f"Missing required configuration: {', '.join(missing)}")


# Expose a Settings instance (loaded from environment). Validation is explicit.
settings = Settings()
