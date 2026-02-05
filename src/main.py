import argparse
import sys

import config
from airtable_client import AirtableClient
from openai_client import OpenAIClient
import pipeline
import profile_builder


def run_command(args) -> None:
    try:
        with open(args.jd_file, "r", encoding="utf-8") as f:
            job_description_raw = f.read()

        settings = config.settings
        settings.validate()

        airtable = AirtableClient(settings.AIRTABLE_TOKEN, settings.AIRTABLE_BASE_ID)
        openai = OpenAIClient(settings.OPENAI_API_KEY)

        result = pipeline.run_pipeline(
            job_url=args.job_url,
            job_description_raw=job_description_raw,
            settings=settings,
            airtable_client=airtable,
            openai_client=openai,
        )

        print("✅ Pipeline completed.")
        print("Job Record:", result.get("job_record", {}).get("id"))
        score = result.get("score", {})
        print("Strategy:", score.get("strategy"))
        print("FitScore:", score.get("fit_score"))
        print("RUN_STATUS=PASS")
        print("JOB_RECORD_ID=", result.get("job_record", {}).get("id"))
        print("FIT_SCORE=", score.get("fit_score"))
        print("STRATEGY=", score.get("strategy"))
    except Exception as e:
        print("RUN_STATUS=FAIL")
        raise


def profile_build_command(args) -> None:
    settings = config.settings
    settings.validate()

    airtable = AirtableClient(settings.AIRTABLE_TOKEN, settings.AIRTABLE_BASE_ID)
    openai = OpenAIClient(settings.OPENAI_API_KEY)

    result = profile_builder.build_profile(
        settings=settings,
        airtable_client=airtable,
        openai_client=openai,
    )

    print("✅ Profile build completed.")
    print("Profile ID:", result.get("profile_id"))
    print("CandidateJSON chars:", result.get("candidate_json_chars"))
    print("CandidateProfilePackAI chars:", result.get("profile_pack_chars"))


def main() -> None:
    parser = argparse.ArgumentParser(prog="job-agent-mvp")
    sub = parser.add_subparsers(dest="cmd")

    # Startup diagnostics (do not print secret values)
    settings = config.settings
    print("AIRTABLE_BASE_ID loaded?:", bool(settings.AIRTABLE_BASE_ID))
    print("AIRTABLE_TOKEN loaded?:", bool(settings.AIRTABLE_TOKEN))
    print("RSS_FEED_URL loaded?:", bool(getattr(settings, "RSS_FEED_URL", None)))
    print("RSS_SOURCE_FEED_NAME:", getattr(settings, "RSS_SOURCE_FEED_NAME", None))

    run = sub.add_parser("run", help="Run the job processing pipeline")
    run.add_argument("--job-url", required=True, help="Job URL")
    run.add_argument("--jd-file", required=True, help="Path to job description file")

    profile_build = sub.add_parser("profile-build", help="Build and enrich CandidateProfile")

    args = parser.parse_args()
    if args.cmd == "run":
        run_command(args)
    elif args.cmd == "profile-build":
        profile_build_command(args)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
