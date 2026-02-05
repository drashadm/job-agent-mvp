#!/usr/bin/env python3
import sys
from dotenv import load_dotenv

load_dotenv()

import config
from rss_client import fetch_rss_items


def main() -> int:
    try:
        settings = config.settings
        settings.validate()

        url = getattr(settings, "RSS_FEED_URL", None)
        if not url:
            raise RuntimeError("RSS_FEED_URL is not configured")

        items = fetch_rss_items(url)
        count = len(items)
        print(f"RSS_OK count={count}")
        for item in items[:2]:
            print(item.get("title", ""))
            print(item.get("link", ""))
        return 0

    except Exception as e:
        print("RSS_FAIL", e)
        return 1


if __name__ == "__main__":
    sys.exit(main())
