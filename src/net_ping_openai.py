from config import settings
import requests

def main():
    settings.validate()
    print("Calling OpenAI REST /v1/models ...")
    try:
        r = requests.get(
            "https://api.openai.com/v1/models",
            headers={"Authorization": f"Bearer {settings.OPENAI_API_KEY}"},
            timeout=(10, 20),
        )
        print("Status:", r.status_code)
        print("Body (first 200 chars):", r.text[:200])
    except Exception as e:
        print("ERROR:", type(e).__name__, str(e))

if __name__ == "__main__":
    main()
