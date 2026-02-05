from __future__ import annotations

import httpx
from openai import OpenAI, DefaultHttpxClient


class OpenAIClient:
    def __init__(self, api_key: str):
        http_client = DefaultHttpxClient(
            timeout=httpx.Timeout(30.0, connect=10.0, read=30.0, write=30.0, pool=30.0)
        )
        self.client = OpenAI(api_key=api_key, http_client=http_client, max_retries=1)

    def request_json(self, *, model: str, prompt: str, max_tokens: int = 500, temperature: float = 0.0) -> str:
        resp = self.client.responses.create(
            model=model,
            input=prompt,
            max_output_tokens=max_tokens,
            temperature=temperature,
        )

        text = getattr(resp, "output_text", None)
        if isinstance(text, str) and text.strip():
            return text.strip()

        output = getattr(resp, "output", None)
        if isinstance(output, list):
            for block in output:
                content = block.get("content")
                if isinstance(content, list):
                    for item in content:
                        t = item.get("text")
                        if isinstance(t, str) and t.strip():
                            return t.strip()

        return str(resp)
