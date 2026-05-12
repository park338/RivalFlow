from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass
from typing import Any

import httpx


DEEPSEEK_API_KEY_ENV = "DEEPSEEK_API_KEY"


@dataclass(slots=True)
class LLMTrace:
    model: str
    latency_ms: int
    prompt_tokens: int
    completion_tokens: int
    total_tokens: int
    content_preview: str


class DeepSeekClient:
    def __init__(
        self,
        api_key: str | None = None,
        model: str = "deepseek-v4-flash",
        base_url: str = "https://api.deepseek.com",
        timeout_seconds: int = 40,
    ) -> None:
        resolved_key = api_key or os.getenv(DEEPSEEK_API_KEY_ENV) or ""
        self.api_key = resolved_key.strip()
        self.model = model
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds

    async def complete_json(
        self,
        *,
        system_prompt: str,
        user_prompt: str,
        max_tokens: int = 1400,
        temperature: float = 0.2,
    ) -> tuple[dict[str, Any], LLMTrace]:
        if not self.api_key:
            raise ValueError(f"Missing {DEEPSEEK_API_KEY_ENV} environment variable")

        payload = {
            "model": self.model,
            "temperature": temperature,
            "max_tokens": max_tokens,
            "response_format": {"type": "json_object"},
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
        }
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }
        started_at = time.perf_counter()
        async with httpx.AsyncClient(timeout=self.timeout_seconds) as client:
            response = await client.post(
                f"{self.base_url}/chat/completions",
                headers=headers,
                json=payload,
            )
        latency_ms = int((time.perf_counter() - started_at) * 1000)
        response.raise_for_status()
        raw = response.json()

        choice = (raw.get("choices") or [{}])[0]
        message = choice.get("message") or {}
        content = message.get("content") or "{}"
        parsed = self._safe_json_loads(content)
        usage = raw.get("usage") or {}
        trace = LLMTrace(
            model=raw.get("model") or self.model,
            latency_ms=latency_ms,
            prompt_tokens=int(usage.get("prompt_tokens") or 0),
            completion_tokens=int(usage.get("completion_tokens") or 0),
            total_tokens=int(usage.get("total_tokens") or 0),
            content_preview=str(content)[:300],
        )
        return parsed, trace

    @staticmethod
    def _safe_json_loads(payload: str) -> dict[str, Any]:
        text = payload.strip()
        if text.startswith("```"):
            text = text.strip("`")
            text = text.replace("json", "", 1).strip()
        try:
            data = json.loads(text)
            if isinstance(data, dict):
                return data
        except json.JSONDecodeError:
            return {}
        return {}
