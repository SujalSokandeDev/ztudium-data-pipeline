"""
ai_client.py — Unified AI client with automatic Gemini fallback.

Provides OpenAI as the primary provider and Gemini 3.1 Pro as a
fallback. Once OpenAI fails (quota, rate-limit, or API error), the
module switches to Gemini for ALL remaining calls in the current run.

Usage:
    from ai_client import get_ai_client, ai_chat_completion, ai_json_response

    # Get the currently-active client (for guard checks like `if not client`)
    client = get_ai_client()

    # High-level helpers (handle fallback internally):
    result_dict = ai_json_response(system_prompt, payload, model="gpt-4o", temperature=0.1)
    response    = ai_chat_completion(model="gpt-4o", temperature=0.3, max_tokens=3000, messages=[...])
"""

import json
import logging
import os
import time
from datetime import datetime

from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception

logger = logging.getLogger("ai_client")

# ── Constants ───────────────────────────────────────────────
GEMINI_BASE_URL = "https://generativelanguage.googleapis.com/v1beta/openai/"
GEMINI_MODEL = "gemini-3.1-pro-preview"
GEMINI_FALLBACK_MODEL = os.getenv("GEMINI_FALLBACK_MODEL", "gemini-2.5-pro")
OPENAI_SAFETY_MODEL = os.getenv("OPENAI_SAFETY_MODEL", "gpt-4o")

# Map OpenAI model names → Gemini equivalents
_MODEL_MAP = {
    "gpt-4o": GEMINI_MODEL,
    "gpt-4o-mini": GEMINI_MODEL,
    "gpt-4-turbo": GEMINI_MODEL,
    "gpt-3.5-turbo": GEMINI_MODEL,
}

# ── Module-level state ──────────────────────────────────────
_openai_client = None
_gemini_client = None
_use_gemini = False  # Flipped to True on first OpenAI failure
_clients_initialised = False

try:
    from openai import OpenAI
except ImportError:
    OpenAI = None  # type: ignore[assignment,misc]


def _init_clients():
    """Lazily initialise OpenAI and Gemini clients.

    Reads environment variables at call time (not import time) so that
    dotenv / GitHub Actions env injection is always picked up.
    """
    global _openai_client, _gemini_client, _clients_initialised

    if _clients_initialised:
        return
    _clients_initialised = True

    if OpenAI is None:
        logger.warning("openai package not installed — AI features disabled")
        return

    openai_key = os.getenv("OPENAI_API_KEY", "")
    gemini_key = os.getenv("GEMINI_API_KEY", "")

    if openai_key and _openai_client is None:
        _openai_client = OpenAI(api_key=openai_key, timeout=90.0)
        logger.debug("OpenAI client initialised")

    if gemini_key and _gemini_client is None:
        _gemini_client = OpenAI(
            api_key=gemini_key,
            base_url=GEMINI_BASE_URL,
            timeout=90.0,
        )
        logger.debug("Gemini client initialised (fallback)")


def get_ai_client():
    """Return the currently-active AI client, or None if unavailable.

    This is useful for guard-checks like ``if not get_ai_client(): skip``.
    """
    _init_clients()
    if _use_gemini:
        return _gemini_client
    return _openai_client or _gemini_client


def _map_model(openai_model: str) -> str:
    """Translate an OpenAI model name to its Gemini equivalent."""
    return _MODEL_MAP.get(openai_model, GEMINI_MODEL)


def _is_provider_fatal(exc: Exception) -> bool:
    """Detect quota / rate-limit / auth errors that should trigger fallback."""
    msg = str(exc).lower()
    fatal_markers = (
        "429",
        "rate limit",
        "quota",
        "exceeded",
        "insufficient_quota",
        "billing",
        "401",
        "invalid api key",
        "incorrect api key",
        "account deactivated",
    )
    return any(marker in msg for marker in fatal_markers)


def _is_retryable(exc: Exception) -> bool:
    """Detect transient errors worth retrying on the same provider."""
    msg = str(exc).lower()
    retry_markers = (
        "connection reset",
        "temporarily unavailable",
        "503",
        "502",
        "504",
        "overloaded",
    )
    return any(marker in msg for marker in retry_markers)


def _attach_model_used(response, provider: str, model: str):
    try:
        setattr(response, "_ztudium_provider_used", provider)
        setattr(response, "_ztudium_model_used", model)
    except Exception:
        pass
    return response


def response_model_used(response) -> str:
    return str(getattr(response, "_ztudium_model_used", "") or getattr(response, "model", "") or "unknown")


def response_provider_used(response) -> str:
    return str(getattr(response, "_ztudium_provider_used", "") or "unknown")


def _sleep_for_retry(delay: int, provider: str, model: str, attempt: int, exc: Exception):
    logger.warning(
        "AI call failed at %s on %s/%s attempt %s: %s",
        datetime.now().isoformat(timespec="seconds"),
        provider,
        model,
        attempt,
        str(exc)[:220],
    )
    if delay > 0:
        time.sleep(delay)


def _switch_to_gemini(reason: str):
    """Flip the global flag so all subsequent calls use Gemini."""
    global _use_gemini
    if _use_gemini:
        return  # Already switched
    _init_clients()
    if _gemini_client:
        _use_gemini = True
        logger.warning(
            "⚡ Switching to Gemini fallback for remaining AI calls. Reason: %s",
            reason,
        )
    else:
        logger.error(
            "OpenAI failed (%s) and GEMINI_API_KEY is not configured — cannot fallback",
            reason,
        )


# ── Public helpers ──────────────────────────────────────────

def ai_chat_completion(*, model: str = "gpt-4o", fallback_model: str | None = None, **kwargs):
    """Drop-in replacement for ``client.chat.completions.create(...)``.

    Tries OpenAI first. On provider-fatal errors, switches to Gemini and
    retries. Transient errors are retried on the same provider (up to 3x).
    An explicit `fallback_model` can be provided (e.g., "gemini-2.5-pro")
    to override the default mapping.

    Returns the full ChatCompletion response object.
    """
    _init_clients()

    @retry(
        reraise=True,
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception(_is_retryable),
    )
    def _call(client, target_model: str):
        return client.chat.completions.create(model=target_model, **kwargs)

    def resolve_target_model(is_fallback: bool) -> str:
        if is_fallback:
            return fallback_model or _map_model(model)
        return model

    # Attempt 1: current provider
    client = _gemini_client if _use_gemini else _openai_client
    target_model = resolve_target_model(_use_gemini or client is _gemini_client)

    if client is None:
        # No primary client — try the other
        if _gemini_client:
            _switch_to_gemini("no OpenAI client available")
            client = _gemini_client
            target_model = resolve_target_model(True)
        elif _openai_client:
            client = _openai_client
            target_model = resolve_target_model(False)
        else:
            raise RuntimeError(
                "No AI provider configured. Set OPENAI_API_KEY or GEMINI_API_KEY."
            )

    try:
        return _call(client, target_model)
    except Exception as exc:
        if _is_provider_fatal(exc) and not _use_gemini and _gemini_client:
            _switch_to_gemini(str(exc)[:200])
            return _call(_gemini_client, resolve_target_model(True))
        raise


def ai_chat_completion_reliable(
    *,
    model: str = "gpt-4o",
    primary_gemini_model: str | None = None,
    secondary_gemini_model: str | None = None,
    openai_fallback_model: str | None = None,
    **kwargs,
):
    """Gemini-first completion chain for insight generation.

    Order:
    1. Gemini 3.1 Pro Preview, initial attempt + one retry.
    2. Gemini 2.5 Pro, with exponential backoff.
    3. OpenAI safety net, preserving the same messages and options.
    """
    _init_clients()
    primary = primary_gemini_model or GEMINI_MODEL
    secondary = secondary_gemini_model or GEMINI_FALLBACK_MODEL
    openai_model = openai_fallback_model or OPENAI_SAFETY_MODEL
    errors = []

    def _call(client, provider: str, target_model: str):
        response = client.chat.completions.create(model=target_model, **kwargs)
        return _attach_model_used(response, provider, target_model)

    if _gemini_client:
        for attempt, delay in enumerate((0, 1), start=1):
            try:
                if delay:
                    time.sleep(delay)
                return _call(_gemini_client, "gemini", primary)
            except Exception as exc:
                errors.append(exc)
                _sleep_for_retry(0, "gemini", primary, attempt, exc)

        for attempt, delay in enumerate((0, 1, 2, 4), start=1):
            try:
                if delay:
                    time.sleep(delay)
                return _call(_gemini_client, "gemini", secondary)
            except Exception as exc:
                errors.append(exc)
                if attempt < 4:
                    _sleep_for_retry(0, "gemini", secondary, attempt, exc)
                else:
                    logger.error(
                        "Gemini fallback exhausted at %s on %s: %s",
                        datetime.now().isoformat(timespec="seconds"),
                        secondary,
                        str(exc)[:220],
                    )

    if _openai_client:
        for attempt, delay in enumerate((0, 1), start=1):
            try:
                if delay:
                    time.sleep(delay)
                return _call(_openai_client, "openai", openai_model)
            except Exception as exc:
                errors.append(exc)
                _sleep_for_retry(0, "openai", openai_model, attempt, exc)

    detail = "; ".join(str(err)[:180] for err in errors[-3:]) or "No configured AI provider."
    raise RuntimeError(f"All AI model attempts failed: {detail}")


def ai_json_response(
    system_prompt: str,
    payload: dict,
    *,
    model: str = "gpt-4o",
    fallback_model: str | None = None,
    temperature: float = 0.1,
    max_tokens: int | None = None,
) -> dict:
    """Send a chat completion expecting a JSON response, with fallback.

    Returns the parsed JSON dict.
    """
    extra = {}
    if max_tokens is not None:
        extra["max_tokens"] = max_tokens

    response = ai_chat_completion(
        model=model,
        fallback_model=fallback_model,
        temperature=temperature,
        response_format={"type": "json_object"},
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": json.dumps(payload, ensure_ascii=False)},
        ],
        **extra,
    )
    content = response.choices[0].message.content or "{}"
    return json.loads(content)
