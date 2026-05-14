import json
import logging
import re
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeoutError
from typing import Any

from app.catalog.canonical_indicator_catalog import (
    detect_unsupported_indicator,
    is_supported_indicator,
    resolve_indicator_alias,
)
from app.catalog.country_group_catalog import get_country_group
from app.core.config import settings
from app.llm.gemini_client import GeminiClientError, generate_gemini_text, is_gemini_enabled
from app.resolver.country_resolver import COUNTRIES
from app.router.front_router_prompt import ALLOWED_FRONT_INTENTS, ALLOWED_FRONT_ROUTES, build_front_router_prompt


logger = logging.getLogger(__name__)


def route_with_front_llm_draft(
    user_message: str,
    conversation_context: dict[str, Any],
    rule_route_draft: Any | None = None,
) -> dict[str, Any] | None:
    if not settings.enable_front_llm_router:
        return None

    if not is_gemini_enabled():
        return None

    prompt = build_front_router_prompt(
        user_message=user_message,
        conversation_context=conversation_context,
        rule_route_draft=rule_route_draft,
    )

    try:
        text = _generate_with_timeout(prompt)
        parsed = _parse_json_object(text)
        return _sanitize_front_router_result(parsed)
    except (GeminiClientError, json.JSONDecodeError, ValueError, TimeoutError) as error:
        logger.warning("Front LLM router failed: %s", error)
        return None
    except Exception:
        logger.exception("Unexpected Front LLM router error")
        return None


def _generate_with_timeout(prompt: str) -> str:
    timeout_seconds = max(settings.gemini_router_timeout_ms, 1) / 1000

    executor = ThreadPoolExecutor(max_workers=1)
    future = executor.submit(
        generate_gemini_text,
        prompt,
        settings.gemini_router_model,
    )

    try:
        return future.result(timeout=timeout_seconds)
    except FutureTimeoutError as error:
        raise TimeoutError("Front LLM router timeout") from error
    finally:
        executor.shutdown(wait=False, cancel_futures=True)


def _parse_json_object(text: str) -> dict[str, Any]:
    cleaned = _strip_code_fence(text)
    data = json.loads(cleaned)
    if not isinstance(data, dict):
        raise ValueError("Front router response must be a JSON object")
    return data


def _sanitize_front_router_result(data: dict[str, Any]) -> dict[str, Any]:
    route = str(data.get("route") or "").upper().strip()
    if route not in ALLOWED_FRONT_ROUTES:
        route = "DATA_QUERY"

    intent_hint = _optional_upper(data.get("intent_hint"))
    if intent_hint and intent_hint not in ALLOWED_FRONT_INTENTS:
        intent_hint = None

    draft_indicators, unsupported_terms_from_indicators = _normalize_indicators(data.get("draft_indicators"))
    unsupported_terms = _dedupe([
        *_string_list(data.get("unsupported_terms")),
        *unsupported_terms_from_indicators,
    ])

    draft_countries = _normalize_countries(data.get("draft_countries"))
    draft_country_groups = _normalize_country_groups(data.get("draft_country_groups"))

    clarification_questions = _dedupe(_string_list(data.get("clarification_questions")))

    return {
        "route": route,
        "intent_hint": intent_hint,
        "rewritten_query": _optional_str(data.get("rewritten_query")),
        "draft_indicators": draft_indicators,
        "draft_countries": draft_countries,
        "draft_country_groups": draft_country_groups,
        "draft_start_year": _int_or_none(data.get("draft_start_year")),
        "draft_end_year": _int_or_none(data.get("draft_end_year")),
        "draft_limit": _clamp_int(data.get("draft_limit"), 1, 50),
        "draft_ranking_order": _ranking_order(data.get("draft_ranking_order")),
        "unsupported_terms": unsupported_terms,
        "clarification_questions": clarification_questions,
        "uses_previous_context": bool(data.get("uses_previous_context")),
        "confidence": _clamp_float(data.get("confidence"), 0.0, 1.0),
        "reason": _optional_str(data.get("reason")) or "",
    }


def _normalize_indicators(value: Any) -> tuple[list[str], list[str]]:
    indicators: list[str] = []
    unsupported_terms: list[str] = []

    for raw in _string_list(value):
        text = str(raw).strip()
        if not text:
            continue

        if is_supported_indicator(text):
            indicators.append(text)
            continue

        alias_match = resolve_indicator_alias(text)
        if alias_match and is_supported_indicator(alias_match.indicator.code):
            indicators.append(alias_match.indicator.code)
            continue

        unsupported = detect_unsupported_indicator(text)
        if unsupported:
            unsupported_terms.append(unsupported.label_vi)

    return _dedupe(indicators), _dedupe(unsupported_terms)


def _normalize_countries(value: Any) -> list[str]:
    result: list[str] = []
    for raw in _string_list(value):
        code = str(raw).upper().strip()
        if code in COUNTRIES and code not in result:
            result.append(code)
    return result


def _normalize_country_groups(value: Any) -> list[str]:
    result: list[str] = []
    for raw in _string_list(value):
        code = str(raw).upper().strip()
        if get_country_group(code) and code not in result:
            result.append(code)
    return result


def _strip_code_fence(text: str) -> str:
    cleaned = str(text or "").strip()
    match = re.match(r"^```(?:json)?\s*(.*?)\s*```$", cleaned, flags=re.DOTALL | re.IGNORECASE)
    if match:
        return match.group(1).strip()
    return cleaned


def _string_list(value: Any) -> list[str]:
    if not value:
        return []
    if isinstance(value, (list, tuple, set)):
        return [str(item) for item in value if str(item or "").strip()]
    return [str(value)]


def _optional_str(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _optional_upper(value: Any) -> str | None:
    text = _optional_str(value)
    return text.upper() if text else None


def _int_or_none(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _clamp_int(value: Any, min_value: int, max_value: int) -> int | None:
    number = _int_or_none(value)
    if number is None:
        return None
    return max(min_value, min(max_value, number))


def _ranking_order(value: Any) -> str | None:
    text = str(value or "").lower().strip()
    if text in {"asc", "desc"}:
        return text
    return None


def _clamp_float(value: Any, min_value: float, max_value: float) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        number = min_value
    return max(min_value, min(max_value, number))


def _dedupe(values: list[str]) -> list[str]:
    result: list[str] = []
    for value in values:
        text = str(value or "").strip()
        if text and text not in result:
            result.append(text)
    return result
