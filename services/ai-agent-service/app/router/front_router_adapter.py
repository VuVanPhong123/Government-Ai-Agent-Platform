from typing import Any

from app.pipeline.schemas import FrontRouterDraft, RuleRouteDraft


def build_front_router_draft_from_existing_router(
    router_result: dict[str, Any] | None,
    rule_draft: RuleRouteDraft | None = None,
) -> FrontRouterDraft:
    data = router_result if isinstance(router_result, dict) else {}

    if data:
        return FrontRouterDraft(
            route=_optional_str(data.get("route")),
            intent_hint=_optional_str(data.get("intent") or data.get("intent_hint")),
            rewritten_query=_optional_str(data.get("rewritten_query")),
            draft_indicators=_string_list(data.get("draft_indicators") or data.get("indicators")),
            draft_countries=_string_list(data.get("draft_countries") or data.get("countries")),
            draft_country_groups=_string_list(data.get("draft_country_groups") or data.get("country_groups")),
            draft_start_year=_int_or_none(data.get("draft_start_year") or data.get("start_year")),
            draft_end_year=_int_or_none(data.get("draft_end_year") or data.get("end_year")),
            draft_limit=_int_or_none(data.get("draft_limit") or data.get("limit")),
            draft_ranking_order=_optional_str(data.get("draft_ranking_order") or data.get("ranking_order")),
            unsupported_terms=_string_list(data.get("unsupported_terms")),
            clarification_questions=_string_list(data.get("clarification_questions")),
            uses_previous_context=bool(data.get("uses_previous_context") or data.get("uses_previous_result")),
            confidence=_float_or_zero(data.get("confidence")),
            reason=_optional_str(data.get("reason")) or "",
        )

    if rule_draft is None:
        return FrontRouterDraft(reason="No existing router result.")

    return FrontRouterDraft(
        route=rule_draft.route,
        intent_hint=rule_draft.intent_hint,
        draft_indicators=list(rule_draft.draft_indicators),
        draft_countries=list(rule_draft.draft_countries),
        draft_country_groups=list(rule_draft.draft_country_groups),
        draft_start_year=rule_draft.draft_start_year,
        draft_end_year=rule_draft.draft_end_year,
        draft_limit=rule_draft.draft_limit,
        draft_ranking_order=rule_draft.draft_ranking_order,
        unsupported_terms=list(rule_draft.unsupported_terms),
        clarification_questions=list(rule_draft.clarification_questions),
        uses_previous_context=rule_draft.uses_previous_context,
        confidence=rule_draft.confidence,
        reason=rule_draft.reason,
    )


def _string_list(value: Any) -> list[str]:
    if not value:
        return []
    if isinstance(value, list):
        return [str(item) for item in value if item]
    if isinstance(value, tuple):
        return [str(item) for item in value if item]
    return [str(value)]


def _optional_str(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _int_or_none(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _float_or_zero(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0
