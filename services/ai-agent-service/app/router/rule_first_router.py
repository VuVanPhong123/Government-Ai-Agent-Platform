import re
from typing import Any

from app.catalog.canonical_indicator_catalog import (
    detect_unsupported_indicator,
    normalize_catalog_text,
    resolve_indicator_alias,
)
from app.catalog.country_group_catalog import resolve_country_groups
from app.pipeline.schemas import RuleRouteDraft
from app.resolver.country_resolver import resolve_countries


FOLLOW_UP_ANALYSIS_KEYWORDS = (
    "giai thich",
    "tom tat",
    "tóm tắt",
    "dien giai",
    "diễn giải",
    "y nghia",
    "ý nghĩa",
    "ket luan",
    "kết luận",
    "insight",
    "vi sao nuoc",
    "vì sao nước",
    "vi sao",
    "tai sao",
    "nhan xet",
    "phan tich them",
    "ket qua nay",
    "xu huong nay",
)



FOLLOW_UP_MODIFY_KEYWORDS = (
    "them",
    "doi giai doan",
    "đổi giai đoạn",
    "tu nam",
    "từ năm",
    "den nam",
    "đến năm",
    "thay bang",
    "thay bằng",
    "bo ",
    "bỏ ",
    "xoa ",
    "xóa ",
    "chi lay",
    "chỉ lấy",
    "cho them",
    "cho thêm",
    "doi sang nam",
    "top 5",
    "top 10",
    "thap nhat",
    "cao nhat",
    "doi thanh",
)

GENERAL_EXPLANATION_KEYWORDS = (
    "la gi",
    "là gì",
    "nghia la gi",
    "nghĩa là gì",
    "y nghia",
    "ý nghĩa",
    "cach hieu",
    "cách hiểu",
    "dung de",
    "dùng để",
    "phan anh dieu gi",
    "phản ánh điều gì",
    "phan anh gi",
    "phản ánh gì",
    "cho biet dieu gi",
    "cho biết điều gì",
    "dung de danh gia",
    "dùng để đánh giá",
    "noi len dieu gi",
    "nói lên điều gì",
    "the hien dieu gi",
    "thể hiện điều gì",
)

COVERAGE_KEYWORDS = (
    "coverage",
    "co bao nhieu quan sat",
    "có bao nhiêu quan sát",
    "co du lieu khong",
    "có dữ liệu không",
    "thieu du lieu",
    "thiếu dữ liệu",
    "phu du lieu",
    "phủ dữ liệu",
    "pham vi du lieu",
    "du lieu co tu",
    "coverage du lieu",
)

COMPARE_KEYWORDS = (
    "so sanh",
    "compare",
    "vs",
    "voi",
    "giua",
)

RANKING_KEYWORDS = (
    "top",
    "cao nhat",
    "thap nhat",
    "xep hang",
    "ranking",
    "highest",
    "lowest",
)

TREND_KEYWORDS = (
    "xu huong",
    "trend",
    "qua cac nam",
    "theo thoi gian",
    "giai doan",
)

OFF_TOPIC_KEYWORDS = (
    "thoi tiet",
    "viet tho",
    "ke chuyen",
    "nau an",
)


def run_rule_first_router(
    user_message: str,
    conversation_context: dict[str, Any] | None = None,
) -> RuleRouteDraft:
    context = conversation_context or {}
    normalized = normalize_catalog_text(user_message)
    indicator_match = resolve_indicator_alias(user_message)
    unsupported_match = detect_unsupported_indicator(user_message)
    country_groups = [match.group.code for match in resolve_country_groups(user_message)]
    countries = [match.country.code for match in resolve_countries(user_message)]
    years = _extract_years(normalized)
    limit = _extract_limit(normalized)
    ranking_order = _extract_order(normalized)

    if _contains_any(normalized, OFF_TOPIC_KEYWORDS):
        return RuleRouteDraft(
            matched=True,
            route="OFF_TOPIC",
            confidence=0.9,
            needs_front_llm=False,
            needs_parser_agent=False,
            needs_db=False,
            reason="Deterministic off-topic keyword matched.",
        )
    if indicator_match and _contains_any(normalized, GENERAL_EXPLANATION_KEYWORDS):
        return RuleRouteDraft(
            matched=True,
            route="GENERAL_EXPLANATION",
            confidence=0.95,
            needs_front_llm=False,
            needs_parser_agent=False,
            needs_db=False,
            intent_hint="GENERAL_EXPLANATION",
            draft_indicators=[indicator_match.indicator.code],
            reason="General explanation keyword matched with supported indicator.",
        )
    if unsupported_match:
        return RuleRouteDraft(
            matched=True,
            route="DATA_QUERY",
            confidence=0.95,
            needs_front_llm=False,
            needs_parser_agent=True,
            needs_db=False,
            intent_hint="UNSUPPORTED",
            unsupported_terms=[unsupported_match.label_vi or unsupported_match.matched_alias],
            draft_countries=countries,
            draft_country_groups=country_groups,
            draft_start_year=years[0] if years else None,
            draft_end_year=years[-1] if years else None,
            reason="Unsupported indicator matched canonical unsupported catalog.",
        )

    has_previous_answer = bool(
        context.get("last_answer")
        or context.get("last_rows")
        or context.get("last_data_summary")
    )
    has_previous_query = bool(context.get("last_parsed_query") or context.get("last_validated_query"))
    has_explicit_data_query = bool(
        indicator_match
        and (
            _contains_any(normalized, COVERAGE_KEYWORDS)
            or _contains_any(normalized, COMPARE_KEYWORDS)
            or _contains_any(normalized, RANKING_KEYWORDS)
        )
    )

    if has_previous_answer and _contains_any(normalized, FOLLOW_UP_ANALYSIS_KEYWORDS):
        return RuleRouteDraft(
            matched=True,
            route="FOLLOW_UP_ANALYSIS",
            confidence=0.95,
            needs_front_llm=False,
            needs_parser_agent=False,
            needs_db=False,
            uses_previous_context=True,
            reason="Follow-up analysis keyword matched with previous context.",
        )

    delta = _extract_delta(normalized, countries)
    if has_previous_query and not has_explicit_data_query and (_contains_any(normalized, FOLLOW_UP_MODIFY_KEYWORDS) or delta):
        clear_delta = bool(delta)
        return RuleRouteDraft(
            matched=True,
            route="FOLLOW_UP_MODIFY_QUERY",
            confidence=0.9 if clear_delta else 0.75,
            needs_front_llm=not clear_delta,
            needs_parser_agent=True,
            needs_db=True,
            uses_previous_context=True,
            draft_limit=limit,
            draft_ranking_order=ranking_order,
            draft_start_year=years[0] if years else None,
            draft_end_year=years[-1] if years else None,
            delta=delta or None,
            reason="Follow-up query modification matched with previous query context.",
        )

    if _contains_any(normalized, COVERAGE_KEYWORDS):
        has_slot = bool(indicator_match or countries or country_groups)
        return RuleRouteDraft(
            matched=True,
            route="DATA_QUERY",
            confidence=0.9 if indicator_match else (0.85 if has_slot else 0.65),
            needs_front_llm=not bool(indicator_match),
            needs_parser_agent=True,
            needs_db=True,
            intent_hint="COVERAGE",
            draft_indicators=[indicator_match.indicator.code] if indicator_match else [],
            draft_countries=countries,
            draft_country_groups=country_groups,
            reason="Coverage keyword matched.",
        )

    if (
        indicator_match
        and _contains_any(normalized, COMPARE_KEYWORDS)
        and (len(countries) >= 2 or country_groups)
    ):
        return RuleRouteDraft(
            matched=True,
            route="DATA_QUERY",
            confidence=0.92,
            needs_front_llm=False,
            needs_parser_agent=True,
            needs_db=True,
            intent_hint="COMPARE_COUNTRIES",
            draft_indicators=[indicator_match.indicator.code],
            draft_countries=countries,
            draft_country_groups=country_groups,
            draft_start_year=years[0] if years else None,
            draft_end_year=years[-1] if years else None,
            reason="Deterministic compare query matched indicator and country slots.",
        )

    if indicator_match and _contains_any(normalized, RANKING_KEYWORDS):
        return RuleRouteDraft(
            matched=True,
            route="DATA_QUERY",
            confidence=0.92,
            needs_front_llm=False,
            needs_parser_agent=True,
            needs_db=True,
            intent_hint="RANKING",
            draft_indicators=[indicator_match.indicator.code],
            draft_start_year=years[0] if years else None,
            draft_end_year=years[-1] if years else None,
            draft_limit=limit or 10,
            draft_ranking_order=ranking_order or "desc",
            reason="Deterministic ranking query matched indicator and ranking slots.",
        )

    if (
        indicator_match
        and (countries or country_groups)
        and (years or _contains_any(normalized, TREND_KEYWORDS))
    ):
        return RuleRouteDraft(
            matched=True,
            route="DATA_QUERY",
            confidence=0.9,
            needs_front_llm=False,
            needs_parser_agent=True,
            needs_db=True,
            intent_hint="TREND_ANALYSIS" if _contains_any(normalized, TREND_KEYWORDS) else "TIME_SERIES",
            draft_indicators=[indicator_match.indicator.code],
            draft_countries=countries,
            draft_country_groups=country_groups,
            draft_start_year=years[0] if years else None,
            draft_end_year=years[-1] if years else None,
            reason="Deterministic time-series query matched indicator and country slots.",
        )

    is_compare_or_ranking = _contains_any(normalized, COMPARE_KEYWORDS) or _contains_any(normalized, RANKING_KEYWORDS)
    if is_compare_or_ranking and not indicator_match and not has_previous_query:
        return RuleRouteDraft(
            matched=True,
            route="NEED_CLARIFICATION",
            confidence=0.9,
            needs_front_llm=False,
            needs_parser_agent=False,
            needs_db=False,
            clarification_reason="missing_indicator",
            clarification_questions=[
                "Bạn muốn phân tích chỉ số nào? Ví dụ: nợ công/GDP, lạm phát CPI, thất nghiệp, tăng trưởng GDP thực."
            ],
            reason="Compare/ranking query is missing indicator and has no previous context.",
        )

    return RuleRouteDraft(
        matched=False,
        confidence=0.0,
        needs_front_llm=True,
        needs_parser_agent=True,
        reason="No reliable deterministic route matched.",
    )


def _contains_any(normalized_text: str, keywords: tuple[str, ...]) -> bool:
    return any(keyword in normalized_text for keyword in keywords)


def _extract_years(normalized_text: str) -> list[int]:
    return [int(year) for year in re.findall(r"\b((?:19|20)\d{2})\b", normalized_text)]


def _extract_limit(normalized_text: str) -> int | None:
    match = re.search(r"\btop\s+(\d+)\b", normalized_text)
    if not match:
        return None
    return max(1, min(int(match.group(1)), 50))


def _extract_order(normalized_text: str) -> str | None:
    if any(token in normalized_text for token in ("thap nhat", "lowest", "nho nhat")):
        return "asc"
    if any(token in normalized_text for token in ("cao nhat", "highest", "lon nhat", "top")):
        return "desc"
    return None


def _extract_delta(normalized_text: str, countries: list[str]) -> dict[str, Any]:
    delta: dict[str, Any] = {}
    years = _extract_years(normalized_text)
    if years:
        delta["year"] = years[-1] if len(years) == 1 else None
        delta["start_year"] = years[0]
        delta["end_year"] = years[-1]
    limit = _extract_limit(normalized_text)
    if limit is not None:
        delta["limit"] = limit
    ranking_order = _extract_order(normalized_text)
    if ranking_order:
        delta["ranking_order"] = ranking_order
    if countries and "them" in normalized_text:
        delta["add_countries"] = countries
    elif countries:
        delta["countries"] = countries
    return {key: value for key, value in delta.items() if value is not None}
