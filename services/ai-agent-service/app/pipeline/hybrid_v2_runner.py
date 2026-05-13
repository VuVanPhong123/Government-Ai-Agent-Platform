import logging
from dataclasses import asdict
from typing import Any

from app.composer.chart_composer import (
    build_anomaly_bar_chart_data,
    build_compare_line_chart_data,
    build_ranking_bar_chart_data,
    build_series_line_chart_data,
)
from app.composer.deterministic_guard import append_result_warnings
from app.composer.display_formatter import get_indicator_label, sanitize_user_facing_text
from app.composer.template_composer import (
    compose_anomaly_answer,
    compose_compare_answer,
    compose_coverage_answer,
    compose_need_clarification_answer,
    compose_off_topic_answer,
    compose_ranking_answer,
    compose_trend_answer,
    compose_unsupported_answer,
    sanitize_clarification_questions,
)
from app.conversation.context_store import summarize_rows, update_conversation_context
from app.conversation.followup_merge import merge_followup_query
from app.core.config import settings
from app.executor.tool_executor import execute_query_plan
from app.catalog.canonical_indicator_catalog import resolve_indicator_alias
from app.catalog.country_group_catalog import resolve_country_groups
from app.parser.parser_service_client import call_parser_service
from app.parser.parser_agent import run_parser_agent
from app.planner.validated_plan_adapter import build_plan_from_validated_query
from app.resolver.country_resolver import resolve_countries
from app.router.front_router_adapter import build_front_router_draft_from_existing_router
from app.router.rule_first_router import run_rule_first_router
from app.schemas.chat import AiAgentChartConfig, AiAgentMetadata, AiChatRequest, AiChatResponse
from app.validator.query_validator import validate_parsed_candidate
from app.validator.result_validator import result_validation_to_dict, validate_tool_result


logger = logging.getLogger(__name__)


def run_hybrid_v2_pipeline(
    payload: AiChatRequest,
    message: str,
    previous_context: dict[str, Any],
    router_context: dict[str, Any],
    router_decision: Any | None = None,
) -> AiChatResponse | None:
    try:
        rule_draft = run_rule_first_router(message, router_context)
        front_draft = build_front_router_draft_from_existing_router(None, rule_draft)
        front_router_decision = None
        executed_front_router = False
        model_parsed: dict[str, Any] | None = None
        parser_model_debug: dict[str, Any] = {
            "executed_model_parser": False,
            "parserServiceAvailable": False,
            "parserModelDebug": {},
        }

        if rule_draft.route == "FOLLOW_UP_ANALYSIS" and rule_draft.confidence >= 0.9:
            return _followup_analysis_response(
                payload,
                message,
                previous_context,
                router_context,
                rule_draft,
                _router_debug(
                    "FOLLOW_UP_ANALYSIS",
                    rule_draft,
                    front_draft,
                    front_router_decision,
                    executed_front_router,
                    parser_model_debug,
                ),
            )
        if rule_draft.route == "NEED_CLARIFICATION" and rule_draft.confidence >= 0.9:
            return _clarification_response(
                payload,
                message,
                rule_draft.clarification_questions,
                rule_draft,
                router_debug=_router_debug(
                    "NEED_CLARIFICATION",
                    rule_draft,
                    front_draft,
                    front_router_decision,
                    executed_front_router,
                    parser_model_debug,
                ),
            )
        if rule_draft.route == "OFF_TOPIC" and rule_draft.confidence >= 0.9:
            return _off_topic_response(
                payload,
                message,
                rule_draft,
                _router_debug(
                    "OFF_TOPIC",
                    rule_draft,
                    front_draft,
                    front_router_decision,
                    executed_front_router,
                    parser_model_debug,
                ),
            )
        if _looks_like_general_explanation(message):
            return None

        if _should_execute_front_router(rule_draft):
            front_router_decision = router_decision or _route_with_front_llm(message, router_context)
            executed_front_router = True
            router_result = front_router_decision.to_dict() if hasattr(front_router_decision, "to_dict") else None
            front_draft = build_front_router_draft_from_existing_router(router_result, rule_draft)

        effective_message = message

        if rule_draft.route == "FOLLOW_UP_MODIFY_QUERY" and rule_draft.delta:
            previous_query = previous_context.get("last_validated_query") or previous_context.get("last_parsed_query") or {}
            merged_query = merge_followup_query(previous_query, rule_draft.delta)
            model_parsed = _model_parsed_from_query(merged_query)
            effective_message = f"{previous_context.get('last_user_message') or ''} {message}".strip()
        elif rule_draft.route == "FOLLOW_UP_MODIFY_QUERY" and front_draft.rewritten_query:
            effective_message = front_draft.rewritten_query

        if model_parsed is None and _should_call_parser_model(effective_message, rule_draft, front_draft):
            model_candidate, parser_model_debug = _call_parser_model_candidate(
                effective_message,
                message,
                router_context,
                rule_draft,
                front_draft,
            )
            if model_candidate:
                model_parsed = model_candidate

        candidate = run_parser_agent(
            user_message=effective_message,
            conversation_context=router_context,
            rule_draft=rule_draft,
            front_draft=front_draft,
            model_parsed=model_parsed,
        )
        validation = validate_parsed_candidate(candidate)

        if validation.status == "needs_clarification" and not executed_front_router and rule_draft.confidence < 0.9:
            front_router_decision = _route_with_front_llm(message, router_context)
            executed_front_router = True
            front_draft = build_front_router_draft_from_existing_router(front_router_decision.to_dict(), rule_draft)
            candidate = run_parser_agent(
                user_message=effective_message,
                conversation_context=router_context,
                rule_draft=rule_draft,
                front_draft=front_draft,
                model_parsed=model_parsed,
            )
            validation = validate_parsed_candidate(candidate)

        if validation.status == "needs_clarification" and not parser_model_debug["executed_model_parser"]:
            model_candidate, parser_model_debug = _call_parser_model_candidate(
                effective_message,
                message,
                router_context,
                rule_draft,
                front_draft,
            )
            if model_candidate:
                model_parsed = model_candidate
            candidate = run_parser_agent(
                user_message=effective_message,
                conversation_context=router_context,
                rule_draft=rule_draft,
                front_draft=front_draft,
                model_parsed=model_parsed,
            )
            validation = validate_parsed_candidate(candidate)

        router_debug = _router_debug(
            validation.validated_query.get("route") if validation.validated_query else _debug_route_for_invalid(candidate, validation),
            rule_draft,
            front_draft,
            front_router_decision,
            executed_front_router,
            parser_model_debug,
        )

        if not validation.ok:
            if validation.status == "needs_clarification":
                return _clarification_response(payload, message, validation.clarification_questions, rule_draft, candidate, validation, router_debug)
            if validation.status == "unsupported":
                return _unsupported_response(payload, message, validation.unsupported_terms, rule_draft, candidate, validation, router_debug)
            if validation.status == "no_data":
                return _no_data_response(payload, message, validation, router_debug)
            return None

        if not validation.validated_query:
            return None

        plan = build_plan_from_validated_query(validation.validated_query)
        if plan.tool_name == "none":
            return None

        executed = execute_query_plan(plan)
        result = executed["result"]
        rows = _rows_from_result(result, plan.question_type)
        result_validation = validate_tool_result(rows, validation.validated_query)
        return _data_response(
            payload=payload,
            message=message,
            candidate=candidate,
            validation=validation,
            plan=plan,
            tool_name=executed["tool"],
            result=result,
            rows=rows,
            result_validation=result_validation_to_dict(result_validation),
            rule_draft=rule_draft,
            router_debug=router_debug,
        )
    except Exception as error:
        logger.exception(
            "Hybrid v2 pipeline failed, fallback_to_legacy=%s: %s",
            settings.enable_hybrid_v2_fallback,
            error,
        )
        if settings.enable_hybrid_v2_fallback:
            return None
        raise


def _followup_analysis_response(
    payload: AiChatRequest,
    message: str,
    previous_context: dict[str, Any],
    router_context: dict[str, Any],
    rule_draft: Any,
    router_debug: dict[str, Any] | None = None,
) -> AiChatResponse:
    summary = router_context.get("last_data_summary") or {}
    indicator = get_indicator_label(summary.get("indicator")) if summary.get("indicator") else "kết quả trước đó"
    row_count = summary.get("row_count")
    detail = f" với {row_count} dòng dữ liệu" if row_count else ""
    answer = (
        f"Dựa trên {indicator}{detail}, có thể nhận xét định tính rằng khác biệt trong kết quả thường phản ánh "
        "bối cảnh kinh tế, cấu trúc chính sách và chất lượng dữ liệu của từng nước. "
        "Đây là phân tích định tính, không phải bằng chứng nhân quả trực tiếp."
    )
    response = AiChatResponse(
        answer=sanitize_user_facing_text(answer),
        questionType="VALID_SIMPLE_QUERY",
        status="success",
        data=[{"message": message, "route": "FOLLOW_UP_ANALYSIS", "previousDataSummary": summary}],
        chart=AiAgentChartConfig(type="none"),
        warnings=[],
        metadata=_metadata("template", ["rule_first_router", "conversation_context"], rule_draft=rule_draft),
        parsedQuery=previous_context.get("last_parsed_query") or None,
        parserDebug=None,
        routerDebug=router_debug or {"route": "FOLLOW_UP_ANALYSIS", "pipeline": "hybrid_v2", "ruleFirst": asdict(rule_draft)},
    )
    update_conversation_context(
        payload.conversationId,
        {
            "last_user_message": message,
            "last_answer": response.answer,
            "last_route": "FOLLOW_UP_ANALYSIS",
            "last_status": "success",
            "last_question_type": response.questionType,
        },
    )
    return response


def _clarification_response(
    payload: AiChatRequest,
    message: str,
    questions: list[str],
    rule_draft: Any,
    candidate: Any | None = None,
    validation: Any | None = None,
    router_debug: dict[str, Any] | None = None,
) -> AiChatResponse:
    clean_questions = sanitize_clarification_questions(questions)
    answer = compose_need_clarification_answer(clean_questions)
    response = AiChatResponse(
        answer=answer,
        questionType="NEED_CLARIFICATION",
        status="needs_clarification",
        clarificationQuestions=clean_questions,
        data=[{"message": message, "route": "NEED_CLARIFICATION"}],
        chart=AiAgentChartConfig(type="none"),
        warnings=clean_questions,
        metadata=_metadata("template", ["rule_first_router"], rule_draft=rule_draft, candidate=candidate, validation=validation),
        parsedQuery=asdict(candidate) if candidate else None,
        parserDebug=asdict(candidate) if candidate else None,
        routerDebug=router_debug or {"route": "NEED_CLARIFICATION", "pipeline": "hybrid_v2", "ruleFirst": asdict(rule_draft)},
    )
    _update_basic(payload.conversationId, message, answer, "NEED_CLARIFICATION", "needs_clarification", response.questionType)
    return response


def _unsupported_response(
    payload: AiChatRequest,
    message: str,
    unsupported_terms: list[str],
    rule_draft: Any,
    candidate: Any,
    validation: Any,
    router_debug: dict[str, Any] | None = None,
) -> AiChatResponse:
    warnings = [f"Hiện hệ thống chưa có chỉ số {term} trong dữ liệu hiện có." for term in unsupported_terms] or [validation.reason]
    answer = compose_unsupported_answer(warnings)
    response = AiChatResponse(
        answer=answer,
        questionType="UNSUPPORTED_DATA_QUERY",
        status="unsupported",
        data=[{"message": message, "route": "UNSUPPORTED", "unsupportedTerms": unsupported_terms}],
        chart=AiAgentChartConfig(type="none"),
        warnings=[],
        metadata=_metadata("template", ["rule_first_router", "parser_agent", "query_validator"], rule_draft=rule_draft, candidate=candidate, validation=validation, unsupported_terms=unsupported_terms),
        parsedQuery=asdict(candidate),
        parserDebug=asdict(candidate),
        routerDebug=router_debug or {"route": "DATA_QUERY", "intent": "UNSUPPORTED", "pipeline": "hybrid_v2", "ruleFirst": asdict(rule_draft)},
    )
    _update_basic(payload.conversationId, message, answer, "UNSUPPORTED", "unsupported", response.questionType, parsed_query=asdict(candidate))
    return response


def _off_topic_response(
    payload: AiChatRequest,
    message: str,
    rule_draft: Any,
    router_debug: dict[str, Any] | None = None,
) -> AiChatResponse:
    answer = compose_off_topic_answer()
    response = AiChatResponse(
        answer=answer,
        questionType="OFF_TOPIC",
        status="off_topic",
        data=[{"message": message, "route": "OFF_TOPIC"}],
        chart=AiAgentChartConfig(type="none"),
        warnings=[],
        metadata=_metadata("template", ["rule_first_router"], rule_draft=rule_draft),
        parsedQuery=None,
        parserDebug=None,
        routerDebug=router_debug or {"route": "OFF_TOPIC", "pipeline": "hybrid_v2", "ruleFirst": asdict(rule_draft)},
    )
    _update_basic(payload.conversationId, message, answer, "OFF_TOPIC", "off_topic", response.questionType)
    return response


def _no_data_response(
    payload: AiChatRequest,
    message: str,
    validation: Any,
    router_debug: dict[str, Any] | None = None,
) -> AiChatResponse:
    answer = "Không tìm thấy dữ liệu phù hợp cho yêu cầu này."
    response = AiChatResponse(
        answer=answer,
        questionType="NO_DATA",
        status="no_data",
        data=[{"message": message, "route": "DATA_QUERY"}],
        chart=AiAgentChartConfig(type="none"),
        warnings=validation.warnings,
        metadata=_metadata("template", ["query_validator"], validation=validation),
        parsedQuery=None,
        parserDebug=None,
        routerDebug=router_debug or {"route": "DATA_QUERY", "pipeline": "hybrid_v2", "status": "no_data"},
    )
    _update_basic(payload.conversationId, message, answer, "DATA_QUERY", "no_data", response.questionType)
    return response


def _data_response(
    payload: AiChatRequest,
    message: str,
    candidate: Any,
    validation: Any,
    plan: Any,
    tool_name: str,
    result: Any,
    rows: list[dict],
    result_validation: dict[str, Any],
    rule_draft: Any,
    router_debug: dict[str, Any],
) -> AiChatResponse:
    validated_query = validation.validated_query or {}
    indicator_code = validated_query.get("indicator")
    countries = validated_query.get("countries") or []
    start_year = validated_query.get("effective_start_year")
    end_year = validated_query.get("effective_end_year")
    warnings = _dedupe_strings([*validation.warnings, *plan.warnings, *result_validation.get("warnings", [])])
    question_type = plan.question_type

    if question_type == "VALID_COMPARE_QUERY":
        result_rows = result.get("rows", rows) if isinstance(result, dict) else rows
        answer = compose_compare_answer(indicator_code, countries, start_year, end_year, result_rows, result_validation)
        chart = AiAgentChartConfig(type="line" if result_rows else "none", title=f"So sánh {get_indicator_label(indicator_code)}", xKey="year", yKeys=countries, data=build_compare_line_chart_data(result_rows))
        data_item = {"indicator": indicator_code, "countries": countries, "coverage": result.get("coverage", []) if isinstance(result, dict) else [], "rows": result_rows}
    elif question_type == "VALID_RANKING_QUERY":
        answer = compose_ranking_answer(indicator_code, plan.arguments.get("year"), rows, plan.arguments.get("limit"), plan.arguments.get("order"))
        chart = AiAgentChartConfig(type="bar" if rows else "none", title=f"Top quốc gia theo {get_indicator_label(indicator_code)}", xKey="country_code", yKeys=["value"], data=build_ranking_bar_chart_data(rows))
        data_item = {"indicator": indicator_code, "year": plan.arguments.get("year"), "rows": rows}
    elif question_type == "VALID_COVERAGE_QUERY":
        answer = compose_coverage_answer(indicator_code, rows)
        chart = AiAgentChartConfig(type="table" if rows else "none", title=f"Phạm vi dữ liệu {get_indicator_label(indicator_code)}", data=rows)
        data_item = {"indicator": indicator_code, "rows": rows}
    elif question_type == "VALID_ANOMALY_QUERY":
        answer = compose_anomaly_answer(indicator_code, countries, start_year, end_year, rows)
        chart = AiAgentChartConfig(type="bar" if rows else "none", title=f"Điểm bất thường của {get_indicator_label(indicator_code)}", xKey="year", yKeys=["anomaly_score"], data=build_anomaly_bar_chart_data(rows))
        data_item = {"indicator": indicator_code, "countries": countries, "rows": rows}
    else:
        is_analytics = plan.tool_name == "get_indicator_analytics_series"
        answer = compose_trend_answer(indicator_code, countries, start_year, end_year, rows, is_analytics)
        chart_data = rows if is_analytics else build_series_line_chart_data(rows)
        y_keys = ["actual_value", "trend_value"] if is_analytics else ["value"]
        chart = AiAgentChartConfig(type="line" if rows else "none", title=f"Xu hướng {get_indicator_label(indicator_code)}", xKey="year", yKeys=y_keys, data=chart_data)
        data_item = {"indicator": indicator_code, "countries": countries, "is_analytics_series": is_analytics, "rows": rows}

    answer = append_result_warnings(sanitize_user_facing_text(answer), warnings)
    metadata = _metadata(
        "template",
        ["rule_first_router", "parser_agent", "query_validator", "validated_plan_adapter", tool_name, "result_validator"],
        rule_draft=rule_draft,
        candidate=candidate,
        validation=validation,
        result_validation=result_validation,
        unsupported_terms=validation.unsupported_terms,
        missing_countries=result_validation.get("missing_countries", []),
        validated_query=validated_query,
    )
    data_item = {
        "message": message,
        "conversationId": payload.conversationId,
        "resolved": _resolved_metadata(validated_query),
        "plan": _plan_to_dict(plan),
        "route": validated_query.get("route"),
        "resultValidation": result_validation,
        **data_item,
    }
    response = AiChatResponse(
        answer=answer,
        questionType=question_type,
        status="success",
        data=[data_item],
        chart=chart,
        warnings=warnings,
        metadata=metadata,
        parsedQuery=asdict(candidate),
        parserDebug=asdict(candidate),
        routerDebug={
            **router_debug,
            "route": validated_query.get("route"),
            "executed_parser_agent": True,
            "executed_db": True,
            "needs_parser": bool(rule_draft.needs_parser_agent),
            "needs_db": bool(rule_draft.needs_db),
        },
    )
    _update_query_success(payload.conversationId, message, response, candidate, validated_query, plan, rows, chart, result_validation)
    return response


def _rows_from_result(result: Any, question_type: str) -> list[dict]:
    if question_type == "VALID_COMPARE_QUERY" and isinstance(result, dict):
        return result.get("rows") or []
    if isinstance(result, list):
        return result
    return []


def _debug_route_for_invalid(candidate: Any, validation: Any) -> str:
    if validation.status in {"unsupported", "no_data"}:
        return "DATA_QUERY"
    if validation.status == "needs_clarification":
        return "NEED_CLARIFICATION"
    route = getattr(candidate, "route", None)
    return route if route else str(validation.status)


def _looks_like_general_explanation(message: str) -> bool:
    normalized = message.lower()
    return any(
        token in normalized
        for token in (
            "là gì",
            "la gi",
            "nghĩa là gì",
            "nghia la gi",
            "ý nghĩa",
            "y nghia",
            "cách hiểu",
            "cach hieu",
            "dùng để",
            "dung de",
        )
    )


def _should_execute_front_router(rule_draft: Any) -> bool:
    return bool(rule_draft.needs_front_llm or rule_draft.confidence < 0.9)


def _route_with_front_llm(message: str, router_context: dict[str, Any]) -> Any:
    from app.router.gemini_router import route_message

    return route_message(message, router_context)


def _should_call_parser_model(message: str, rule_draft: Any, front_draft: Any) -> bool:
    route = rule_draft.route
    if route == "FOLLOW_UP_ANALYSIS":
        return False
    if route == "OFF_TOPIC" and rule_draft.confidence >= 0.9:
        return False
    if route == "NEED_CLARIFICATION" and rule_draft.confidence >= 0.9:
        return False
    if rule_draft.unsupported_terms and rule_draft.confidence >= 0.9:
        return False
    if (
        rule_draft.confidence >= 0.9
        and (rule_draft.draft_indicators or rule_draft.unsupported_terms)
        and not _has_complex_slots(message)
    ):
        return False
    if _has_enough_deterministic_slots(message, rule_draft, front_draft):
        confidence = max(
            float(rule_draft.confidence or 0.0),
            float(front_draft.confidence or rule_draft.confidence or 0.0),
        )
        if confidence >= 0.85:
            return False

    combined_indicators = _dedupe_strings([*rule_draft.draft_indicators, *front_draft.draft_indicators])
    combined_countries = _dedupe_strings([*rule_draft.draft_countries, *front_draft.draft_countries])
    combined_groups = _dedupe_strings([*rule_draft.draft_country_groups, *front_draft.draft_country_groups])
    intent = front_draft.intent_hint or rule_draft.intent_hint

    if not rule_draft.matched:
        return True
    if rule_draft.confidence < 0.85:
        return True
    if front_draft.confidence < 0.85:
        return True
    if (route in {"DATA_QUERY", "FOLLOW_UP_MODIFY_QUERY"} or intent) and not combined_indicators and not rule_draft.unsupported_terms:
        return True
    if intent == "COMPARE_COUNTRIES" and len(combined_countries) + len(combined_groups) < 2:
        return True
    if intent in {"TREND_ANALYSIS", "TIME_SERIES", "VALUE_LOOKUP"} and not combined_countries and not combined_groups:
        return True
    if intent == "COVERAGE" and not combined_indicators:
        return True
    return _has_complex_slots(message) and min(rule_draft.confidence, front_draft.confidence) < 0.9


def _has_enough_deterministic_slots(message: str, rule_draft: Any, front_draft: Any) -> bool:
    resolver_indicator = resolve_indicator_alias(message)
    resolver_countries = [match.country.code for match in resolve_countries(message)]
    resolver_groups = [match.group.code for match in resolve_country_groups(message)]

    indicators = _dedupe_strings(
        [
            *rule_draft.draft_indicators,
            *front_draft.draft_indicators,
            *([resolver_indicator.indicator.code] if resolver_indicator else []),
        ]
    )
    countries = _dedupe_strings(
        [
            *rule_draft.draft_countries,
            *front_draft.draft_countries,
            *resolver_countries,
        ]
    )
    groups = _dedupe_strings(
        [
            *rule_draft.draft_country_groups,
            *front_draft.draft_country_groups,
            *resolver_groups,
        ]
    )
    intent = front_draft.intent_hint or rule_draft.intent_hint

    if rule_draft.unsupported_terms:
        return True
    if not indicators:
        return False
    if intent == "COMPARE_COUNTRIES":
        return len(countries) + len(groups) >= 2
    if intent == "RANKING":
        return True
    if intent == "COVERAGE":
        return True
    if intent in {"TREND_ANALYSIS", "TIME_SERIES", "VALUE_LOOKUP"}:
        return len(countries) + len(groups) >= 1
    if indicators and (countries or groups):
        return True
    return False


def _has_complex_slots(message: str) -> bool:
    normalized = message.lower()
    years = [token for token in normalized.split() if token.isdigit() and len(token) == 4]
    slot_markers = sum(
        1
        for token in (" so sánh ", " compare ", " vs ", ",", " và ", " and ", " top ", " thấp nhất", " cao nhất")
        if token in f" {normalized} "
    )
    return len(years) >= 2 and slot_markers >= 2


def _call_parser_model_candidate(
    effective_message: str,
    original_message: str,
    router_context: dict[str, Any],
    rule_draft: Any,
    front_draft: Any,
) -> tuple[dict[str, Any] | None, dict[str, Any]]:
    parser_response = call_parser_service(
        effective_message,
        context={
            "conversation": router_context,
            "rule_draft": asdict(rule_draft),
            "front_draft": asdict(front_draft),
            "original_user_message": original_message,
        },
    )
    parsed = parser_response.get("parsed") if isinstance(parser_response, dict) else None
    parser_debug = _parser_model_debug(parser_response)
    debug = {
        "executed_model_parser": True,
        "parserServiceAvailable": isinstance(parser_response, dict),
        "parserModelDebug": parser_debug,
    }
    if isinstance(parsed, dict) and parsed.get("intent"):
        return parsed, debug
    return None, debug


def _parser_model_debug(parser_response: Any) -> dict[str, Any]:
    if not isinstance(parser_response, dict):
        return {
            "safe_to_execute": False,
            "catalog_pass": False,
            "schema_pass": False,
            "deployment_schema_pass": False,
            "fallback_reason": "parser_service_unavailable",
            "latency_ms": None,
            "intent": None,
        }

    parsed = parser_response.get("parsed") if isinstance(parser_response.get("parsed"), dict) else {}
    return {
        "safe_to_execute": parser_response.get("safe_to_execute"),
        "catalog_pass": parser_response.get("catalog_pass"),
        "schema_pass": parser_response.get("schema_pass"),
        "deployment_schema_pass": parser_response.get("deployment_schema_pass"),
        "fallback_reason": parser_response.get("fallback_reason"),
        "latency_ms": parser_response.get("latency_ms"),
        "intent": parsed.get("intent"),
    }


def _router_debug(
    route: str | None,
    rule_draft: Any,
    front_draft: Any,
    front_router_decision: Any | None,
    executed_front_router: bool,
    parser_model_debug: dict[str, Any],
) -> dict[str, Any]:
    front_decision_dict = (
        front_router_decision.to_dict()
        if front_router_decision is not None and hasattr(front_router_decision, "to_dict")
        else None
    )
    return {
        "route": route,
        "pipeline": "hybrid_v2",
        "executed_front_router": executed_front_router,
        "frontRouter": asdict(front_draft) if front_draft else None,
        "frontRouterDecision": front_decision_dict,
        "ruleFirst": asdict(rule_draft),
        "executed_model_parser": bool(parser_model_debug.get("executed_model_parser")),
        "parserServiceAvailable": bool(parser_model_debug.get("parserServiceAvailable")),
        "parserModelDebug": parser_model_debug.get("parserModelDebug") or {},
        "executed_parser_agent": False,
        "executed_db": False,
        "needs_parser": bool(rule_draft.needs_parser_agent),
        "needs_db": bool(rule_draft.needs_db),
    }


def _model_parsed_from_query(query: dict[str, Any]) -> dict[str, Any]:
    indicators = query.get("indicators") or ([query["indicator"]] if query.get("indicator") else [])
    return {
        "intent": query.get("intent") or "RANKING",
        "indicators": indicators,
        "countries": query.get("countries") or [],
        "country_groups": query.get("country_groups") or [],
        "start_year": query.get("start_year") or query.get("effective_start_year"),
        "end_year": query.get("end_year") or query.get("effective_end_year"),
        "limit": query.get("limit"),
        "ranking_order": query.get("ranking_order"),
    }


def _dedupe_strings(values: list[Any]) -> list[str]:
    result: list[str] = []
    for value in values:
        text = str(value).strip()
        if text and text not in result:
            result.append(text)
    return result


def _metadata(
    source: str,
    tools_used: list[str],
    rule_draft: Any | None = None,
    candidate: Any | None = None,
    validation: Any | None = None,
    result_validation: dict[str, Any] | None = None,
    unsupported_terms: list[str] | None = None,
    missing_countries: list[str] | None = None,
    validated_query: dict[str, Any] | None = None,
) -> AiAgentMetadata:
    query = validated_query or (validation.validated_query if validation and validation.validated_query else {})
    indicators = query.get("indicators") or (candidate.indicators if candidate else [])
    countries = query.get("countries") or (candidate.countries if candidate else [])
    years = [year for year in (query.get("effective_start_year"), query.get("effective_end_year")) if isinstance(year, int)]
    return AiAgentMetadata(
        source=source,
        toolsUsed=tools_used,
        indicators=list(indicators),
        analytics_indicators=[],
        raw_only_indicators=list(indicators),
        countries=list(countries),
        years=sorted(set(years)),
        resolved=_resolved_metadata(query) if query else None,
        validation=asdict(validation) if validation else None,
        resultValidation=result_validation,
        ruleFirst=asdict(rule_draft) if rule_draft else None,
        parserAgent=asdict(candidate) if candidate else None,
        unsupportedTerms=unsupported_terms or [],
        missingCountries=missing_countries or [],
        pipeline="hybrid_v2",
        fallbackUsed=False,
    )


def _resolved_metadata(query: dict[str, Any]) -> dict[str, Any]:
    return {
        "indicators": query.get("indicators") or [],
        "countries": query.get("countries") or [],
        "country_groups": query.get("country_groups") or [],
        "start_year": query.get("effective_start_year"),
        "end_year": query.get("effective_end_year"),
        "limit": query.get("limit"),
        "ranking_order": query.get("ranking_order"),
        "needs_clarification": False,
        "clarification_questions": [],
    }


def _plan_to_dict(plan: Any) -> dict[str, Any]:
    return {
        "question_type": plan.question_type,
        "tool_name": plan.tool_name,
        "arguments": plan.arguments,
        "warnings": plan.warnings,
    }


def _update_basic(
    conversation_id: str | None,
    message: str,
    answer: str,
    route: str,
    status: str,
    question_type: str,
    parsed_query: dict[str, Any] | None = None,
) -> None:
    patch = {
        "last_user_message": message,
        "last_answer": answer,
        "last_route": route,
        "last_status": status,
        "last_question_type": question_type,
    }
    if parsed_query is not None:
        patch["last_parsed_query"] = parsed_query
    update_conversation_context(conversation_id, patch)


def _update_query_success(
    conversation_id: str | None,
    message: str,
    response: AiChatResponse,
    candidate: Any,
    validated_query: dict[str, Any],
    plan: Any,
    rows: list[dict],
    chart: AiAgentChartConfig,
    result_validation: dict[str, Any],
) -> None:
    row_summary = summarize_rows(rows, settings.conversation_context_max_rows)
    chart_dict = chart.model_dump()
    update_conversation_context(
        conversation_id,
        {
            "last_user_message": message,
            "last_answer": response.answer,
            "last_route": validated_query.get("route"),
            "last_status": response.status,
            "last_question_type": response.questionType,
            "last_parsed_query": asdict(candidate),
            "last_validated_query": validated_query,
            "last_query_plan": _plan_to_dict(plan),
            "last_rows": row_summary["top_rows"],
            "last_chart": {key: value for key, value in chart_dict.items() if key != "data"},
            "last_result_validation": result_validation,
            "last_data_summary": {
                "indicator": validated_query.get("indicator"),
                "countries": validated_query.get("countries") or [],
                "years": [year for year in (validated_query.get("effective_start_year"), validated_query.get("effective_end_year")) if year],
                "start_year": validated_query.get("effective_start_year"),
                "end_year": validated_query.get("effective_end_year"),
                "limit": validated_query.get("limit"),
                "order": validated_query.get("ranking_order"),
                "row_count": row_summary["row_count"],
                "top_rows": row_summary["top_rows"],
            },
        },
    )
