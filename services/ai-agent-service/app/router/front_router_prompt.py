import json
from dataclasses import asdict
from typing import Any

from app.catalog.catalog_prompt_builder import (
    build_compact_country_group_catalog_for_prompt,
    build_compact_indicator_catalog_for_prompt,
    build_compact_unsupported_indicator_catalog_for_prompt,
)


ALLOWED_FRONT_ROUTES = [
    "DATA_QUERY",
    "FOLLOW_UP_ANALYSIS",
    "GENERAL_EXPLANATION",
    "NEED_CLARIFICATION",
    "UNSUPPORTED",
    "OFF_TOPIC",
]

ALLOWED_FRONT_INTENTS = [
    "COMPARE_COUNTRIES",
    "RANKING",
    "TIME_SERIES",
    "TREND_ANALYSIS",
    "ANOMALY_DETECTION",
    "COVERAGE",
    "VALUE_LOOKUP",
    "NEED_CLARIFICATION",
    "UNSUPPORTED",
    "OFF_TOPIC",
    "GENERAL_EXPLANATION",
]


def build_front_router_prompt(
    user_message: str,
    conversation_context: dict[str, Any],
    rule_route_draft: Any | None = None,
) -> str:
    payload = {
        "user_message": user_message,
        "conversation_context": conversation_context,
        "rule_route_draft": asdict(rule_route_draft) if rule_route_draft else None,
        "supported_indicator_catalog_compact": build_compact_indicator_catalog_for_prompt(max_aliases_per_indicator=5),
        "unsupported_indicator_catalog_compact": build_compact_unsupported_indicator_catalog_for_prompt(),
        "country_group_catalog_compact": build_compact_country_group_catalog_for_prompt(),
        "allowed_routes": ALLOWED_FRONT_ROUTES,
    }

    return f"""
Bạn là Front LLM Router / Context Rewriter cho hệ thống phân tích dữ liệu kinh tế - xã hội.

Vai trò:
- Bạn chỉ route, rewrite câu hỏi thành standalone query, hoặc trả lời định nghĩa/khái niệm ngắn.
- Bạn KHÔNG phải composer cuối cho câu trả lời dữ liệu.
- Bạn KHÔNG viết SQL.
- Bạn KHÔNG query DB.
- Bạn KHÔNG output JSON structured final query như indicators/countries/year.
- Bạn KHÔNG quyết định final DB support; Normalization Guard và Query Validator sẽ kiểm tra sau.
- Bạn KHÔNG trả lời số liệu nếu câu hỏi cần DB.

Quy tắc route:
- DATA_QUERY: user hỏi số liệu, so sánh, ranking, trend, anomaly, coverage. Output rewritten_query standalone. answer phải null. needs_parser=true, needs_db=true.
- Follow-up sửa câu trước như thêm quốc gia, đổi năm, đổi top N: vẫn route DATA_QUERY và rewritten_query phải là câu standalone đã merge với context.
- FOLLOW_UP_ANALYSIS: user muốn giải thích/nhận xét kết quả đã có. Không parser, không DB.
- GENERAL_EXPLANATION: user hỏi định nghĩa/ý nghĩa chỉ số. Có thể trả lời ngắn trong answer. Không parser, không DB.
- NEED_CLARIFICATION: chỉ dùng khi thật sự thiếu thông tin không thể suy từ context. clarification_question phải non-null.
- UNSUPPORTED: yêu cầu chỉ số/khả năng không hỗ trợ rõ ràng.
- OFF_TOPIC: ngoài phạm vi chỉ số kinh tế - xã hội.

Ràng buộc nghiệp vụ:
- current account/GDP và external debt/GNI đang unsupported trừ khi dữ liệu được bổ sung sau này.
- trade openness được hỗ trợ về mặt khái niệm; downstream guard sẽ map sang trade_pct_gdp.
- GDP per capita được downstream guard map sang log_rGDP_pc_USD.
- ASEAN/G7/BRICS giữ trong rewritten_query tự nhiên; Guard/Validator sẽ mở rộng nhóm.

Schema output bắt buộc, JSON object duy nhất, không markdown:
{{
  "route": "DATA_QUERY",
  "answer": null,
  "rewritten_query": "standalone query or null",
  "needs_parser": true,
  "needs_db": true,
  "clarification_question": null,
  "uses_previous_context": false,
  "reason": "",
  "confidence": 0.0
}}

Input:
{json.dumps(payload, ensure_ascii=False, indent=2, default=str)}
""".strip()
