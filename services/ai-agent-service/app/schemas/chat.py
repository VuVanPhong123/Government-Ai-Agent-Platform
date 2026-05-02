from typing import Any, Literal

from pydantic import BaseModel, Field


QuestionType = Literal[
    "OFF_TOPIC",
    "NEED_CLARIFICATION",
    "VALID_SIMPLE_QUERY",
    "VALID_COMPARE_QUERY",
    "VALID_RANKING_QUERY",
    "VALID_TREND_QUERY",
    "VALID_ANOMALY_QUERY",
    "VALID_COVERAGE_QUERY",
    "UNSUPPORTED_DATA_QUERY",
]


ChartType = Literal[
    "line",
    "bar",
    "scatter",
    "table",
    "none",
]


class AiChatRequest(BaseModel):
    message: str = Field(..., min_length=1)
    conversationId: str | None = None
    context: dict[str, Any] | None = None


class AiAgentChartConfig(BaseModel):
    type: ChartType = "none"
    title: str | None = None
    xKey: str | None = None
    yKeys: list[str] | None = None
    data: list[dict[str, Any]] | None = None


class AiAgentMetadata(BaseModel):
    source: Literal["template", "gemini", "mock"] = "mock"
    toolsUsed: list[str] = []
    indicators: list[str] = []
    countries: list[str] = []
    years: list[int] = []


class AiChatResponse(BaseModel):
    answer: str
    questionType: QuestionType = "VALID_SIMPLE_QUERY"
    data: list[dict[str, Any]] = []
    chart: AiAgentChartConfig = AiAgentChartConfig()
    warnings: list[str] = []
    metadata: AiAgentMetadata = AiAgentMetadata()


class HealthResponse(BaseModel):
    status: Literal["ok", "degraded", "error"] = "ok"
    service: str
    version: str