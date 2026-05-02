from fastapi import APIRouter, Depends

from app.core.security import verify_internal_api_key
from app.schemas.chat import AiChatRequest, AiChatResponse, AiAgentChartConfig, AiAgentMetadata

router = APIRouter(
    prefix="/agent",
    tags=["agent"],
    dependencies=[Depends(verify_internal_api_key)],
)


@router.post("/chat", response_model=AiChatResponse)
def chat(payload: AiChatRequest) -> AiChatResponse:
    normalized_message = payload.message.strip()

    return AiChatResponse(
        answer=(
            "AI Agent Service đã nhận được câu hỏi. "
            "Đây hiện là mock response của Phase 2. "
            f"Câu hỏi của bạn là: {normalized_message}"
        ),
        questionType="VALID_SIMPLE_QUERY",
        data=[
            {
                "message": normalized_message,
                "conversationId": payload.conversationId,
                "context": payload.context,
            }
        ],
        chart=AiAgentChartConfig(
            type="none",
            title=None,
            xKey=None,
            yKeys=None,
            data=None,
        ),
        warnings=[
            "Phase 2 mới dựng skeleton, chưa query database và chưa gọi Gemini."
        ],
        metadata=AiAgentMetadata(
            source="mock",
            toolsUsed=[],
            indicators=[],
            countries=[],
            years=[],
        ),
    )