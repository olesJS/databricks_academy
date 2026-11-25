"""F1 RAG assistant as an MLflow ResponsesAgent (models from code).

Wraps rag.qa.ask so it can be served by a Model Serving endpoint.
The rag/ package is shipped with the model via code_paths (see notebooks/06_log_agent.py).

Optional request.custom_inputs: {"category": str, "chunk_size": int, "k": int}
"""

from collections.abc import Generator
from uuid import uuid4

import mlflow
from mlflow.pyfunc import ResponsesAgent
from mlflow.types.responses import (
    ResponsesAgentRequest,
    ResponsesAgentResponse,
    ResponsesAgentStreamEvent,
)

from rag.config import DEFAULT_CHUNK_SIZE, TOP_K
from rag.qa import NO_ANSWERS, ask


def _as_dict(item) -> dict:
    return item.model_dump() if hasattr(item, "model_dump") else dict(item)


def _last_user_text(request: ResponsesAgentRequest) -> str:
    """Text of the last user message; content may be a string or a list of content parts."""
    for item in reversed(request.input):
        msg = _as_dict(item)
        if msg.get("role") != "user":
            continue
        content = msg.get("content")
        if isinstance(content, str):
            text = content
        else:
            text = " ".join(
                part.get("text", "") for part in (content or []) if isinstance(part, dict)
            )
        if text.strip():
            return text.strip()
    raise ValueError("Request contains no user message with text")


def _format_answer(result: dict) -> str:
    answer = result["answer"]
    if answer in NO_ANSWERS or not result["sources"]:
        return answer
    lines = [f"- {title} § {section} ({url})" for title, section, url in result["sources"]]
    return answer + "\n\nSources:\n" + "\n".join(lines)


class F1RagAgent(ResponsesAgent):
    def _answer(self, request: ResponsesAgentRequest) -> tuple[dict, dict]:
        params = request.custom_inputs or {}
        result = ask(
            _last_user_text(request),
            category=params.get("category"),
            chunk_size=int(params.get("chunk_size", DEFAULT_CHUNK_SIZE)),
            k=int(params.get("k", TOP_K)),
        )
        item = self.create_text_output_item(text=_format_answer(result), id=str(uuid4()))
        custom_outputs = {
            "chunk_ids": result["chunk_ids"],
            "sources": [
                {"title": t, "section": s, "url": u} for t, s, u in result["sources"]
            ],
            "model": result["model"],
            "latency_ms": result["latency_ms"],
        }
        return item, custom_outputs

    def predict(self, request: ResponsesAgentRequest) -> ResponsesAgentResponse:
        item, custom_outputs = self._answer(request)
        return ResponsesAgentResponse(output=[item], custom_outputs=custom_outputs)

    def predict_stream(
        self, request: ResponsesAgentRequest
    ) -> Generator[ResponsesAgentStreamEvent, None, None]:
        # ask() is not streaming, so the whole answer is emitted as one event
        item, custom_outputs = self._answer(request)
        yield ResponsesAgentStreamEvent(
            type="response.output_item.done", item=item, custom_outputs=custom_outputs
        )


mlflow.models.set_model(F1RagAgent())
