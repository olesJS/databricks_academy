import time

import mlflow
from mlflow.entities import SpanType
from databricks.sdk import WorkspaceClient
from databricks.vector_search.client import VectorSearchClient

from rag.config import (
    CATEGORIES, DEFAULT_CHUNK_SIZE, LLM_ENDPOINT, LLM_MAX_TOKENS, LLM_TEMPERATURE,
    TOP_K, VS_ENDPOINT, VS_INDEX,
)

RESULT_COLUMNS = ["chunk_id", "title", "category", "section", "chunk_text", "url"]

_index = None

# every LLM call through the OpenAI client becomes a span in the trace (prompt, answer, tokens)
mlflow.openai.autolog()


def get_index():
    global _index
    if _index is None:
        client = VectorSearchClient(disable_notice=True)
        _index = client.get_index(endpoint_name=VS_ENDPOINT, index_name=VS_INDEX)
    return _index


def retrieve(
    query: str,
    k: int = TOP_K,
    category: str | None = None,
    chunk_size: int = DEFAULT_CHUNK_SIZE
) -> list[dict]:
    """Return the top-k chunks for a question."""
    if category is not None and category not in CATEGORIES:
        raise ValueError(f"Unknown category '{category}', expected one of {CATEGORIES}")

    filters = {"chunk_size": chunk_size}
    if category is not None:
        filters["category"] = category

    with mlflow.start_span(name="vector_search", span_type=SpanType.RETRIEVER) as span:
        span.set_inputs({"query": query, "k": k, "filters": filters})

        response = get_index().similarity_search(
            query_text=query,
            columns=RESULT_COLUMNS,
            filters=filters,
            num_results=k,
            query_type="HYBRID"
        )

        rows = response.get("result", {}).get("data_array") or []
        # every row = requested columns + score as the last element
        results = [dict(zip(RESULT_COLUMNS + ["score"], row)) for row in rows]

        # MLflow's document format
        span.set_outputs([
            {
                "id": r["chunk_id"],
                "page_content": r["chunk_text"],
                "metadata": {
                    "title": r["title"], 
                    "section": r["section"],
                    "url": r["url"], 
                    "score": r["score"]
                }
            }
            for r in results
        ])

    return results


NO_ANSWER_RAG = "I don't know based on the available articles."
NO_ANSWER_NO_RAG = "I don't know."
NO_ANSWERS = (NO_ANSWER_RAG, NO_ANSWER_NO_RAG)

SYSTEM_PROMPT_RAG = f"""You answer questions about Formula 1.
Use ONLY the context below, taken from Wikipedia articles.
- If the context does not contain the answer, reply exactly: "{NO_ANSWER_RAG}"
- Do not use outside knowledge, even if you are sure.
- Cite the sources you used in square brackets, like [Lewis Hamilton § Formula One career].
- Be concise: 1-4 sentences."""

SYSTEM_PROMPT_NO_RAG = f"""You answer questions about Formula 1.
- If you are not sure, reply exactly: "{NO_ANSWER_NO_RAG}"
- Be concise: 1-4 sentences."""

_llm = None


def get_llm():
    global _llm
    if _llm is None:
        _llm = WorkspaceClient().serving_endpoints.get_open_ai_client()
    return _llm


def format_context(chunks: list[dict]) -> str:
    return "\n\n".join(
        f"[{i}] {c["title"]} at {c["section"]}\n{c["chunk_text"]}"
        for i, c in enumerate(chunks, start=1)
    )


@mlflow.trace(name="ask", span_type=SpanType.CHAIN)
def ask(
    question: str,
    category: str | None = None,
    use_retrieval: bool = True,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
    k: int = TOP_K
) -> dict:
    started = time.perf_counter()
    chunks = retrieve(question, k=k, category=category, chunk_size=chunk_size) if use_retrieval else []

    result = {
        "question": question,
        "category": category,
        "use_retrieval": use_retrieval,
        "chunk_size": chunk_size if use_retrieval else None,
        "model": LLM_ENDPOINT,
        "chunk_ids": [c["chunk_id"] for c in chunks],
        "sources": list({(c["title"], c["section"], c["url"]): None for c in chunks}),
        "prompt_tokens": 0,
        "completion_tokens": 0,
    }

    if use_retrieval and not chunks:
        result["answer"] = NO_ANSWER_RAG
        result["latency_ms"] = (time.perf_counter() - started) * 1000
        return result
    
    if use_retrieval:
        messages = [
            {"role": "system", "content": SYSTEM_PROMPT_RAG},
            {"role": "user", "content": f"Context:\n{format_context(chunks)}\n\nQuestion: {question}"}
        ]
    else:
        messages = [
            {"role": "system", "content": SYSTEM_PROMPT_NO_RAG},
            {"role": "user", "content": question}
        ]
    
    response = get_llm().chat.completions.create(
        model=LLM_ENDPOINT,
        messages=messages,
        temperature=LLM_TEMPERATURE,
        max_tokens=LLM_MAX_TOKENS
    )

    result["answer"] = response.choices[0].message.content.strip()
    result["promp_tokens"] = response.usage.prompt_tokens
    result["completion_tokens"] = response.usage.completion_tokens
    result["latency_ms"] = (time.perf_counter() - started) * 1000
    return result
