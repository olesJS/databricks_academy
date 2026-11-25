# Instructions for the coding agent

## Context
- Repository with weekly Databricks labs. Current work: `lab-12-rag-assistant/` (RAG assistant over F1 Wikipedia articles).
- Workspace: Databricks Free Edition, CLI profile `uni_free`, serverless compute only.
- Unity Catalog: catalog `workspace`, schemas `f1_rag_bronze`, `f1_rag_silver`, `f1_rag_gold`.
- Reusable code lives in `lab-12-rag-assistant/rag/` (`config.py`, `chunking.py`, `qa.py`). Notebooks: `lab-12-rag-assistant/notebooks/00–05`.

## Scope
- Change files only in `lab-12-rag-assistant/` and `.github/workflows/`. Do not touch other labs.
- Reuse `rag/` modules; do not rewrite or duplicate their logic.

## Databricks
- Dev only: never target or create a `prod` environment.
- Read-only by default: run SQL `SELECT`/`DESCRIBE`/`SHOW` freely, but ask before any statement or tool call that creates, changes or deletes objects.
- Never drop or truncate tables, schemas or volumes; never delete Vector Search endpoints or indexes.
- Do not create paid resources (endpoints, clusters, warehouses) without asking first.

## Secrets
- Never read, print or write tokens, `.env` files or `~/.databrickscfg`.
- CI credentials come only from GitHub Actions secrets.

## Workflow
- Work on a branch; never push to `master`. Deployment happens only through CI after a reviewed PR.
- Before finishing a task: run `pytest` for changed Python code and `databricks bundle validate -t dev` for bundle changes, and show the result.
- Keep changes small and explain what you changed and why.