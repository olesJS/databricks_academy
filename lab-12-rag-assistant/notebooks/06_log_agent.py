# Databricks notebook source
# MAGIC %md
# MAGIC # Log and register the RAG agent
# MAGIC Logs `agent/agent.py` (an MLflow `ResponsesAgent` wrapping `rag.qa.ask`) as a models-from-code
# MAGIC pyfunc with the `rag/` package bundled, validates it, and registers it in Unity Catalog with an alias.
# MAGIC Deployment to a serving endpoint is a separate step and is **not** done here.

# COMMAND ----------

# MAGIC %pip install -q "mlflow>=3.1" databricks-vectorsearch databricks-sdk openai uv

# COMMAND ----------

# MAGIC %restart_python

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("gold_schema", "f1_rag_gold")
dbutils.widgets.text("model_name", "f1_rag_agent")
dbutils.widgets.text("alias", "champion")

# COMMAND ----------

catalog = dbutils.widgets.get("catalog")
gold_schema = dbutils.widgets.get("gold_schema")
model_name = dbutils.widgets.get("model_name")
alias = dbutils.widgets.get("alias")

UC_MODEL = f"{catalog}.{gold_schema}.{model_name}"

# COMMAND ----------

import os
import sys
from importlib.metadata import version

# lab root on sys.path, so that `rag` and `agent` can be imported
LAB_ROOT = os.path.abspath("..")
sys.path.append(LAB_ROOT)

import mlflow
from mlflow import MlflowClient
from mlflow.models.resources import DatabricksServingEndpoint, DatabricksVectorSearchIndex

from rag.config import EMBEDDING_MODEL, LLM_ENDPOINT, VS_INDEX

USER = spark.sql("SELECT current_user()").first()[0]
mlflow.set_experiment(f"/Users/{USER}/lab12-f1-rag")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Smoke test the agent in the notebook

# COMMAND ----------

from mlflow.types.responses import ResponsesAgentRequest

from agent.agent import F1RagAgent

input_example = {
    "input": [{"role": "user", "content": "Which team did Lewis Hamilton join in 2025?"}]
}

response = F1RagAgent().predict(ResponsesAgentRequest(**input_example)).model_dump()
print(response["output"][0]["content"][0]["text"])
print(response["custom_outputs"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Log the agent
# MAGIC Resources enable automatic auth passthrough: the endpoint gets credentials for exactly these objects.

# COMMAND ----------

resources = [
    DatabricksVectorSearchIndex(index_name=VS_INDEX),
    DatabricksServingEndpoint(endpoint_name=LLM_ENDPOINT),
    DatabricksServingEndpoint(endpoint_name=EMBEDDING_MODEL),  # managed embeddings for query_text
]

pip_requirements = [
    f"{pkg}=={version(pkg)}"
    for pkg in ("mlflow", "databricks-vectorsearch", "databricks-sdk", "openai")
]

with mlflow.start_run(run_name="log_f1_rag_agent"):
    model_info = mlflow.pyfunc.log_model(
        name="agent",
        python_model=os.path.join(LAB_ROOT, "agent", "agent.py"),
        code_paths=[os.path.join(LAB_ROOT, "rag")],  # bundle the rag package with the model
        resources=resources,
        input_example=input_example,
        pip_requirements=pip_requirements,
    )

print(model_info.model_uri)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validate the logged model

# COMMAND ----------

# 1. load the logged artifact and predict
loaded = mlflow.pyfunc.load_model(model_info.model_uri)
prediction = loaded.predict(input_example)
answer = prediction["output"][0]["content"][0]["text"]
print(answer)
assert answer.strip(), "empty answer"
assert "Sources:" in answer, "answer has no sources appended"

# COMMAND ----------

# 2. pre-deployment check in a fresh environment built from pip_requirements
mlflow.models.predict(
    model_uri=model_info.model_uri,
    input_data=input_example,
    env_manager="uv",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Register in Unity Catalog

# COMMAND ----------

mlflow.set_registry_uri("databricks-uc")

registered = mlflow.register_model(model_info.model_uri, UC_MODEL)
MlflowClient().set_registered_model_alias(UC_MODEL, alias, registered.version)

print(f"Registered {UC_MODEL} version {registered.version} with alias '{alias}'")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next step (not done here)
# MAGIC Deploy `models:/<catalog>.<schema>.f1_rag_agent@champion` to a Model Serving endpoint,
# MAGIC e.g. with `databricks.agents.deploy(UC_MODEL, registered.version)`.
