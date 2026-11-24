CATALOG = "workspace"
BRONZE_SCHEMA = "f1_rag_bronze"
SILVER_SCHEMA = "f1_rag_silver"
GOLD_SCHEMA = "f1_rag_gold"

BRONZE_TABLE = f"{CATALOG}.{BRONZE_SCHEMA}.wiki_pages"
CHUNKS_TABLE = f"{CATALOG}.{SILVER_SCHEMA}.wiki_chunks"
QA_LOG_TABLE = f"{CATALOG}.{GOLD_SCHEMA}.qa_log"

CHUNK_SIZES = {200: 30, 400: 60}
DEFAULT_CHUNK_SIZE = 400

CATEGORIES = ("driver", "team", "circuit", "season", "general")

# Vector Search
VS_ENDPOINT = "f1_rag_vs"
VS_INDEX = f"{CATALOG}.{SILVER_SCHEMA}.wiki_chunks_index"
EMBEDDING_MODEL = "databricks-gte-large-en"
TOP_K = 5

# LLM (pay-per-token Foundation Model API)
LLM_ENDPOINT = "databricks-meta-llama-3-3-70b-instruct"
LLM_TEMPERATURE = 0.0
LLM_MAX_TOKENS = 500
