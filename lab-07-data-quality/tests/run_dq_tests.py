import sys
import os
import pytest

sys.dont_write_bytecode = True

if len(sys.argv) < 4:
    raise RuntimeError("Usage: run_dq_tests.py <project_root> <catalog> <base_schema>")

root, catalog, schema = sys.argv[1:4]

sys.path.insert(0, os.path.join(root, "src"))

os.environ["DQ_CATALOG"] = catalog
os.environ["DQ_SCHEMA"] = schema

code = pytest.main([
    os.path.join(root, "tests", "dq"),
    "-v",
    "-p", "no:cacheprovider",
])

if code != 0:
    raise RuntimeError(f"DQ gate failed with exit code {int(code)}")