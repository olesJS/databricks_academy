import sys
import os
import pytest

sys.dont_write_bytecode = True

if len(sys.argv) < 2:
    raise RuntimeError("Expected the project root as the first argument")

root = sys.argv[1]

src_path = os.path.join(root, "src")
sys.path.insert(0, src_path)

unit_tests_path = os.path.join(root, "tests", "unit")

code = pytest.main([unit_tests_path, "-v", "-p", "no:cacheprovider"])

if code != 0:
    raise RuntimeError(f"pytest failed with exit code {int(code)}")