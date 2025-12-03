import os

# nếu muốn lấy từ env (an toàn), bạn có thể:

import sys
import importlib
SECRET_KEY = os.environ.get("SUPERSET_SECRET_KEY") or "kMeiPru+Q3Ov+yZJ0Z55uMx8nmZTlQJQ+tWGdHUhNpeyvD/PHmFu2wZd"

# Bảo đảm Python path có thư viện đã cài
sys.path.append("/usr/local/lib/python3.11/site-packages")

# Ép Superset nạp TrinoEngineSpec
try:
    from superset.db_engine_specs import engines
    trino_module = importlib.import_module("superset.db_engine_specs.trino")
    engines.engines["trino"] = trino_module.TrinoEngineSpec
    print("✅ TrinoEngineSpec loaded successfully.")
except Exception as e:
    print("⚠️ Cannot load TrinoEngineSpec:", e)
