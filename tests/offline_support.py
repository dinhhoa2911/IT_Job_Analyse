"""Nạp nguyên module cần kiểm thử, chặn dependencies ngoài trước khi import.

Không import main, router, vector_search thật, config thật hoặc scripts ETL.
Giữ nguyên constants và Pydantic schemas của dự án.
"""

import importlib.util
from pathlib import Path
import sys
from types import ModuleType, SimpleNamespace
from unittest.mock import Mock


ROOT = Path(__file__).resolve().parents[1]
BACKEND = ROOT / "chatbot-api"


def _load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def load_targets():
    config = ModuleType("config")
    config.settings = SimpleNamespace(
        anthropic_api_key="offline-placeholder",
        anthropic_model="offline-model",
        trino_host="offline.invalid", trino_port=8080,
        trino_user="offline", trino_catalog="iceberg",
        trino_metadata_timeout=5, trino_query_timeout=8,
        trino_forecast_timeout=10, trino_skill_gap_timeout=15,
        trino_learning_timeout=20,
    )
    anthropic = ModuleType("anthropic")
    anthropic.Anthropic = Mock(side_effect=AssertionError("External LLM client blocked"))
    trino = ModuleType("trino")
    trino.dbapi = SimpleNamespace(
        Connection=object,
        connect=Mock(side_effect=AssertionError("External Trino connection blocked")),
    )
    vector = ModuleType("services.vector_search")
    vector.HybridSearchService = Mock(side_effect=AssertionError("External search blocked"))
    models = ModuleType("models")
    models.__path__ = [str(BACKEND / "models")]
    services = ModuleType("services")
    services.__path__ = [str(BACKEND / "services")]
    overrides = {
        "config": config, "anthropic": anthropic, "trino": trino,
        "models": models, "services": services, "services.vector_search": vector,
    }
    # Chỉ khôi phục tên bị thay; giữ dependency Pydantic nạp lười trong import.
    # patch.dict toàn registry sẽ xóa dependency mới nhưng còn cache trên package.
    names = set(overrides) | {
        "constants", "models.schemas", "services.query_classifier",
        "services.cv_matcher", "services.skill_gap_analyzer",
    }
    missing = object()
    previous = {name: sys.modules.get(name, missing) for name in names}
    sys.modules.update(overrides)
    try:
        constants = _load("constants", BACKEND / "constants.py")
        schemas = _load("models.schemas", BACKEND / "models/schemas.py")
        classifier = _load("services.query_classifier", BACKEND / "services/query_classifier.py")
        matcher = _load("services.cv_matcher", BACKEND / "services/cv_matcher.py")
        gap = _load("services.skill_gap_analyzer", BACKEND / "services/skill_gap_analyzer.py")
    finally:
        for name, module in previous.items():
            if module is missing:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = module
    return SimpleNamespace(
        constants=constants, schemas=schemas, classifier=classifier,
        matcher=matcher, gap=gap, anthropic=anthropic, trino=trino,
    )
