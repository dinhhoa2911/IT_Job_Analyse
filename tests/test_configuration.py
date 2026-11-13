"""Test Settings thật và cấu hình Spark riêng, không import/thực thi ETL."""

import ast
from contextlib import contextmanager
import importlib.util
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

from dotenv import dotenv_values
from pydantic import ValidationError


ROOT = Path(__file__).resolve().parents[1]
REQUIRED = {
    "ANTHROPIC_API_KEY": "offline-anthropic-placeholder",
    "MINIO_ACCESS_KEY": "offline-access-placeholder",
    "MINIO_SECRET_KEY": "offline-secret-placeholder",
}


def load_module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    previous = sys.modules.get(name)
    sys.modules[name] = module
    try:
        spec.loader.exec_module(module)
    finally:
        if previous is None:
            sys.modules.pop(name, None)
        else:
            sys.modules[name] = previous
    return module


@contextmanager
def isolated_environment(env):
    # Không đọc .env trên máy thật hoặc để credentials hiện có ảnh hưởng test.
    previous = Path.cwd()
    with tempfile.TemporaryDirectory() as folder, patch.dict(os.environ, env, clear=True):
        try:
            os.chdir(folder)
            yield Path(folder)
        finally:
            os.chdir(previous)


class BackendConfigurationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        with isolated_environment(REQUIRED):
            cls.config = load_module("_backend_config_test", ROOT / "chatbot-api/config.py")

    def test_non_sensitive_defaults_and_optional_openai(self):
        expected = {
            "anthropic_model": "claude-sonnet-4-6",
            "milvus_host": "milvus-standalone", "milvus_port": 19530,
            "milvus_collection": "it_jobs_rag", "trino_host": "trino",
            "trino_port": 8080, "trino_catalog": "iceberg", "trino_user": "chatbot",
            "embedding_model": "text-embedding-3-large", "embedding_dim": 3072,
            "reranker_model": "cross-encoder/ms-marco-MiniLM-L-6-v2",
            "minio_endpoint": "minio:9000", "minio_cv_bucket": "user-cvs",
            "top_k_results": 10, "retrieval_k": 30, "rerank_k": 20,
            "trino_metadata_timeout": 5, "trino_query_timeout": 8,
            "trino_forecast_timeout": 10, "trino_skill_gap_timeout": 15,
            "trino_learning_timeout": 20, "openai_api_key": "",
        }
        with isolated_environment(REQUIRED):
            settings = self.config.Settings(_env_file=None)
        for name, value in expected.items():
            with self.subTest(field=name):
                self.assertEqual(getattr(settings, name), value)

    def test_environment_overrides_defaults_with_integer_conversion(self):
        env = dict(REQUIRED, MILVUS_HOST="fixture-milvus", MILVUS_PORT="19531",
                   TRINO_PORT="9090", TRINO_QUERY_TIMEOUT="12", TOP_K_RESULTS="4",
                   MINIO_ENDPOINT="fixture-minio:9000", OPENAI_API_KEY="offline-openai")
        with isolated_environment(env):
            settings = self.config.Settings(_env_file=None)
        self.assertEqual((settings.milvus_host, settings.milvus_port), ("fixture-milvus", 19531))
        self.assertEqual((settings.trino_port, settings.trino_query_timeout), (9090, 12))
        self.assertEqual(settings.top_k_results, 4)
        self.assertEqual(settings.minio_endpoint, "fixture-minio:9000")
        self.assertEqual(settings.openai_api_key, "offline-openai")

    def test_kwargs_over_environment_over_dotenv_over_default(self):
        with isolated_environment(REQUIRED) as folder:
            (folder / ".env").write_text("TRINO_HOST=dotenv-trino\nTRINO_PORT=8082\n", encoding="utf-8")
            self.assertEqual(self.config.Settings().trino_host, "dotenv-trino")
            with patch.dict(os.environ, {"TRINO_HOST": "env-trino"}):
                self.assertEqual(self.config.Settings().trino_host, "env-trino")
                self.assertEqual(self.config.Settings(trino_host="kwargs-trino").trino_host, "kwargs-trino")
            self.assertEqual(self.config.Settings(_env_file=None).trino_host, "trino")

    def test_missing_required_credentials(self):
        for name in REQUIRED:
            env = {key: value for key, value in REQUIRED.items() if key != name}
            with self.subTest(field=name), isolated_environment(env):
                with self.assertRaises(ValidationError) as caught:
                    self.config.Settings(_env_file=None)
                self.assertIn((name.lower(),), [err["loc"] for err in caught.exception.errors()])

    def test_empty_required_credentials_and_invalid_integer(self):
        for name in REQUIRED:
            with self.subTest(field=name), isolated_environment(dict(REQUIRED, **{name: ""})):
                with self.assertRaises(ValidationError):
                    self.config.Settings(_env_file=None)
        with isolated_environment(dict(REQUIRED, TRINO_PORT="invalid-port-placeholder")):
            with self.assertRaises(ValidationError) as caught:
                self.config.Settings(_env_file=None)
            self.assertNotIn("invalid-port-placeholder", str(caught.exception))

    def test_credentials_hidden_from_repr(self):
        with isolated_environment(REQUIRED):
            settings = self.config.Settings(_env_file=None)
        for value in REQUIRED.values():
            self.assertNotIn(value, repr(settings))

    def test_backend_example_is_loadable_and_preserves_contract_defaults(self):
        with isolated_environment({}):
            settings = self.config.Settings(_env_file=ROOT / "chatbot-api/.env.example")
        self.assertEqual(settings.embedding_dim, 3072)
        self.assertEqual(settings.milvus_collection, "it_jobs_rag")
        self.assertEqual(settings.trino_query_timeout, 8)


class SparkConfigurationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.config = load_module("_spark_config_test", ROOT / "scripts/spark_config.py")

    def test_defaults_preserve_distinct_minio_and_hdfs_paths(self):
        settings = self.config.load_settings({})
        self.assertEqual(settings.minio_endpoint, "http://minio:9000")
        self.assertEqual(settings.hive_metastore_uri, "thrift://hive-metastore:9083")
        self.assertEqual(settings.warehouse_path, "s3a://warehouse/iceberg_data")
        self.assertEqual(settings.weekly_warehouse_path, "hdfs://dinhhoa-master:9000/user/ndh/warehouse")
        self.assertEqual(settings.cluster_warehouse_path, "hdfs://tuankiet170-master:9000/data_lake/warehouse")
        self.assertEqual((settings.hdfs_host, settings.hdfs_port, settings.hdfs_user), ("dinhhoa-master", 9000, "root"))
        self.assertEqual((settings.milvus_host, settings.milvus_port, settings.embedding_batch_size), ("milvus-standalone", "19530", 500))

    def test_environment_overrides_all_connection_groups(self):
        env = dict(
            SPARK_MINIO_ENDPOINT="http://fixture-minio:9000", HIVE_METASTORE_URI="thrift://fixture-hive:9083",
            SPARK_WAREHOUSE_PATH="s3a://fixture/warehouse", SPARK_WEEKLY_WAREHOUSE_PATH="hdfs://fixture/weekly",
            SPARK_CLUSTER_WAREHOUSE_PATH="hdfs://fixture/cluster", HDFS_HOST="fixture-hdfs",
            HDFS_PORT="9870", HDFS_USER="fixture", MILVUS_HOST="fixture-milvus", MILVUS_PORT="19531",
            EMBEDDING_BATCH_SIZE="20", MINIO_ACCESS_KEY="fixture-access", MINIO_SECRET_KEY="fixture-secret",
            OPENAI_API_KEY="fixture-openai",
        )
        with patch.dict(os.environ, env, clear=True):
            settings = self.config.load_settings(require_minio=True, require_openai=True)
        self.assertEqual((settings.minio_endpoint, settings.warehouse_path), (env["SPARK_MINIO_ENDPOINT"], env["SPARK_WAREHOUSE_PATH"]))
        self.assertEqual(settings.hive_metastore_uri, env["HIVE_METASTORE_URI"])
        self.assertEqual((settings.weekly_warehouse_path, settings.cluster_warehouse_path), (env["SPARK_WEEKLY_WAREHOUSE_PATH"], env["SPARK_CLUSTER_WAREHOUSE_PATH"]))
        self.assertEqual((settings.hdfs_host, settings.hdfs_port, settings.hdfs_user), ("fixture-hdfs", 9870, "fixture"))
        self.assertEqual((settings.milvus_host, settings.milvus_port, settings.embedding_batch_size), ("fixture-milvus", "19531", 20))
        self.assertEqual((settings.minio_access_key, settings.minio_secret_key, settings.openai_api_key), ("fixture-access", "fixture-secret", "fixture-openai"))

    def test_missing_credentials_are_required_only_for_consuming_branch(self):
        self.assertEqual(self.config.load_settings({}).hdfs_port, 9000)
        for env in [{}, {"MINIO_ACCESS_KEY": "fixture"}, {"MINIO_ACCESS_KEY": " ", "MINIO_SECRET_KEY": "fixture"}]:
            with self.subTest(env=env), self.assertRaisesRegex(ValueError, "MINIO_"):
                self.config.load_settings(env, require_minio=True)
        with self.assertRaisesRegex(ValueError, "OPENAI_API_KEY"):
            self.config.load_settings({}, require_openai=True)

    def test_bad_integer_errors_do_not_echo_values(self):
        for name in ["HDFS_PORT", "EMBEDDING_BATCH_SIZE"]:
            for value in ["invalid-fixture", "0", "-1"]:
                with self.subTest(field=name, value=value), self.assertRaises(ValueError) as caught:
                    self.config.load_settings({name: value})
                self.assertIn(name, str(caught.exception))
                self.assertNotIn("invalid-fixture", str(caught.exception))

    def test_credentials_hidden_from_repr(self):
        settings = self.config.load_settings({"MINIO_ACCESS_KEY": "fixture-access", "MINIO_SECRET_KEY": "fixture-secret", "OPENAI_API_KEY": "fixture-openai"})
        for value in ["fixture-access", "fixture-secret", "fixture-openai"]:
            self.assertNotIn(value, repr(settings))

    def test_spark_example_loadable_without_external_dependencies(self):
        settings = self.config.load_settings(dotenv_values(ROOT / "scripts/.env.example"), require_minio=True, require_openai=True)
        self.assertEqual(settings.milvus_port, "19530")
        self.assertEqual(settings.embedding_batch_size, 500)

    def test_driver_configuration_bindings_without_executing_etl(self):
        settings = self.config.load_settings({"SPARK_MINIO_ENDPOINT": "http://fixture:9000", "HDFS_PORT": "9870", "EMBEDDING_BATCH_SIZE": "12"})
        cases = {
            "Load_Bronze.py": {"MINIO_ENDPOINT": "http://fixture:9000"},
            "Clean_Silver.py": {"WAREHOUSE_PATH": "s3a://warehouse/iceberg_data"},
            "Build_Gold.py": {"HIVE_METASTORE": "thrift://hive-metastore:9083"},
            "Prophet.py": {"WAREHOUSE_PATH": "s3a://warehouse/iceberg_data"},
            "Prophetweek.py": {"WAREHOUSE_PATH": settings.weekly_warehouse_path},
            "Vectorize_To_Milvus.py": {"BATCH_SIZE": 12, "MILVUS_PORT": "19530"},
            "puthdfs.py": {"HDFS_PORT": 9870},
            "fix_and_merge_minio.py": {"MINIO_ENDPOINT": "http://fixture:9000"},
        }
        for name, expected in cases.items():
            tree = ast.parse((ROOT / "scripts" / name).read_text(encoding="utf-8"))
            assignments = {n.targets[0].id: n.value for n in tree.body if isinstance(n, ast.Assign) and isinstance(n.targets[0], ast.Name)}
            for field, value in expected.items():
                with self.subTest(script=name, field=field):
                    node = assignments[field]
                    self.assertIsInstance(node, ast.Attribute)
                    self.assertEqual(node.value.id, "job_settings")
                    self.assertEqual(getattr(settings, node.attr), value)
