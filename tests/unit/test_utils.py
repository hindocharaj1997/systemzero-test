"""
Unit tests for utility modules (config, logging, archive).
"""

import pytest
from pathlib import Path
from unittest.mock import patch

import yaml


# ──────────────────────────────────────────────────────────────────────────────
# Config tests
# ──────────────────────────────────────────────────────────────────────────────

class TestConfigModels:
    """Tests for configuration model classes."""

    def test_data_config_defaults(self):
        from src.utils.config import DataConfig
        cfg = DataConfig()
        assert cfg.input_dir == Path("./data")
        assert cfg.output_dir == Path("./outputs")

    def test_data_config_custom(self):
        from src.utils.config import DataConfig
        cfg = DataConfig(input_dir=Path("/custom/in"), output_dir=Path("/custom/out"))
        assert cfg.input_dir == Path("/custom/in")

    def test_feature_engineering_config_defaults(self):
        from src.utils.config import FeatureEngineeringConfig
        from datetime import date
        cfg = FeatureEngineeringConfig()
        assert cfg.reference_date == date(2026, 2, 1)

    def test_validation_config_defaults(self):
        from src.utils.config import ValidationConfig
        cfg = ValidationConfig()
        assert cfg.strict_mode is False
        assert cfg.max_quarantine_rate == 0.2

    def test_surreal_config_url(self):
        from src.utils.config import SurrealConfig
        cfg = SurrealConfig(host="myhost", port=9000)
        assert cfg.url == "ws://myhost:9000/rpc"

    def test_surreal_config_defaults(self):
        from src.utils.config import SurrealConfig
        cfg = SurrealConfig()
        assert cfg.host == "localhost"
        assert cfg.port == 8000
        assert cfg.namespace == "test"
        assert cfg.database == "ecommerce"

    def test_logging_config_defaults(self):
        from src.utils.config import LoggingConfig
        cfg = LoggingConfig()
        assert cfg.level == "INFO"
        assert cfg.console is True
        assert cfg.file is True

    def test_pipeline_config_defaults(self):
        from src.utils.config import PipelineConfig
        cfg = PipelineConfig()
        assert cfg.name == "ecommerce_data_pipeline"
        assert cfg.version == "1.0.0"

    def test_pipeline_config_properties(self):
        from src.utils.config import PipelineConfig
        cfg = PipelineConfig()
        assert cfg.processed_dir == Path("./outputs/processed")
        assert cfg.quarantine_dir == Path("./outputs/quarantine")
        assert cfg.logs_dir == Path("./outputs/logs")
        assert cfg.archive_dir == Path("./outputs/archive")


class TestLoadConfig:
    """Tests for YAML config loading."""

    def test_load_config_no_file(self, tmp_path):
        """Loading config without a file should return defaults."""
        from src.utils.config import load_config
        # Pass a non-existent path
        cfg = load_config(tmp_path / "nonexistent.yaml")
        assert cfg.name == "ecommerce_data_pipeline"

    def test_load_config_from_yaml(self, tmp_path):
        """Loading config from a YAML file should override defaults."""
        from src.utils.config import load_config

        config_data = {
            "pipeline": {"name": "test_pipeline", "version": "2.0.0"},
            "data": {"input_dir": "/test/data", "output_dir": "/test/out"},
            "surreal": {"host": "db.example.com", "port": 9999},
        }
        config_file = tmp_path / "test_config.yaml"
        with open(config_file, "w") as f:
            yaml.dump(config_data, f)

        cfg = load_config(config_file)
        assert cfg.name == "test_pipeline"
        assert cfg.version == "2.0.0"
        assert cfg.data.input_dir == Path("/test/data")
        assert cfg.surreal.host == "db.example.com"
        assert cfg.surreal.port == 9999

    def test_load_config_default_path(self, tmp_path, monkeypatch):
        """When no path given, should try default config/pipeline_config.yaml."""
        from src.utils.config import load_config
        # Change to tmp dir so default path doesn't exist
        monkeypatch.chdir(tmp_path)
        cfg = load_config(None)
        assert cfg.name == "ecommerce_data_pipeline"  # defaults


# ──────────────────────────────────────────────────────────────────────────────
# Logging tests
# ──────────────────────────────────────────────────────────────────────────────

class TestLoggingSetup:
    """Tests for logging configuration."""

    def test_setup_logging_creates_log_file(self, tmp_path):
        from src.utils.logging import setup_logging
        log_dir = tmp_path / "logs"
        log_file = setup_logging(log_dir, level="DEBUG", console=False, file=True)
        assert log_file.exists()
        assert log_dir.exists()
        assert "pipeline_" in log_file.name

    def test_setup_logging_creates_directory(self, tmp_path):
        from src.utils.logging import setup_logging
        log_dir = tmp_path / "nested" / "logs"
        log_file = setup_logging(log_dir, console=False)
        assert log_dir.exists()

    def test_setup_logging_with_custom_format(self, tmp_path):
        from src.utils.logging import setup_logging
        log_dir = tmp_path / "logs_custom"
        log_file = setup_logging(
            log_dir,
            log_format="{message}",
            console=False,
        )
        assert log_file.exists()

    def test_setup_logging_reconfigure(self, tmp_path):
        """Reconfiguring logging should work without errors."""
        from src.utils.logging import setup_logging
        log_dir = tmp_path / "logs_reconfig"
        log1 = setup_logging(log_dir, console=False)
        log2 = setup_logging(log_dir, console=False)
        # Both should succeed (second call reconfigures)
        assert log1.parent == log2.parent

    def test_get_logger(self):
        from src.utils.logging import get_logger
        log = get_logger("test_module")
        assert log is not None



