"""
Configuration management for the data engineering pipeline.

Uses Pydantic Settings for type-safe configuration with YAML file support
and environment variable overrides.
"""

from pathlib import Path
from typing import Optional
from datetime import date

import yaml
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings


class DataConfig(BaseModel):
    """Data paths configuration."""
    input_dir: Path = Field(default=Path("./data"))
    output_dir: Path = Field(default=Path("./outputs"))


class FeatureEngineeringConfig(BaseModel):
    """Feature engineering configuration."""
    reference_date: date = Field(
        default=date(2026, 2, 1),
        description="Fixed reference date for reproducible time-based calculations"
    )


class ValidationConfig(BaseModel):
    """Validation settings."""
    strict_mode: bool = Field(default=False)
    max_quarantine_rate: float = Field(
        default=0.2,
        description="Maximum acceptable quarantine rate before warning"
    )


class SurrealConfig(BaseModel):
    """SurrealDB connection configuration."""
    host: str = Field(default="localhost")
    port: int = Field(default=8000)
    namespace: str = Field(default="test")
    database: str = Field(default="ecommerce")
    username: str = Field(default="root")
    password: str = Field(default="root")
    
    @property
    def url(self) -> str:
        """Get the WebSocket URL for SurrealDB."""
        return f"ws://{self.host}:{self.port}/rpc"


class LoggingConfig(BaseModel):
    """Logging configuration."""
    level: str = Field(default="INFO")
    format: str = Field(
        default="{time:YYYY-MM-DD HH:mm:ss} | {level:<8} | {name}:{function}:{line} | {message}"
    )
    console: bool = Field(default=True)
    file: bool = Field(default=True)


class PipelineConfig(BaseSettings):
    """
    Main pipeline configuration.
    
    Configuration is loaded from:
    1. Default values
    2. YAML config file (if provided)
    3. Environment variables (prefix: PIPELINE_)
    """
    name: str = Field(default="ecommerce_data_pipeline")
    version: str = Field(default="1.0.0")
    
    data: DataConfig = Field(default_factory=DataConfig)
    feature_engineering: FeatureEngineeringConfig = Field(
        default_factory=FeatureEngineeringConfig
    )
    validation: ValidationConfig = Field(default_factory=ValidationConfig)
    surreal: SurrealConfig = Field(default_factory=SurrealConfig)
    logging: LoggingConfig = Field(default_factory=LoggingConfig)
    
    class Config:
        env_prefix = "PIPELINE_"
        env_nested_delimiter = "__"
    
    @property
    def processed_dir(self) -> Path:
        """Get the processed output directory."""
        return self.data.output_dir / "processed"
    
    @property
    def quarantine_dir(self) -> Path:
        """Get the quarantine output directory."""
        return self.data.output_dir / "quarantine"
    
    @property
    def logs_dir(self) -> Path:
        """Get the logs directory."""
        return self.data.output_dir / "logs"
    
    @property
    def archive_dir(self) -> Path:
        """Get the archive directory."""
        return self.data.output_dir / "archive"


def load_config(config_path: Optional[Path] = None) -> PipelineConfig:
    """
    Load pipeline configuration from YAML file.
    
    Args:
        config_path: Path to YAML config file. If None, uses defaults.
        
    Returns:
        PipelineConfig instance with loaded settings.
    """
    if config_path is None:
        # Try default location
        default_path = Path("config/pipeline_config.yaml")
        if default_path.exists():
            config_path = default_path
    
    if config_path and config_path.exists():
        with open(config_path, "r") as f:
            yaml_config = yaml.safe_load(f)
        
        # Extract nested configs
        pipeline_config = yaml_config.get("pipeline", {})
        
        return PipelineConfig(
            name=pipeline_config.get("name", "ecommerce_data_pipeline"),
            version=pipeline_config.get("version", "1.0.0"),
            data=DataConfig(**yaml_config.get("data", {})),
            feature_engineering=FeatureEngineeringConfig(
                **yaml_config.get("feature_engineering", {})
            ),
            validation=ValidationConfig(**yaml_config.get("validation", {})),
            surreal=SurrealConfig(**yaml_config.get("surreal", {})),
            logging=LoggingConfig(**yaml_config.get("logging", {})),
        )
    
    return PipelineConfig()
