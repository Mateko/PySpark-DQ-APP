"""Module for application configuration."""

from dataclasses import dataclass
from typing import Dict


@dataclass
class SparkConfig:
    """Configuration for Spark settings."""

    app_name: str = "Spark DQ APP"
    master: str = "local[*]"
    log_level: str = "WARN"

@dataclass
class AppConfig:
    spark: SparkConfig = SparkConfig()
    
    @property
    def spark_configs(self) -> Dict[str, str]:
        return {
            "spark.app.name": self.spark.app_name,
            "spark.master": self.spark.master
        }

# Ensure these are available at module level
__all__ = ['SparkConfig', 'AppConfig']
