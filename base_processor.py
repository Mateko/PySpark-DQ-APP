from abc import ABC, abstractmethod
from pyspark.sql import DataFrame
from spark_manager import SparkManager

__all__ = ['BaseProcessor']

class BaseProcessor(ABC):
    def __init__(self):
        self.spark_manager = SparkManager()
        self.spark = self.spark_manager.spark

    @abstractmethod
    def process(self, data: DataFrame) -> DataFrame:
        pass

    def validate_input(self, df: DataFrame) -> bool:
        """Validate input DataFrame"""
        return df is not None and not df.rdd.isEmpty()

    def cleanup(self):
        """Cleanup resources"""
        pass
