from pyspark.sql import SparkSession
from typing import Optional

__all__ = ['SparkManager']

class SparkManager:
    _instance: Optional['SparkManager'] = None
    _spark: Optional[SparkSession] = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(SparkManager, cls).__new__(cls)
        return cls._instance

    def __init__(self):
        if self._spark is None:
            self._spark = (SparkSession.builder
                         .appName("PythonSparkApp")
                         .config("spark.driver.memory", "4g")
                         .config("spark.executor.memory", "4g")
                         .getOrCreate())

    @property
    def spark(self) -> SparkSession:
        return self._spark

    def stop(self):
        if self._spark:
            self._spark.stop()
            self._spark = None
