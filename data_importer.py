import os
from typing import Optional, Dict, Any
from pyspark.sql import DataFrame, SparkSession
from pathlib import Path

class DataImporter:
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.base_dir = Path(__file__).parent
        self.input_dir = self.base_dir / 'data' / 'input'
        self.output_dir = self.base_dir / 'data' / 'output'
        self.temp_dir = self.base_dir / 'data' / 'temp'
        self._create_dirs()

    def _create_dirs(self):
        """Ensure all required directories exist"""
        for dir_path in [self.input_dir, self.output_dir, self.temp_dir]:
            dir_path.mkdir(parents=True, exist_ok=True)

    def read_csv(self, filename: str, **options) -> DataFrame:
        """Read CSV file from input directory"""
        file_path = self.input_dir / filename
        default_options = {
            'header': 'true',
            'inferSchema': 'true'
        }
        return self.spark.read.options(**{**default_options, **options}).csv(str(file_path))

    def read_parquet(self, filename: str) -> DataFrame:
        """Read Parquet file from input directory"""
        file_path = self.input_dir / filename
        return self.spark.read.parquet(str(file_path))

    def read_json(self, filename: str, **options) -> DataFrame:
        """Read JSON file from input directory"""
        file_path = self.input_dir / filename
        return self.spark.read.options(**options).json(str(file_path))

    def save_df(self, df: DataFrame, filename: str, format: str = 'parquet', mode: str = 'overwrite', **options):
        """Save DataFrame to output directory"""
        file_path = self.output_dir / filename
        df.write.format(format).mode(mode).options(**options).save(str(file_path))

    def list_input_files(self) -> Dict[str, list]:
        """List all files in input directory by extension"""
        files = {'csv': [], 'parquet': [], 'json': [], 'other': []}
        for file in self.input_dir.glob('*.*'):
            ext = file.suffix.lower()[1:]
            if ext in files:
                files[ext].append(file.name)
            else:
                files['other'].append(file.name)
        return files
