import os
import sys

# Add the current directory to Python path
current_dir = os.path.dirname(os.path.abspath(__file__))
if current_dir not in sys.path:
    sys.path.append(current_dir)

import logging
import IPython
from spark_manager import SparkManager
from base_processor import BaseProcessor
from data_importer import DataImporter
from data_quality import DataQualityChecker

class SampleProcessor(BaseProcessor):
    def process(self, data):
        # Implement your processing logic here
        return data

def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

def start_shell():
    """Start an interactive IPython shell with SparkSession"""
    setup_logging()
    logger = logging.getLogger(__name__)
    
    # Initialize Spark
    spark_manager = SparkManager()
    spark = spark_manager.spark
    
    # Create sample processor and data importer
    processor = SampleProcessor()
    importer = DataImporter(spark)
    
    # Create quality checker
    quality_checker = DataQualityChecker(spark)
    
    # Make objects available in shell namespace
    shell_namespace = {
        'spark': spark,
        'processor': processor,
        'logger': logger,
        'importer': importer,
        'quality_checker': quality_checker
    }
    
    # Start IPython shell with usage instructions
    banner = """PySpark Interactive Shell
Available objects:
    - spark: SparkSession
    - processor: Data processor
    - logger: Logger
    - importer: Data importer utility
    - quality_checker: Data quality checker

Example usage:
    df = importer.read_parquet('mydata.parquet')
    metrics = quality_checker.compare_partitions(df, 'dt')
    report = quality_checker.generate_report(metrics)
    
Available directories:
    Input files: {input_dir}
    Output files: {output_dir}
    Temporary files: {temp_dir}
""".format(
        input_dir=importer.input_dir,
        output_dir=importer.output_dir,
        temp_dir=importer.temp_dir
    )
    
    IPython.embed(user_ns=shell_namespace, banner1=banner)

if __name__ == "__main__":
    try:
        start_shell()
    except KeyboardInterrupt:
        print("\nExiting...")
    finally:
        # Ensure spark session is stopped
        SparkManager().stop()

