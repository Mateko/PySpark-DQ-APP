# Data Quality Application

A PySpark-based application for data quality monitoring and comparison between data partitions.

## Features

- Interactive PySpark shell
- Data quality metrics comparison
- Support for CSV, Parquet, and JSON files
- Automatic data directory management
- Statistical analysis of data changes

## Setup

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Directory structure:
```
dq_app/
├── data/
│   ├── input/    # Place input files here
│   ├── output/   # Processed files
│   └── temp/     # Temporary files
```

## Usage

1. Start the interactive shell:
```bash
python main.py
```

2. Basic usage examples:
```python
# Load data
df = importer.read_csv('mydata.csv')

# Add partition column
df = df.withColumn('dt', F.current_date())

# Compare partitions
metrics = quality_checker.compare_partitions(df, partition_col='dt')
report = quality_checker.generate_report(metrics)

# Show significant changes
significant_df = quality_checker.get_significant_changes(metrics)
significant_df.show()
```

## Metrics

Available metrics for comparison:
- Record count
- Distinct values count
- Null values count
- Minimum value
- Maximum value
- Mean
- Standard deviation
