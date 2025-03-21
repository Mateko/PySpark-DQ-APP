"""Module for data quality checking and metrics comparison."""

from typing import Dict, List

from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.types import (BooleanType, DoubleType, StringType, StructField,
                              StructType)
from pyspark.sql.window import Window

__all__ = ['DataQualityChecker']


class DataQualityChecker:
    """Class for comparing data quality metrics between partitions."""

    def __init__(self, spark: SparkSession) -> None:
        """Initialize DataQualityChecker.

        Args:
            spark: SparkSession instance
        """
        self.spark = spark
        self.metrics = {
            'count': lambda col: F.count(col),
            'distinct_count': lambda col: F.countDistinct(col),
            'null_count': lambda col: F.sum(
                F.when(F.col(col).isNull(), 1).otherwise(0)),
            'min': lambda col: F.min(F.col(col).cast('double')),
            'max': lambda col: F.max(F.col(col).cast('double')),
            'mean': lambda col: F.avg(F.col(col).cast('double')),
            'stddev': lambda col: F.stddev(F.col(col).cast('double'))
        }

    def compare_partitions(self, df: DataFrame, partition_col: str = 'dt', 
                         n_partitions: int = 2) -> DataFrame:
        """Compare data quality metrics between partitions.

        Args:
            df: Input DataFrame
            partition_col: Column name to partition by
            n_partitions: Number of partitions to compare

        Returns:
            DataFrame with comparison metrics
        """
        window_spec = (Window
                      .partitionBy(F.lit(1))
                      .orderBy(F.desc(partition_col)))
        
        latest_partitions = (df.select(partition_col)
                           .distinct()
                           .withColumn('rn', F.row_number().over(window_spec))
                           .filter(F.col('rn') <= n_partitions)
                           .drop('rn')
                           .collect())
        
        if len(latest_partitions) < 2:
            raise ValueError("Need at least 2 partitions for comparison")
            
        partition_values = [row[partition_col] for row in latest_partitions]
        return self._compare_metrics(df, partition_col, partition_values)

    def _compare_metrics(self, df: DataFrame, partition_col: str, 
                        partitions: List) -> DataFrame:
        """Compare metrics for specified partitions.

        Args:
            df: Input DataFrame
            partition_col: Column name to partition by
            partitions: List of partition values

        Returns:
            DataFrame with comparison metrics
        """
        allowed_types = ('integer', 'long', 'double', 'float', 'string', 'decimal')
        numeric_cols = [
            f.name for f in df.schema.fields 
            if (f.dataType.typeName() in allowed_types and f.name != partition_col)
        ]
        
        metrics_df = None
        for col in numeric_cols:
            col_metrics = self._calculate_column_metrics(df, col, partition_col, partitions)
            metrics_df = col_metrics if metrics_df is None else metrics_df.union(col_metrics)
            
        return metrics_df

    def _calculate_column_metrics(self, df: DataFrame, column: str, 
                                partition_col: str, partitions: List) -> DataFrame:
        """Calculate metrics for a specific column.

        Args:
            df: Input DataFrame
            column: Column name to calculate metrics for
            partition_col: Column name to partition by
            partitions: List of partition values

        Returns:
            DataFrame with calculated metrics
        """
        metrics = []
        for metric_name, metric_func in self.metrics.items():
            expr = metric_func(column).alias(metric_name)
            metrics.append(expr)

        return (df.filter(F.col(partition_col).isin(partitions))
                .withColumn(column, F.col(column).cast('double'))
                .groupBy(partition_col)
                .agg(*metrics)
                .withColumn('column_name', F.lit(column)))

    def generate_report(self, metrics_df: DataFrame) -> DataFrame:
        """Generate a report of metrics comparison.

        Args:
            metrics_df: DataFrame with calculated metrics

        Returns:
            DataFrame with comparison report
        """
        columns = (metrics_df
                  .select('column_name')
                  .distinct()
                  .collect())
        
        dfs_to_union = []
        for col_row in columns:
            col_name = col_row['column_name']
            col_metrics = (metrics_df
                .filter(F.col('column_name') == col_name)
                .select(
                    F.lit(col_name).alias('column'),
                    *[F.first(metric).alias(f"{metric}_old") for metric in self.metrics.keys()],
                    *[F.last(metric).alias(f"{metric}_new") for metric in self.metrics.keys()]
                ))
            
            for metric in self.metrics.keys():
                col_metrics = (col_metrics
                    .withColumn(
                        f"{metric}_change_%",
                        F.when(
                            (F.col(f"{metric}_old").isNotNull()) & 
                            (F.col(f"{metric}_new").isNotNull()),
                            F.when(
                                F.col(f"{metric}_old") != 0,
                                F.round(
                                    ((F.col(f"{metric}_new") - F.col(f"{metric}_old")) / 
                                     F.col(f"{metric}_old") * 100),
                                    2
                                )
                            ).otherwise(
                                F.when(F.col(f"{metric}_new") != F.col(f"{metric}_old"), 
                                      float('inf')
                                ).otherwise(0.0)
                            )
                        ).otherwise(None)
                    )
                    .withColumn(
                        f"{metric}_significant",
                        F.when(
                            (F.col(f"{metric}_old").isNotNull()) & 
                            (F.col(f"{metric}_new").isNotNull()),
                            (F.abs(F.col(f"{metric}_change_%")) > 5) |
                            (F.col(f"{metric}_old") != F.col(f"{metric}_new"))
                        ).otherwise(False)
                    ))
            
            dfs_to_union.append(col_metrics)
        
        if not dfs_to_union:
            return self.spark.createDataFrame([], self._get_report_schema())
            
        result_df = dfs_to_union[0]
        for df in dfs_to_union[1:]:
            result_df = result_df.union(df)
            
        return result_df.orderBy('column')
        
    def _get_report_schema(self) -> StructType:
        """Get schema for the report DataFrame.

        Returns:
            StructType schema for the report
        """
        fields = [StructField('column', StringType(), True)]
        
        for metric in self.metrics.keys():
            fields.extend([
                StructField(f"{metric}_old", DoubleType(), True),
                StructField(f"{metric}_new", DoubleType(), True),
                StructField(f"{metric}_change_%", DoubleType(), True),
                StructField(f"{metric}_significant", BooleanType(), True)
            ])
            
        return StructType(fields)

    def get_significant_changes(self, metrics_df: DataFrame) -> DataFrame:
        """Get significant changes from the metrics DataFrame.

        Args:
            metrics_df: DataFrame with calculated metrics

        Returns:
            DataFrame with significant changes
        """
        report = self.generate_report(metrics_df)
        
        significant_conditions = [
            F.col(f"{metric}_significant") == True 
            for metric in self.metrics.keys()
        ]
        
        return report.filter(F.array_contains(F.array(*significant_conditions), True))
