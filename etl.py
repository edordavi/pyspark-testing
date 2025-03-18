from functools import lru_cache
from pathlib import Path
from pyspark.sql.functions import sum,avg,max,count, col, to_timestamp,min,max
from pyspark import SparkContext
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    BooleanType,
    FloatType,
)

from pyspark.sql.functions import col


@lru_cache(maxsize=1)
def get_spark():
    sc = SparkContext(master="local[1]", appName="ML Logs Transformer")
    spark = SparkSession(sc)
    return spark


def load_logs(logs_path: Path) -> DataFrame:
    """
    TODO(Part 1.1): Complete this method
    """
    spark = get_spark()

    return spark.read.json(logs_path)


    logs = []
    return get_spark().createDataFrame(
        logs,
        StructType(
            [
                StructField("logId", StringType()),
                StructField("expId", IntegerType()),
                StructField("metricId", IntegerType()),
                StructField("valid", BooleanType()),
                StructField("createdAt", StringType()),
                StructField("ingestedAt", StringType()),
                StructField("step", IntegerType()),
                StructField("value", FloatType()),
            ]
        ),
    )


def load_experiments(experiments_path: Path) -> DataFrame:
    """
    TODO(Part 1.2): Complete this method
    """
    spark = get_spark()
    return spark.read.options(delimiter=',',header = True, inferSchema = True).csv(experiments_path)

    experiments = []
    return get_spark().createDataFrame(
        experiments,
        StructType(
            [StructField("expId", IntegerType()), StructField("expName", StringType())]
        ),
    )


def load_metrics() -> DataFrame:
    """
    TODO(Part 1.3): Complete this method
    """
    metrics = [(0,'Loss'),(1,'Accuracy')]
    return get_spark().createDataFrame(
        metrics,
        StructType(
            [
                StructField("metricId", IntegerType()),
                StructField("metricName", StringType()),
            ]
        ),
    )


def join_tables(
    logs: DataFrame, experiments: DataFrame, metrics: DataFrame
) -> DataFrame:
    """
    TODO(Part 2): Complete this method
    """
    logs_exps_df = logs.join(experiments,logs.expId==experiments.expId,'inner')

    all_data_df = logs_exps_df.join(metrics,logs_exps_df.metricId == metrics.metricId, 'inner')

    all_data_df.createOrReplaceTempView('alldata')

    spark = get_spark()

    # return spark.sql('select ad.logId, ad.expId, ad.expName, ad.metricId, ad.metricName, ad.valid, ad.createdAt, ad.ingestedAt, ad.step, ad.value from alldata ad')

    return all_data_df.select(col('logId'),col('expId'),col('expName'),col('metricId'),col('metricName')
                                ,col('valid'),col('createdAt'),col('ingestedAt'),col('step'),col('value'))

    joined_tables = []
    return get_spark().createDataFrame(
        joined_tables,
        StructType(
            [
                StructField("logId", StringType()),
                StructField("expId", IntegerType()),
                StructField("expName", StringType()),
                StructField("metricId", IntegerType()),
                StructField("metricName", StringType()),
                StructField("valid", BooleanType()),
                StructField("createdAt", StringType()),
                StructField("ingestedAt", StringType()),
                StructField("step", IntegerType()),
                StructField("value", FloatType()),
            ]
        ),
    )
    [
        "logId",
        "expId",
        "expName",
        "metricId",
        "metricName",
        "valid",
        "createdAt",
        "ingestedAt",
        "step",
        "value",
    ]


def filter_late_logs(data: DataFrame, hours: int) -> DataFrame:
    """
    TODO(Part 3): Complete this method
    """
    filtered_logs = []
    return get_spark().createDataFrame(
        filtered_logs,
        StructType(
            [
                StructField("logId", StringType()),
                StructField("expId", IntegerType()),
                StructField("expName", StringType()),
                StructField("metricId", IntegerType()),
                StructField("metricName", StringType()),
                StructField("valid", BooleanType()),
                StructField("createdAt", StringType()),
                StructField("ingestedAt", StringType()),
                StructField("step", IntegerType()),
                StructField("value", FloatType()),
            ]
        ),
    )


def calculate_experiment_final_scores(data: DataFrame) -> DataFrame:
    """
    TODO(Part 4): Complete this method
    """
    scores = []
    return get_spark().createDataFrame(
        scores,
        StructType(
            [
                StructField("expId", IntegerType()),
                StructField("metricId", IntegerType()),
                StructField("expName", StringType()),
                StructField("metricName", StringType()),
                StructField("maxValue", FloatType()),
                StructField("minValue", FloatType()),
            ]
        ),
    )


def save(data: DataFrame, output_path: Path):
    """
    TODO(Part 5): Complete this method
    """
    pass
