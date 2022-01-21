import time
from pyspark.sql.session import SparkSession


def get_spark_session(appName: str):
    """Initializing spark session"""
    return SparkSession.builder \
        .appName(appName) \
        .config('spark.executor.extraClassPath', '/usr/lib/spark/jars/slf4j-log4j12-1.7.16.jar') \
        .getOrCreate()

