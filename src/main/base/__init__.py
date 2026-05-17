import abc
from pyspark.sql import SparkSession, DataFrame


class PySparkJobInterface(abc.ABC):

    def __init__(self):
        self.spark = self.init_spark_session()

    @abc.abstractmethod
    def init_spark_session(self) -> SparkSession:
        """Create spark session"""
        raise NotImplementedError

    def read_csv(self, input_path: str) -> DataFrame:
        return self.spark.read.options(header=True, inferSchema=True).csv(input_path)

    @abc.abstractmethod
    def latest_customer_changes(self, customer_events_df: DataFrame) -> DataFrame:
        raise NotImplementedError

    @abc.abstractmethod
    def apply_customer_scd2(self, existing_dim_df: DataFrame, customer_events_df: DataFrame) -> DataFrame:
        raise NotImplementedError

    @abc.abstractmethod
    def current_customer_snapshot(self, scd2_df: DataFrame) -> DataFrame:
        raise NotImplementedError

    def stop(self) -> None:
        self.spark.stop()
