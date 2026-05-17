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
    def distinct_ids(self, data_file1: DataFrame) -> int:
        raise NotImplementedError

    @abc.abstractmethod
    def valid_age_count(self, data_file2: DataFrame) -> int:
        raise NotImplementedError

    def stop(self) -> None:
        self.spark.stop()
