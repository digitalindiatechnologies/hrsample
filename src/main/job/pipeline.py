from pyspark.sql import SparkSession, DataFrame
from main.base import PySparkJobInterface


class PySparkJob(PySparkJobInterface):

    def init_spark_session(self) -> SparkSession:
        # TODO: Put your code here
        ...

    def distinct_ids(self, data_file1: DataFrame) -> int:
        # TODO: Put your code here
        ...

    def valid_age_count(self, data_file2: DataFrame) -> int:
        # TODO: Put your code here
        ...
