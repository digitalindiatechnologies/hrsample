import pytest
from pyspark.sql import SparkSession
from main.job.pipeline import PySparkJob

job = PySparkJob()

data_file1_schema = ["id","order_no"]
data_file1_sample = [
    ("a101", "2000"),
    ("a103", "4000"),
    ("a103", "4500"),
    ("a105", "9500"),
    ("a106", "1500"),
    ("a101", "500"),
    ("a103", "2500"),
    ("a102", "4700"),
]

data_file2_schema = ["name", "age"]
data_file2_sample = [
    ("u", 10),
    ("v", 60),
    ("w", 20),
    ("x", 19),
    ("y", 18),
    ("z", 17),
]

data_file2_sample2 = [
    ("aa", 11),
    ("bb", 6),
    ("cc", 20),
    ("dd", 17),
    ("ee", 18),
    ("ff", 14),
]

def create_sample(sample, data_schema):
    return job.spark.createDataFrame(data=sample, schema=data_schema)


@pytest.mark.filterwarnings("ignore")
def test_init_spark_session():
    assert isinstance(job.spark, SparkSession), "-- spark session not implemented"

@pytest.mark.filterwarnings("ignore")
def test_distinct_ids():
    data_file1_df = create_sample(data_file1_sample, data_file1_schema)
    nb_distinct_ids = job.distinct_ids(data_file1_df)

    assert (5 == nb_distinct_ids)

@pytest.mark.filterwarnings("ignore")
def test_valid_age_count():
    data_file2_df = create_sample(data_file2_sample, data_file2_schema)
    nb_valid_age = job.valid_age_count(data_file2_df)

    assert (4 == nb_valid_age)

@pytest.mark.filterwarnings("ignore")
def test_valid_age_count2():
    data_file2_df = create_sample(data_file2_sample2, data_file2_schema)
    nb_valid_age = job.valid_age_count(data_file2_df)

    assert (2 == nb_valid_age)
