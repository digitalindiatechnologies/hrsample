import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from main.job.pipeline import PySparkJob

job = PySparkJob()

activity_schema = [
    "row_type",
    "customer_id",
    "full_name",
    "email",
    "city",
    "loyalty_tier",
    "effective_start_date",
    "effective_end_date",
    "is_current",
    "version",
    "event_ts",
    "op",
]

activity_sample = [
    ("DIM", "C001", "Alice Rao", "alice@example.com", "Mumbai", "Silver", "2024-01-01", "9999-12-31", True, 1, None, None),
    ("DIM", "C002", "Bob Sen", "bob@example.com", "Delhi", "Gold", "2024-02-01", "9999-12-31", True, 2, None, None),
    ("EVENT", "C001", "Alice Rao", "alice@example.com", "Pune", "Silver", None, None, None, None, "2024-07-01 09:00:00", "UPSERT"),
    ("EVENT", "C002", "Bob Sen", "bob@example.com", "Delhi", "Gold", None, None, None, None, "2024-07-02 10:00:00", "UPSERT"),
    ("EVENT", "C003", "Farah Khan", "farah@example.com", "Kochi", "Bronze", None, None, None, None, "2024-07-03 11:00:00", "UPSERT"),
]

dim_columns = [
    "customer_id",
    "full_name",
    "email",
    "city",
    "loyalty_tier",
    "effective_start_date",
    "effective_end_date",
    "is_current",
    "version",
]


def create_sample(sample, data_schema):
    return job.spark.createDataFrame(data=sample, schema=data_schema)


def collect_scd_rows(df):
    formatted = (
        df
        .withColumn("effective_start_date", F.date_format("effective_start_date", "yyyy-MM-dd"))
        .withColumn("effective_end_date", F.date_format("effective_end_date", "yyyy-MM-dd"))
    )
    return [
        tuple(row)
        for row in formatted.select(dim_columns).orderBy("customer_id", "version").collect()
    ]


@pytest.mark.filterwarnings("ignore")
def test_init_spark_session():
    assert isinstance(job.spark, SparkSession), "-- spark session not implemented"


@pytest.mark.filterwarnings("ignore")
def test_apply_customer_scd2_with_single_activity_dataset():
    activity_df = create_sample(activity_sample, activity_schema)

    actual = collect_scd_rows(job.apply_customer_scd2(activity_df))

    expected = [
        ("C001", "Alice Rao", "alice@example.com", "Mumbai", "Silver", "2024-01-01", "2024-06-30", False, 1),
        ("C001", "Alice Rao", "alice@example.com", "Pune", "Silver", "2024-07-01", "9999-12-31", True, 2),
        ("C002", "Bob Sen", "bob@example.com", "Delhi", "Gold", "2024-02-01", "9999-12-31", True, 2),
        ("C003", "Farah Khan", "farah@example.com", "Kochi", "Bronze", "2024-07-03", "9999-12-31", True, 1),
    ]

    assert actual == expected


@pytest.mark.filterwarnings("ignore")
def test_current_customer_snapshot():
    activity_df = create_sample(activity_sample, activity_schema)
    scd2_df = job.apply_customer_scd2(activity_df)

    actual = [
        tuple(row)
        for row in job.current_customer_snapshot(scd2_df).orderBy("customer_id").collect()
    ]

    expected = [
        ("C001", "Alice Rao", "alice@example.com", "Pune", "Silver", 2),
        ("C002", "Bob Sen", "bob@example.com", "Delhi", "Gold", 2),
        ("C003", "Farah Khan", "farah@example.com", "Kochi", "Bronze", 1),
    ]

    assert actual == expected
