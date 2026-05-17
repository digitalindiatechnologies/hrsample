import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from main.job.pipeline import PySparkJob

job = PySparkJob()

dim_schema = [
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

dim_sample = [
    ("C001", "Alice Rao", "alice@example.com", "Mumbai", "Silver", "2024-01-01", "9999-12-31", True, 1),
    ("C002", "Bob Sen", "bob.old@example.com", "Delhi", "Silver", "2023-05-01", "2024-01-31", False, 1),
    ("C002", "Bob Sen", "bob@example.com", "Delhi", "Gold", "2024-02-01", "9999-12-31", True, 2),
    ("C003", "Cara Iyer", "cara@example.com", "Bengaluru", "Bronze", "2024-03-01", "9999-12-31", True, 1),
    ("C004", "David Kim", "david@example.com", "Chennai", "Gold", "2024-01-15", "2024-04-30", False, 2),
    ("C004", "David Kim", "david@example.com", "Chennai", "Gold", "2024-05-01", "9999-12-31", True, 3),
    ("C005", "Eva Bose", "eva@example.com", "Kolkata", "Silver", "2024-01-01", "2024-05-31", False, 1),
]

events_schema = [
    "customer_id",
    "full_name",
    "email",
    "city",
    "loyalty_tier",
    "event_ts",
    "op",
    "ingestion_id",
]

events_sample = [
    ("C001", "Alice Rao", "alice@example.com", "Hyderabad", "Silver", "2024-06-15 08:30:00", "UPSERT", 10),
    ("C001", "Alice Rao", "alice@example.com", "Pune", "Silver", "2024-07-01 09:00:00", "UPSERT", 11),
    ("C002", "Bob Sen", "bob@example.com", "Delhi", "Gold", "2024-07-02 10:00:00", "UPSERT", 12),
    ("C003", "Cara Iyer", "cara@example.com", "Bengaluru", "Bronze", "2024-07-03 18:00:00", "DELETE", 13),
    ("C004", "David Kim", "david@example.com", "Jaipur", "Gold", "2024-07-05 09:00:00", "UPSERT", 14),
    ("C004", "David Kim", "david@example.com", "Jaipur", "Platinum", "2024-07-05 09:00:00", "UPSERT", 15),
    ("C006", "Fatima Ali", "fatima@example.com", "Kochi", "Bronze", "2024-07-04 12:00:00", "UPSERT", 16),
    ("C007", "Gita Shah", "gita@example.com", "Surat", "Gold", "2024-07-04 12:00:00", "MERGE", 17),
    ("", "Missing Customer", "missing@example.com", "Noida", "Silver", "2024-07-04 12:00:00", "UPSERT", 18),
]


def create_sample(sample, data_schema):
    return job.spark.createDataFrame(data=sample, schema=data_schema)


def collect_latest(df):
    return {
        row.customer_id: (
            row.full_name,
            row.email,
            row.city,
            row.loyalty_tier,
            row.event_ts,
            row.op,
            row.ingestion_id,
            row.event_date,
        )
        for row in df.orderBy("customer_id").collect()
    }


def collect_scd_rows(df):
    date_cols = ["effective_start_date", "effective_end_date"]
    formatted = df
    for column_name in date_cols:
        formatted = formatted.withColumn(column_name, F.date_format(column_name, "yyyy-MM-dd"))

    return [
        tuple(row)
        for row in formatted.select(dim_schema).orderBy("customer_id", "version", "effective_start_date").collect()
    ]


@pytest.mark.filterwarnings("ignore")
def test_init_spark_session():
    assert isinstance(job.spark, SparkSession), "-- spark session not implemented"


@pytest.mark.filterwarnings("ignore")
def test_latest_customer_changes_compacts_batch():
    events_df = create_sample(events_sample, events_schema)

    latest = collect_latest(job.latest_customer_changes(events_df))

    assert set(latest.keys()) == {"C001", "C002", "C003", "C004", "C006"}
    assert latest["C001"][2] == "Pune"
    assert latest["C004"][3] == "Platinum"
    assert latest["C004"][6] == 15
    assert str(latest["C003"][7]) == "2024-07-03"


@pytest.mark.filterwarnings("ignore")
def test_apply_customer_scd2_preserves_history_and_applies_changes():
    dim_df = create_sample(dim_sample, dim_schema)
    events_df = create_sample(events_sample, events_schema)

    actual = collect_scd_rows(job.apply_customer_scd2(dim_df, events_df))

    expected = [
        ("C001", "Alice Rao", "alice@example.com", "Mumbai", "Silver", "2024-01-01", "2024-06-30", False, 1),
        ("C001", "Alice Rao", "alice@example.com", "Pune", "Silver", "2024-07-01", "9999-12-31", True, 2),
        ("C002", "Bob Sen", "bob.old@example.com", "Delhi", "Silver", "2023-05-01", "2024-01-31", False, 1),
        ("C002", "Bob Sen", "bob@example.com", "Delhi", "Gold", "2024-02-01", "9999-12-31", True, 2),
        ("C003", "Cara Iyer", "cara@example.com", "Bengaluru", "Bronze", "2024-03-01", "2024-07-02", False, 1),
        ("C004", "David Kim", "david@example.com", "Chennai", "Gold", "2024-01-15", "2024-04-30", False, 2),
        ("C004", "David Kim", "david@example.com", "Chennai", "Gold", "2024-05-01", "2024-07-04", False, 3),
        ("C004", "David Kim", "david@example.com", "Jaipur", "Platinum", "2024-07-05", "9999-12-31", True, 4),
        ("C005", "Eva Bose", "eva@example.com", "Kolkata", "Silver", "2024-01-01", "2024-05-31", False, 1),
        ("C006", "Fatima Ali", "fatima@example.com", "Kochi", "Bronze", "2024-07-04", "9999-12-31", True, 1),
    ]

    assert actual == expected


@pytest.mark.filterwarnings("ignore")
def test_current_customer_snapshot_after_scd2_merge():
    dim_df = create_sample(dim_sample, dim_schema)
    events_df = create_sample(events_sample, events_schema)
    scd2_df = job.apply_customer_scd2(dim_df, events_df)

    actual = [
        tuple(row)
        for row in job.current_customer_snapshot(scd2_df).orderBy("customer_id").collect()
    ]

    expected = [
        ("C001", "Alice Rao", "alice@example.com", "Pune", "Silver", 2),
        ("C002", "Bob Sen", "bob@example.com", "Delhi", "Gold", 2),
        ("C004", "David Kim", "david@example.com", "Jaipur", "Platinum", 4),
        ("C006", "Fatima Ali", "fatima@example.com", "Kochi", "Bronze", 1),
    ]

    assert actual == expected
