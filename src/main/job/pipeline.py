from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from main.base import PySparkJobInterface


class PySparkJob(PySparkJobInterface):

    def init_spark_session(self) -> SparkSession:
        return (
            SparkSession.builder
            .master("local[*]")
            .appName("customer-scd2-assessment")
            .getOrCreate()
        )

    def latest_customer_changes(self, customer_events_df: DataFrame) -> DataFrame:
        events = (
            customer_events_df
            .withColumn("customer_id", F.trim(F.col("customer_id")))
            .withColumn("op", F.upper(F.trim(F.col("op"))))
            .withColumn("event_timestamp", F.to_timestamp("event_ts"))
            .withColumn("event_date", F.to_date("event_ts"))
            .withColumn("ingestion_id_long", F.col("ingestion_id").cast("long"))
            .filter(F.col("customer_id").isNotNull() & (F.col("customer_id") != ""))
            .filter(F.col("op").isin("UPSERT", "DELETE"))
            .filter(F.col("event_timestamp").isNotNull())
        )

        latest_window = Window.partitionBy("customer_id").orderBy(
            F.col("event_timestamp").desc(),
            F.col("ingestion_id_long").desc_nulls_last(),
        )

        return (
            events
            .withColumn("rn", F.row_number().over(latest_window))
            .filter(F.col("rn") == 1)
            .drop("rn", "event_timestamp", "ingestion_id_long")
        )

    def apply_customer_scd2(self, existing_dim_df: DataFrame, customer_events_df: DataFrame) -> DataFrame:
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

        tracked_columns = ["full_name", "email", "city", "loyalty_tier"]

        dim = (
            existing_dim_df
            .withColumn("effective_start_date", F.to_date("effective_start_date"))
            .withColumn("effective_end_date", F.to_date("effective_end_date"))
            .withColumn("is_current", F.col("is_current").cast("boolean"))
            .withColumn("version", F.col("version").cast("int"))
            .select(dim_columns)
        )

        changes = self.latest_customer_changes(customer_events_df).alias("chg")
        current = dim.filter(F.col("is_current") == F.lit(True)).alias("cur")
        historical = dim.filter(F.col("is_current") == F.lit(False)).select(dim_columns)

        joined = changes.join(current, on="customer_id", how="left")

        current_exists = F.col("cur.version").isNotNull()
        same_attributes = None
        for column_name in tracked_columns:
            comparison = F.col("chg." + column_name).eqNullSafe(F.col("cur." + column_name))
            same_attributes = comparison if same_attributes is None else same_attributes & comparison

        upserts_to_insert = joined.filter(
            (F.col("chg.op") == "UPSERT")
            & (~current_exists | ~same_attributes)
        )

        upserts_to_close = upserts_to_insert.filter(current_exists)

        deletes_to_close = joined.filter(
            (F.col("chg.op") == "DELETE")
            & current_exists
        )

        close_projection = [
            F.col("customer_id"),
            F.col("cur.full_name").alias("full_name"),
            F.col("cur.email").alias("email"),
            F.col("cur.city").alias("city"),
            F.col("cur.loyalty_tier").alias("loyalty_tier"),
            F.col("cur.effective_start_date").alias("effective_start_date"),
            F.date_sub(F.col("chg.event_date"), 1).alias("effective_end_date"),
            F.lit(False).alias("is_current"),
            F.col("cur.version").alias("version"),
        ]

        rows_to_close = (
            upserts_to_close
            .select(*close_projection)
            .unionByName(deletes_to_close.select(*close_projection))
        )

        close_keys = rows_to_close.select("customer_id").distinct()

        unchanged_current = (
            current
            .join(close_keys, on="customer_id", how="left_anti")
            .select(dim_columns)
        )

        closed_current = rows_to_close.select(dim_columns)

        inserted_current = upserts_to_insert.select(
            F.col("customer_id"),
            F.col("chg.full_name").alias("full_name"),
            F.col("chg.email").alias("email"),
            F.col("chg.city").alias("city"),
            F.col("chg.loyalty_tier").alias("loyalty_tier"),
            F.col("chg.event_date").alias("effective_start_date"),
            F.to_date(F.lit("9999-12-31")).alias("effective_end_date"),
            F.lit(True).alias("is_current"),
            (F.coalesce(F.col("cur.version"), F.lit(0)) + F.lit(1)).cast("int").alias("version"),
        )

        return (
            historical
            .unionByName(unchanged_current)
            .unionByName(closed_current)
            .unionByName(inserted_current)
            .select(dim_columns)
        )

    def current_customer_snapshot(self, scd2_df: DataFrame) -> DataFrame:
        return (
            scd2_df
            .filter(F.col("is_current") == F.lit(True))
            .select(
                "customer_id",
                "full_name",
                "email",
                "city",
                "loyalty_tier",
                "version",
            )
        )
