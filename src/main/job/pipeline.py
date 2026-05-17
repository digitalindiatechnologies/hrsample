from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from main.base import PySparkJobInterface


class PySparkJob(PySparkJobInterface):

    def init_spark_session(self) -> SparkSession:
        print("STEP 1: Creating Spark session")
        return (
            SparkSession.builder
            .master("local[*]")
            .appName("customer-scd2-assessment")
            .getOrCreate()
        )

    def print_df(self, step_name: str, df: DataFrame) -> None:
        print("\n" + "=" * 80)
        print(step_name)
        print("=" * 80)
        df.show(truncate=False)

    def apply_customer_scd2(self, customer_activity_df: DataFrame) -> DataFrame:
        print("\nSTEP 2: Starting SCD Type 2 processing")
        self.print_df("Input customer activity dataframe", customer_activity_df)

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

        cleaned = (
            customer_activity_df
            .withColumn("row_type", F.upper(F.trim(F.col("row_type"))))
            .withColumn("op", F.upper(F.trim(F.col("op"))))
            .withColumn("customer_id", F.trim(F.col("customer_id")))
        )
        self.print_df("STEP 3: Clean row_type, op, and customer_id", cleaned)

        dim = (
            cleaned
            .filter(F.col("row_type") == "DIM")
            .withColumn("effective_start_date", F.to_date("effective_start_date"))
            .withColumn("effective_end_date", F.to_date("effective_end_date"))
            .withColumn("is_current", F.col("is_current").cast("boolean"))
            .withColumn("version", F.col("version").cast("int"))
            .select(dim_columns)
        )
        self.print_df("STEP 4: Existing dimension rows where row_type = DIM", dim)

        events = (
            cleaned
            .filter((F.col("row_type") == "EVENT") & (F.col("op") == "UPSERT"))
            .withColumn("event_date", F.to_date("event_ts"))
            .filter(F.col("customer_id").isNotNull() & (F.col("customer_id") != ""))
            .select(
                "customer_id",
                "full_name",
                "email",
                "city",
                "loyalty_tier",
                "event_date",
            )
            .alias("evt")
        )
        self.print_df("STEP 5: Incoming UPSERT event rows with event_date", events)

        current = dim.filter(F.col("is_current") == F.lit(True)).alias("cur")
        historical = dim.filter(F.col("is_current") == F.lit(False)).select(dim_columns)
        self.print_df("STEP 6: Current dimension rows before merge", current)
        self.print_df("STEP 7: Historical dimension rows before merge", historical)

        joined = events.join(current, on="customer_id", how="left")
        self.print_df("STEP 8: Join events to current dimension rows", joined)

        current_exists = F.col("cur.version").isNotNull()
        same_attributes = None
        for column_name in tracked_columns:
            comparison = F.col("evt." + column_name).eqNullSafe(F.col("cur." + column_name))
            same_attributes = comparison if same_attributes is None else same_attributes & comparison

        upserts_to_insert = joined.filter(~current_exists | ~same_attributes)
        changed_existing = upserts_to_insert.filter(current_exists)
        self.print_df("STEP 9: Events that need a new current row", upserts_to_insert)
        self.print_df("STEP 10: Existing customers whose current row must be closed", changed_existing)

        closed_current = changed_existing.select(
            F.col("customer_id"),
            F.col("cur.full_name").alias("full_name"),
            F.col("cur.email").alias("email"),
            F.col("cur.city").alias("city"),
            F.col("cur.loyalty_tier").alias("loyalty_tier"),
            F.col("cur.effective_start_date").alias("effective_start_date"),
            F.date_sub(F.col("evt.event_date"), 1).alias("effective_end_date"),
            F.lit(False).alias("is_current"),
            F.col("cur.version").alias("version"),
        )
        self.print_df("STEP 11: Closed old current rows", closed_current)

        unchanged_current = (
            current
            .join(closed_current.select("customer_id"), on="customer_id", how="left_anti")
            .select(dim_columns)
        )
        self.print_df("STEP 12: Current rows that remain unchanged", unchanged_current)

        inserted_current = upserts_to_insert.select(
            F.col("customer_id"),
            F.col("evt.full_name").alias("full_name"),
            F.col("evt.email").alias("email"),
            F.col("evt.city").alias("city"),
            F.col("evt.loyalty_tier").alias("loyalty_tier"),
            F.col("evt.event_date").alias("effective_start_date"),
            F.to_date(F.lit("9999-12-31")).alias("effective_end_date"),
            F.lit(True).alias("is_current"),
            (F.coalesce(F.col("cur.version"), F.lit(0)) + F.lit(1)).cast("int").alias("version"),
        )
        self.print_df("STEP 13: Inserted new current rows", inserted_current)

        final_scd2 = (
            historical
            .unionByName(unchanged_current)
            .unionByName(closed_current)
            .unionByName(inserted_current)
            .select(dim_columns)
        )
        self.print_df("STEP 14: Final SCD Type 2 dimension", final_scd2)

        return final_scd2

    def current_customer_snapshot(self, scd2_df: DataFrame) -> DataFrame:
        print("\nSTEP 15: Building current customer snapshot")
        snapshot = (
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
        self.print_df("STEP 16: Final current customer snapshot", snapshot)

        return snapshot
