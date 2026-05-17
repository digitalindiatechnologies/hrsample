import sys
from main.job.pipeline import PySparkJob


def main():
    job = PySparkJob()

    print("<<Reading CSV>>")
    existing_dim_df = job.read_csv(sys.argv[1])
    customer_events_df = job.read_csv(sys.argv[2])

    print("<<Final Customer SCD2 Dimension>>")
    scd2_df = job.apply_customer_scd2(existing_dim_df, customer_events_df)
    scd2_df.orderBy("customer_id", "version").show(truncate=False)

    print("<<Current Customer Snapshot>>")
    snapshot_df = job.current_customer_snapshot(scd2_df)
    snapshot_df.orderBy("customer_id").show(truncate=False)

    job.stop()


if __name__ == '__main__':
    main()
