# Reference Solution Notes

The reference implementation is in `src/main/job/pipeline.py`.

The assessment now uses a single input dataset with five records:

- two existing dimension rows
- one changed customer event
- one no-op customer event
- one new customer event

Core approach:

1. Split the dataframe into `DIM` rows and `EVENT` rows.
2. Cast effective dates, `is_current`, and `version` on the dimension rows.
3. Keep only `UPSERT` event rows and derive `event_date` from `event_ts`.
4. Join events to the current dimension rows by `customer_id`.
5. Compare tracked attributes with null-safe equality.
6. Close only existing current rows whose tracked attributes changed.
7. Insert one new current row for changed existing customers and new customers.
8. Keep no-op customers unchanged.
9. Return a current snapshot by filtering `is_current = true`.
