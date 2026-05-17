# Hard PySpark Assessment: Customer SCD Type 2 Merge

## Environment
- Spark Version: 3.x
- Python Version: 3.8+

## Read-Only Files
- `src/app.py`
- `src/tests/test_pipeline.py`
- `src/main/__init__.py`
- `src/main/base/__init__.py`
- `src/main/job/__init__.py`
- `hackerrank.yml`
- `requirements.txt`
- `data/*`

Candidates should implement only:

- `src/main/job/pipeline.py`

## Problem

You are building the customer dimension loader for an analytics warehouse. The warehouse stores customer records as a Slowly Changing Dimension Type 2 table.

Two CSV files are provided:

### `data_file1.csv`: Existing customer dimension

Columns:

- `customer_id`
- `full_name`
- `email`
- `city`
- `loyalty_tier`
- `effective_start_date`
- `effective_end_date`
- `is_current`
- `version`

`effective_end_date = 9999-12-31` means the row is currently active.

### `data_file2.csv`: Incoming customer change events

Columns:

- `customer_id`
- `full_name`
- `email`
- `city`
- `loyalty_tier`
- `event_ts`
- `op`
- `ingestion_id`

`op` can be:

- `UPSERT`: insert a new customer or apply a changed customer state
- `DELETE`: close the current customer record without inserting a replacement row

## Candidate Tasks

Implement the following methods in `src/main/job/pipeline.py`.

### 1. `init_spark_session(self)`

Create and return a local Spark session.

### 2. `latest_customer_changes(self, customer_events_df)`

Return the latest valid event per `customer_id`.

Rules:

- Ignore rows where `customer_id` is null or blank.
- Ignore rows whose `op` is not `UPSERT` or `DELETE`.
- If a customer has multiple events in the batch, keep only the event with the greatest `event_ts`.
- If multiple events have the same `event_ts`, keep the one with the greatest numeric `ingestion_id`.
- Add an `event_date` column derived from `event_ts`.

### 3. `apply_customer_scd2(self, existing_dim_df, customer_events_df)`

Apply the compacted changes from `latest_customer_changes` to the existing dimension.

Rules:

- Preserve all existing historical rows where `is_current = false`.
- For a new `UPSERT` customer, insert version `1` with:
  - `effective_start_date = event_date`
  - `effective_end_date = 9999-12-31`
  - `is_current = true`
- For an existing customer where any tracked attribute changed, close the old current row and insert a new current row.
- Tracked attributes are `full_name`, `email`, `city`, and `loyalty_tier`.
- A closed row must have:
  - `effective_end_date = event_date - 1 day`
  - `is_current = false`
  - original `version`
- A replacement row must have:
  - `version = previous current version + 1`
  - `effective_start_date = event_date`
  - `effective_end_date = 9999-12-31`
  - `is_current = true`
- If an `UPSERT` event has the same tracked attributes as the current row, treat it as a no-op.
- For a `DELETE` event on an existing current customer, close the current row and do not insert a replacement row.
- A `DELETE` event for a customer without a current row should not create any output row.

### 4. `current_customer_snapshot(self, scd2_df)`

Return only current customer rows with the columns:

- `customer_id`
- `full_name`
- `email`
- `city`
- `loyalty_tier`
- `version`

## Expected Output

The application prints:

1. The final SCD2 dimension after applying the batch.
2. The current customer snapshot.

The unit tests validate correctness using Spark DataFrame comparisons.

## Commands

Run:

```bash
python3 src/app.py data/data_file1.csv data/data_file2.csv
```

Install:

```bash
pip3 install -r requirements.txt
```

Test:

```bash
py.test -p no:warnings
```
