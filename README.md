# PySpark Assessment: Customer SCD Type 2 Merge

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

A retail loyalty team maintains a customer dimension table using Slowly Changing Dimension Type 2 logic.

You are given one CSV file: `data/data_file1.csv`.

The same file contains two kinds of rows:

- `row_type = DIM`: the current customer dimension rows already present in the warehouse
- `row_type = EVENT`: incoming customer update events for the daily batch

## Dataset

Columns:

- `row_type`
- `customer_id`
- `full_name`
- `email`
- `city`
- `loyalty_tier`
- `effective_start_date`
- `effective_end_date`
- `is_current`
- `version`
- `event_ts`
- `op`

The sample data has five records:

| row_type | customer_id | scenario |
| --- | --- | --- |
| DIM | C001 | Existing current customer in Mumbai |
| DIM | C002 | Existing current customer in Delhi |
| EVENT | C001 | City changed from Mumbai to Pune |
| EVENT | C002 | No attribute change |
| EVENT | C003 | New customer |

Input data:

```text
+--------+-----------+----------+-----------------+------+------------+--------------------+------------------+----------+-------+-------------------+------+
|row_type|customer_id|full_name |email            |city  |loyalty_tier|effective_start_date|effective_end_date|is_current|version|event_ts           |op    |
+--------+-----------+----------+-----------------+------+------------+--------------------+------------------+----------+-------+-------------------+------+
|DIM     |C001       |Alice Rao |alice@example.com|Mumbai|Silver      |2024-01-01          |9999-12-31        |true      |1      |null               |null  |
|DIM     |C002       |Bob Sen   |bob@example.com  |Delhi |Gold        |2024-02-01          |9999-12-31        |true      |2      |null               |null  |
|EVENT   |C001       |Alice Rao |alice@example.com|Pune  |Silver      |null                |null              |null      |null   |2024-07-01 09:00:00|UPSERT|
|EVENT   |C002       |Bob Sen   |bob@example.com  |Delhi |Gold        |null                |null              |null      |null   |2024-07-02 10:00:00|UPSERT|
|EVENT   |C003       |Farah Khan|farah@example.com|Kochi |Bronze      |null                |null              |null      |null   |2024-07-03 11:00:00|UPSERT|
+--------+-----------+----------+-----------------+------+------------+--------------------+------------------+----------+-------+-------------------+------+
```

## Candidate Tasks

Implement the following methods in `src/main/job/pipeline.py`.

### 1. `init_spark_session(self)`

Create and return a local Spark session.

### 2. `apply_customer_scd2(self, customer_activity_df)`

Build the final SCD Type 2 customer dimension from the single input dataframe.

Rules:

- Split the input into existing dimension rows where `row_type = DIM` and event rows where `row_type = EVENT`.
- Process only `UPSERT` events.
- Tracked attributes are `full_name`, `email`, `city`, and `loyalty_tier`.
- If an existing customer receives an `UPSERT` with any changed tracked attribute:
  - close the old current row with `effective_end_date = event_date - 1 day`
  - set the old row `is_current = false`
  - insert a new current row with `version = old version + 1`
- If an existing customer receives an `UPSERT` with no tracked attribute changes, keep the current row unchanged.
- If a new customer receives an `UPSERT`, insert version `1`.
- New current rows must have:
  - `effective_start_date = event_date`
  - `effective_end_date = 9999-12-31`
  - `is_current = true`

Return columns in this exact order:

- `customer_id`
- `full_name`
- `email`
- `city`
- `loyalty_tier`
- `effective_start_date`
- `effective_end_date`
- `is_current`
- `version`

### 3. `current_customer_snapshot(self, scd2_df)`

Return only current customer rows with the columns:

- `customer_id`
- `full_name`
- `email`
- `city`
- `loyalty_tier`
- `version`

## Expected Result

After processing the five records:

- `C001` should have two SCD rows: old Mumbai row closed on `2024-06-30`, new Pune row starting `2024-07-01`.
- `C002` should remain unchanged because its event has the same tracked attributes.
- `C003` should be inserted as a new current customer with version `1`.

Expected final SCD2 output:

```text
+-----------+----------+-----------------+------+------------+--------------------+------------------+----------+-------+
|customer_id|full_name |email            |city  |loyalty_tier|effective_start_date|effective_end_date|is_current|version|
+-----------+----------+-----------------+------+------------+--------------------+------------------+----------+-------+
|C001       |Alice Rao |alice@example.com|Mumbai|Silver      |2024-01-01          |2024-06-30        |false     |1      |
|C001       |Alice Rao |alice@example.com|Pune  |Silver      |2024-07-01          |9999-12-31        |true      |2      |
|C002       |Bob Sen   |bob@example.com  |Delhi |Gold        |2024-02-01          |9999-12-31        |true      |2      |
|C003       |Farah Khan|farah@example.com|Kochi |Bronze      |2024-07-03          |9999-12-31        |true      |1      |
+-----------+----------+-----------------+------+------------+--------------------+------------------+----------+-------+
```

## Commands

Run:

```bash
python3 src/app.py data/data_file1.csv
```

Install:

```bash
python3 -m pip install -r requirements.txt
```

Test:

```bash
python3 -m pytest -p no:warnings
```
