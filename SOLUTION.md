# Reference Solution Notes

The reference implementation is in `src/main/job/pipeline.py`.

Core approach:

1. Build a local Spark session.
2. Clean and compact incoming events with a `row_number` window:
   - partition by `customer_id`
   - order by `event_ts` descending, then numeric `ingestion_id` descending
3. Split the dimension into historical and current rows.
4. Join compacted changes to current rows.
5. Classify each event as:
   - no-op `UPSERT`
   - changed `UPSERT`
   - new-customer `UPSERT`
   - matched `DELETE`
   - unmatched `DELETE`
6. Close only current records affected by changed upserts or matched deletes.
7. Insert replacement rows only for new or changed upserts.
8. Union historical, unchanged current, closed current, and inserted current rows.

Important implementation details:

- Attribute comparison uses Spark null-safe equality with `eqNullSafe`.
- Dates are cast with `to_date`.
- The open-ended SCD date is represented as `9999-12-31`.
- `current_customer_snapshot` filters to `is_current = true` and emits only business-facing columns.
