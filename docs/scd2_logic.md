# 🔄 SCD Type-2 Implementation (Lakehouse)

## Objective

Maintain historical changes for customer dimension.

---

## Business Key

customer_id

---

## Change Detection Columns

- first_name
- last_name
- email
- city
- state

---

## SCD2 Rules

If incoming record differs from current record:

1. Expire old record:
   - Set effective_end_date = current_date
   - Set is_current = False

2. Insert new version:
   - effective_start_date = current_date
   - effective_end_date = NULL
   - is_current = True

---

## Example

Before change:

| customer_id | city    | start_date | end_date | is_current |
|-------------|---------|------------|----------|------------|
| C001        | Mumbai  | 2026-02-20 | NULL     | TRUE       |

After change:

| customer_id | city   | start_date | end_date   | is_current |
|-------------|--------|------------|------------|------------|
| C001        | Mumbai | 2026-02-20 | 2026-02-21 | FALSE      |
| C001        | Pune   | 2026-02-21 | NULL       | TRUE       |

---

## Implementation Approach

- Read Silver Parquet
- Read existing Gold Parquet
- Full outer join on customer_id
- Detect changes
- Expire old records
- Insert new records
- Overwrite Gold Parquet

---

## Limitations

- Full overwrite model
- No ACID transactions
- Not concurrent-safe

Future upgrade:
- Delta Lake
- Apache Iceberg
- Partitioned SCD2
