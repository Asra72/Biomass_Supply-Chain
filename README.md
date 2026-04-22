# Supply chain data — SQL queries and report verification

Part of a larger piece of work analysing a biomass supply chain dataset.
This project focuses specifically on verifying a summary report against
the underlying data using SQL.

The summary report had been compiled separately from the database.
The goal was to check whether every figure in it matched what the
data actually showed.

---

## Dataset

Three tables:

- **shipments** — 400 rows. one row per shipment with supplier, product,
  volume in tonnes, date, and status (delivered / delayed / cancelled)
- **suppliers** — 8 suppliers, categorised as Agricultural, Forestry, or Processing
- **products** — 6 biomass product types, split into Raw and Processed,
  each with a price per tonne

---

## What I did

Loaded the three tables into SQLite and wrote queries covering:

- shipment counts by status
- total delivered volume and cost
- volume and cost broken down by product type and supplier type
- monthly delivery volumes across 2023
- top suppliers by volume
- cancellation rates by supplier type
- average shipment size per product

Then took the summary report and checked every figure in it against
the query outputs.

---

## What the verification found

Two mismatches:

**July 2023 volume**
Report says 310.0 tonnes. Database gives 281.5 tonnes.
Difference of 28.5 tonnes — about 10% higher than actual.

**Supplier count**
Report says 9 active suppliers. Database has 8.
One supplier in the report does not exist in the supplier table.

Everything else matched: shipment counts, total cost, product type breakdown,
supplier type volumes, all other monthly figures.

---

## Files

```
data/shipments.csv              raw shipments data
data/suppliers.csv              supplier list
data/products.csv               product list with prices
data/summary_report.txt         the report being checked
data/supply_chain.db            sqlite database (created by queries.py)
scripts/generate_data.py        creates the three csv files
scripts/queries.py              loads db and runs all queries
scripts/verify_report.py        checks report against db line by line
outputs/verification_notes.txt  full output of the verification run
```

---

## How to run

```bash
pip install pandas

python scripts/generate_data.py
python scripts/queries.py
python scripts/verify_report.py
```

---

## Tools

Python, pandas, SQLite
