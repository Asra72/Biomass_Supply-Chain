import sqlite3
import pandas as pd

# checks every figure in summary_report.txt against the database
# anything that doesn't match gets flagged

db = sqlite3.connect("data/supply_chain.db")

notes = []

def log(msg):
    print(msg)
    notes.append(msg)

def check(label, reported, actual, tolerance=0.1):
    diff = round(float(actual) - float(reported), 2)
    if abs(diff) > tolerance:
        log(f"MISMATCH  {label}")
        log(f"  report:  {reported}")
        log(f"  actual:  {actual}")
        log(f"  diff:    {diff}")
        log("")
    else:
        log(f"ok  {label}")

log("checking summary_report.txt against supply_chain.db")
log("")


# shipment counts
r = pd.read_sql_query("SELECT status, COUNT(*) as n FROM shipments GROUP BY status", db)
counts = dict(zip(r["status"], r["n"]))
check("delivered shipments",  238, counts.get("delivered", 0))
check("delayed shipments",     86, counts.get("delayed", 0))
check("cancelled shipments",   76, counts.get("cancelled", 0))
check("total shipments",      400, sum(counts.values()))


# total volume and cost
r = pd.read_sql_query("""
    SELECT ROUND(SUM(s.tonnes),1) as t, ROUND(SUM(s.tonnes * p.price_per_tonne),2) as cost
    FROM shipments s JOIN products p ON s.product_id = p.product_id
    WHERE s.status = 'delivered'
""", db)
check("total delivered tonnes", 2634.4, r["t"].iloc[0])
check("total delivered cost",   161740.50, r["cost"].iloc[0])


# by product type
r = pd.read_sql_query("""
    SELECT p.type, ROUND(SUM(s.tonnes),1) as t, ROUND(SUM(s.tonnes * p.price_per_tonne),2) as cost
    FROM shipments s JOIN products p ON s.product_id = p.product_id
    WHERE s.status = 'delivered'
    GROUP BY p.type
""", db)
pt = dict(zip(r["type"], zip(r["t"], r["cost"])))
check("processed tonnes",       1040.7, pt["Processed"][0])
check("processed cost",         104248.80, pt["Processed"][1])
check("raw tonnes",             1593.7, pt["Raw"][0])
check("raw cost",               57491.70, pt["Raw"][1])


# by supplier type
r = pd.read_sql_query("""
    SELECT sup.type, COUNT(*) as n, ROUND(SUM(s.tonnes),1) as t
    FROM shipments s JOIN suppliers sup ON s.supplier_id = sup.supplier_id
    WHERE s.status = 'delivered'
    GROUP BY sup.type
""", db)
st = {row["type"]: (row["n"], row["t"]) for _, row in r.iterrows()}
check("agricultural shipments",  121, st["Agricultural"][0])
check("agricultural tonnes",    1350.2, st["Agricultural"][1])
check("forestry shipments",       62, st["Forestry"][0])
check("forestry tonnes",         655.6, st["Forestry"][1])
check("processing shipments",     55, st["Processing"][0])
check("processing tonnes",       628.6, st["Processing"][1])


# monthly volume
r = pd.read_sql_query("""
    SELECT strftime('%Y-%m', ship_date) as month, ROUND(SUM(tonnes),1) as t
    FROM shipments WHERE status = 'delivered'
    GROUP BY month ORDER BY month
""", db)
monthly = dict(zip(r["month"], r["t"]))

report_monthly = {
    "2023-01": 243.5, "2023-02": 169.2, "2023-03": 234.0,
    "2023-04": 164.1, "2023-05": 180.6, "2023-06": 190.8,
    "2023-07": 310.0, "2023-08": 189.4, "2023-09": 223.9,
    "2023-10": 271.6, "2023-11": 212.2, "2023-12": 273.6,
}
for month, reported in report_monthly.items():
    check(f"monthly volume {month}", reported, monthly.get(month, 0))


# active supplier count
r = pd.read_sql_query("SELECT COUNT(DISTINCT supplier_id) as n FROM suppliers", db)
check("number of active suppliers", 9, r["n"].iloc[0])


# summary
log("")
log("--- done ---")
mismatches = [l for l in notes if l.startswith("MISMATCH")]
log(f"mismatches found: {len(mismatches)}")

db.close()

with open("outputs/verification_notes.txt", "w") as f:
    f.write("\n".join(notes))
print()
print("saved: outputs/verification_notes.txt")
