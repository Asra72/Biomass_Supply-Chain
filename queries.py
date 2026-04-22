import sqlite3
import pandas as pd

# load csvs into sqlite and run queries

db = sqlite3.connect("data/supply_chain.db")

shipments = pd.read_csv("data/shipments.csv")
suppliers = pd.read_csv("data/suppliers.csv")
products  = pd.read_csv("data/products.csv")

shipments.to_sql("shipments", db, if_exists="replace", index=False)
suppliers.to_sql("suppliers", db, if_exists="replace", index=False)
products.to_sql("products",   db, if_exists="replace", index=False)

print("loaded into supply_chain.db")
print(f"  shipments: {len(shipments)} rows")
print(f"  suppliers: {len(suppliers)} rows")
print(f"  products:  {len(products)} rows")
print()

def run(label, sql):
    print(f"--- {label} ---")
    r = pd.read_sql_query(sql, db)
    print(r.to_string(index=False))
    print()
    return r


# how many shipments per status
run("shipments by status", """
    SELECT status, COUNT(*) as shipments
    FROM shipments
    GROUP BY status
    ORDER BY shipments DESC
""")


# total volume delivered (tonnes) and cost
run("total delivered volume and cost", """
    SELECT
        ROUND(SUM(s.tonnes), 1) as total_tonnes,
        ROUND(SUM(s.tonnes * p.price_per_tonne), 2) as total_cost
    FROM shipments s
    JOIN products p ON s.product_id = p.product_id
    WHERE s.status = 'delivered'
""")


# volume and cost by product type
run("volume and cost by product type (delivered)", """
    SELECT
        p.type,
        COUNT(*) as shipments,
        ROUND(SUM(s.tonnes), 1) as total_tonnes,
        ROUND(SUM(s.tonnes * p.price_per_tonne), 2) as total_cost
    FROM shipments s
    JOIN products p ON s.product_id = p.product_id
    WHERE s.status = 'delivered'
    GROUP BY p.type
    ORDER BY total_cost DESC
""")


# shipments by supplier type
run("shipments by supplier type", """
    SELECT
        sup.type as supplier_type,
        COUNT(*) as shipments,
        ROUND(SUM(s.tonnes), 1) as total_tonnes
    FROM shipments s
    JOIN suppliers sup ON s.supplier_id = sup.supplier_id
    WHERE s.status = 'delivered'
    GROUP BY sup.type
    ORDER BY shipments DESC
""")


# monthly volume for 2023
run("monthly delivered volume 2023", """
    SELECT
        strftime('%Y-%m', ship_date) as month,
        COUNT(*) as shipments,
        ROUND(SUM(s.tonnes), 1) as total_tonnes
    FROM shipments s
    WHERE s.status = 'delivered'
    GROUP BY month
    ORDER BY month
""")


# top 3 suppliers by volume delivered
run("top suppliers by volume delivered", """
    SELECT
        sup.name,
        sup.type,
        COUNT(*) as shipments,
        ROUND(SUM(s.tonnes), 1) as total_tonnes
    FROM shipments s
    JOIN suppliers sup ON s.supplier_id = sup.supplier_id
    WHERE s.status = 'delivered'
    GROUP BY s.supplier_id
    ORDER BY total_tonnes DESC
    LIMIT 3
""")


# cancellation rate by supplier type
run("cancellation rate by supplier type", """
    SELECT
        sup.type,
        COUNT(*) as total,
        SUM(CASE WHEN s.status = 'cancelled' THEN 1 ELSE 0 END) as cancelled,
        ROUND(100.0 * SUM(CASE WHEN s.status = 'cancelled' THEN 1 ELSE 0 END) / COUNT(*), 1) as cancel_pct
    FROM shipments s
    JOIN suppliers sup ON s.supplier_id = sup.supplier_id
    GROUP BY sup.type
    ORDER BY cancel_pct DESC
""")


# average shipment size by product
run("average shipment size by product (delivered)", """
    SELECT
        p.name,
        ROUND(AVG(s.tonnes), 1) as avg_tonnes
    FROM shipments s
    JOIN products p ON s.product_id = p.product_id
    WHERE s.status = 'delivered'
    GROUP BY s.product_id
    ORDER BY avg_tonnes DESC
""")

db.close()
print("done")
