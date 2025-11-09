from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from datetime import datetime, timedelta

# Get processing date
dbutils.widgets.text("date","","Processing Date")
input_date = dbutils.widgets.get("date")
processing_month =  datetime.strptime(input_date, "%Y-%m-%d").strftime("%Y%m")
print(f'Load data for month {processing_month}')

query = f"""
with month_store as
(
  select '{processing_month}' as month, s.storeid as storeid
  from sonht35_ojt.health_care.managed_stores s
),
revenue as
(
  SELECT 
  TO_CHAR(t.transaction_date, 'yyyyMM') AS transaction_month,
  t.storeid as storeid,
  sum(p.unit_price * t.quantity) AS monthly_revenue,
  count(*) as total_transactions,
  count(distinct t.productid) as num_products,
  sum(case when p.category = 'Electronics' then 1 else 0 end) as electronics_prods,
  sum(case when p.category = 'Fashion' then 1 else 0 end) as fashion_prods,
  sum(case when c.gender = 'M' then 1 else 0 end) as by_male,
  sum(case when c.gender = 'F' then 1 else 0 end) as by_female
  FROM sonht35_ojt.health_care.managed_transactions t
  INNER JOIN sonht35_ojt.health_care.managed_products p
  ON t.productid = p.productid
  INNER JOIN sonht35_ojt.health_care.managed_customers c
  ON t.customerid = c.customerid
  WHERE TO_CHAR(t.transaction_date, 'yyyyMM') = '{processing_month}'
  GROUP BY TO_CHAR(t.transaction_date, 'yyyyMM'), t.storeid 
),
result as
(
  SELECT
  ms.month as transaction_month, ms.storeid as storeid, r.monthly_revenue, r.total_transactions, r.num_products, r.electronics_prods, r.fashion_prods, r.by_male, r.by_female
  from revenue r
  right join month_store ms
  on r.transaction_month = ms.month and r.storeid = ms.storeid
)
MERGE INTO sonht35_ojt.health_care.fact_revenue f USING result r
ON f.transaction_month = r.transaction_month AND f.storeid = r.storeid 
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
"""