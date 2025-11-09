from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from datetime import datetime, timedelta
from config import connection_properties, jdbc_url

# Get input date
dbutils.widgets.text("date","","Processing Date")
input_date = dbutils.widgets.get("date")

if input_date == "" or input_date is None:
    processing_date = (datetime.now() - timedelta(days=1)).date()
else:
    processing_date = datetime.strptime(input_date, "%Y-%m-%d").date()
print(f"Processing date for date: {processing_date}")

# CHECK table exist
def table_exists(table_name):
    return spark.catalog.tableExists(table_name)

# DELETE/INSERT for timeseries data (use transaction_date)
def load_transaction_to_table(df,table_name,processing_date):
    full_table_name = f"sonht35_ojt.health_care.managed_{table_name}s"
    if not table_exists(full_table_name):
        df.write.mode("overwrite").partitionBy("transaction_date").saveAsTable(full_table_name)
        return
    delta_table = DeltaTable.forName(spark, full_table_name)
    delta_table.delete(f"transaction_date='{processing_date}'")
    df.write.mode("append").saveAsTable(full_table_name)

# DELETE/INSERT for snapshot data (use data_date)
def load_snapshot_to_table(df,table_name,processing_date):
    full_table_name = f"sonht35_ojt.health_care.managed_{table_name}s"
    if not table_exists(full_table_name):
        df.write.mode("overwrite").partitionBy("data_date").saveAsTable(full_table_name)
        return
    delta_table = DeltaTable.forName(spark, full_table_name)
    # delta_table.delete(f"data_date='{processing_date}'")
    delta_table.alias("t").merge(df.alias("s"), f"t.{table_name}id = s.{table_name}id"
                                  ).whenNotMatchedBySourceDelete(f"t.data_date = '{processing_date}'"
                                  ).whenMatchedUpdateAll(
                                  ).whenNotMatchedInsertAll(
                                  ).execute()

from pyspark.sql.functions import col, lit, to_date
# Load data
table_name = ["customer", "store", "product","discount","transaction"]

for tbl in table_name:
    source_table = f"staging.{tbl}s"
    df = (
        spark.read
        .format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", source_table)
        .options(**connection_properties)
        .load()
    )
    if tbl == "transaction":
        df = df.filter(col("transaction_date") == lit(processing_date))
        load_transaction_to_table(df,tbl,processing_date)
    else:
        df = df.filter(col("data_date") == lit(processing_date))
        load_snapshot_to_table(df,tbl,processing_date)