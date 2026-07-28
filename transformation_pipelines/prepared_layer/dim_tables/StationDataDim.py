# Databricks notebook source
# DBTITLE 1,Cell 2
import requests

# Fetch XML data from API
url = "https://apis.deutschebahn.com/db-api-marketplace/apis/timetables/v1/station/*"

headers = {
    "DB-Client-ID": "519270aaffbddccafead0cb4337d98a1",
    "DB-Api-Key": "82c95ed227857ddc0307012ce6a1796e",
    "accept": "application/xml"
}

response = requests.get(url, headers=headers)

# Write XML to workspace file
workspace_path = "/Volumes/workspace/default/raw_data/stations.xml"
with open(workspace_path, "w") as f:
    f.write(response.text)

# Read XML using Spark's XML reader
df_stations = spark.read \
    .format("xml") \
    .option("rowTag", "station") \
    .load(workspace_path)

# display(df_stations)


# COMMAND ----------

from pyspark.sql.functions import current_date, lit

df_stations = df_stations.withColumn("ValidFrom", current_date()) \
    .withColumn("ValidTo", lit('9999-12-31'))

df_stations.createOrReplaceTempView("stations")


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from stations
# MAGIC where _updatets is null

# COMMAND ----------

# DBTITLE 1,Create dimension table
from pyspark.sql.functions import col

# Clean up column names and set proper data types
df_clean = df_stations.select(
    col("_eva").cast("long").alias("eva_number"),
    col("_ds100").alias("ds100_code"),
    col("_name").alias("station_name"),
    col("_meta").alias("meta_stations"),
    col("_db").cast("boolean").alias("is_db_station"),
    col("_creationts").alias("creation_timestamp"),
    col("ValidFrom"),
    col("ValidTo")
)

# Write to Unity Catalog table (automatically uses S3 via schema's managed location)
target_table = "bahn-data-ingestion.s3_data.dim_stations"

# df_clean.write \
#     .format("delta") \
#     .mode("overwrite") \
#     .option("overwriteSchema", "true") \
#     .saveAsTable(target_table)

# print(f"Created dimension table: {target_table}")
# print(f"Record count: {df_clean.count()}")

# COMMAND ----------

df_clean.createOrReplaceTempView("raw_data")

# COMMAND ----------

# DBTITLE 1,CDC + SCD2
# MAGIC %sql
# MAGIC MERGE INTO `bahn-data-ingestion`.s3_data.dim_stations AS Target
# MAGIC USING raw_data AS Source
# MAGIC ON Target.eva_number = Source.eva_number
# MAGIC AND Target.creation_timestamp = Source.creation_timestamp
# MAGIC
# MAGIC WHEN NOT MATCHED BY Target THEN
# MAGIC     INSERT (
# MAGIC         eva_number,
# MAGIC         ds100_code,
# MAGIC         station_name,
# MAGIC         meta_stations,
# MAGIC         is_db_station,
# MAGIC         creation_timestamp,
# MAGIC         ValidFrom,
# MAGIC         ValidTo
# MAGIC     )
# MAGIC     VALUES (
# MAGIC         Source.eva_number,
# MAGIC         Source.ds100_code,
# MAGIC         Source.station_name,
# MAGIC         Source.meta_stations,
# MAGIC         Source.is_db_station,
# MAGIC         Source.creation_timestamp,
# MAGIC         Source.ValidFrom,
# MAGIC         Source.ValidTo
# MAGIC     )
# MAGIC WHEN NOT MATCHED BY Source AND Target.ValidTo = '9999-12-31' THEN
# MAGIC     UPDATE SET 
# MAGIC         Target.ValidTo = current_date()

# COMMAND ----------

# DBTITLE 1,Protection: Restore if HBF count drops
# Protection mechanism: Check active HBF count and restore if too low

# Count active HBF stations
active_hbf_count = spark.sql("""
    SELECT COUNT(*) as count
    FROM `bahn-data-ingestion`.s3_data.dim_stations
    WHERE station_name LIKE '%Hbf' 
    AND ValidTo = '9999-12-31'
""").collect()[0]['count']

print(f"Current active HBF stations: {active_hbf_count}")

# If count is below threshold, restore to version before bad MERGE
if active_hbf_count < 130:
    print(f"⚠️ Warning: Active HBF count ({active_hbf_count}) is below threshold (130)")
    
    # Get current version
    history = spark.sql("DESCRIBE HISTORY `bahn-data-ingestion`.s3_data.dim_stations LIMIT 1")
    current_version = history.select('version').collect()[0]['version']
    restore_version = current_version - 2
    
    print(f"Restoring to version {restore_version}...")
    
    # Restore to previous version (accounting for auto-OPTIMIZE after MERGE)
    spark.sql(f"RESTORE TABLE `bahn-data-ingestion`.s3_data.dim_stations TO VERSION AS OF {restore_version}")
    
    # Verify restoration
    restored_hbf_count = spark.sql("""
        SELECT COUNT(*) as count
        FROM `bahn-data-ingestion`.s3_data.dim_stations
        WHERE station_name LIKE '%Hbf' 
        AND ValidTo = '9999-12-31'
    """).collect()[0]['count']
    
    print(f"✅ Restored! Active HBF count now: {restored_hbf_count}")
else:
    print(f"✅ HBF count looks good ({active_hbf_count} >= 130 threshold)")