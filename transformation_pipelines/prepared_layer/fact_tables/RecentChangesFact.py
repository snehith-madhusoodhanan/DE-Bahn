# Databricks notebook source
# MAGIC %md
# MAGIC Import libraries

# COMMAND ----------

import requests
import xml.etree.ElementTree as ET
import os
import sys
from datetime import datetime

# COMMAND ----------

allHBFs = spark.sql(f'''
            select
            eva_number
            from
            `bahn-data-ingestion`.s3_data.dim_stations
            where
            station_name like '%Hbf'
            and ValidFrom<= current_date()
            and ValidTo>current_date()
            ''')
hbf_list = [row.eva_number for row in allHBFs.collect()]

# COMMAND ----------

# DBTITLE 1,Read recentchanges.xml from Volume
# Read ALL timestamped recentchanges_*.jsonl files from Volume
# (Files are written by EC2 collector script every 2 minutes)
# Spark automatically reads and unions all matching files!

import glob

volume_pattern = '/Volumes/workspace/default/raw_data/recentchanges_*.jsonl'

# Check if any files exist
matching_files = glob.glob(volume_pattern)
if not matching_files:
    raise FileNotFoundError(f"No JSONL files found matching {volume_pattern}. Ensure EC2 collector is running.")

print(f"Found {len(matching_files)} timestamped file(s)")
total_size = sum(os.path.getsize(f) for f in matching_files)
print(f"Total size: {total_size:,} bytes")

# Drop existing view if present
spark.sql("DROP VIEW IF EXISTS recentchanges")

# Read ALL JSONL files at once - Spark unions them automatically!
df = spark.read.json(volume_pattern)

record_count = df.count()
print(f"Loaded {record_count:,} records from {len(matching_files)} file(s)")

if record_count == 0:
    raise ValueError("No records found in JSONL files. Files may be empty.")

df.createOrReplaceTempView("recentchanges")
print("✓ Created temporary view 'recentchanges'")

# COMMAND ----------

# DBTITLE 1,Cell 5
# MAGIC %sql
# MAGIC -- EVENT-LEVEL FACT (no messages)
# MAGIC CREATE OR REPLACE TEMPORARY VIEW recentchanges_events AS
# MAGIC
# MAGIC -- Arrival events (event-level attributes only)
# MAGIC SELECT 
# MAGIC     CAST(eva AS BIGINT) as eva_number,
# MAGIC     id as EventId,
# MAGIC     CAST(_collected_at AS TIMESTAMP) as SnapshotTimestamp,
# MAGIC     'Arrival' as EventType,
# MAGIC     CAST(ar.clt AS BIGINT) as CancellationTime,
# MAGIC     ar.cp as ChangedPlatform,
# MAGIC     CAST(ar.ct AS BIGINT) as ChangedTime,
# MAGIC     ar.l as TrainName,
# MAGIC     COALESCE(ar.cs, NULL) as EventStatus,
# MAGIC     CASE WHEN ar.cs = 'a' THEN true ELSE false END as IsUnplannedEvent,
# MAGIC     true as IsActive
# MAGIC FROM recentchanges
# MAGIC WHERE ar IS NOT NULL
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC -- Departure events (event-level attributes only)
# MAGIC SELECT 
# MAGIC     CAST(eva AS BIGINT) as eva_number,
# MAGIC     id as EventId,
# MAGIC     CAST(_collected_at AS TIMESTAMP) as SnapshotTimestamp,
# MAGIC     'Departure' as EventType,
# MAGIC     CAST(dp.clt AS BIGINT) as CancellationTime,
# MAGIC     dp.cp as ChangedPlatform,
# MAGIC     CAST(dp.ct AS BIGINT) as ChangedTime,
# MAGIC     dp.l as TrainName,
# MAGIC     COALESCE(dp.cs, NULL) as EventStatus,
# MAGIC     CASE WHEN dp.cs = 'a' THEN true ELSE false END as IsUnplannedEvent,
# MAGIC     true as IsActive
# MAGIC FROM recentchanges
# MAGIC WHERE dp IS NOT NULL
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC -- Stop-level events (no event attributes - all NULL)
# MAGIC SELECT DISTINCT
# MAGIC     CAST(eva AS BIGINT) as eva_number,
# MAGIC     id as EventId,
# MAGIC     CAST(_collected_at AS TIMESTAMP) as SnapshotTimestamp,
# MAGIC     'Stop' as EventType,
# MAGIC     NULL as CancellationTime,
# MAGIC     NULL as ChangedPlatform,
# MAGIC     NULL as ChangedTime,
# MAGIC     NULL as TrainName,
# MAGIC     NULL as EventStatus,
# MAGIC     false as IsUnplannedEvent,
# MAGIC     true as IsActive
# MAGIC FROM recentchanges
# MAGIC WHERE ar IS NULL AND dp IS NULL;
# MAGIC
# MAGIC

# COMMAND ----------

# DBTITLE 1,Deduplicate to keep latest record per event+message
# MAGIC %sql
# MAGIC -- Deduplicate EVENTS: Keep only the LATEST snapshot per (EventId, EventType, IsUnplannedEvent)
# MAGIC CREATE OR REPLACE TEMPORARY VIEW recentchanges_events_deduplicated AS
# MAGIC WITH ranked AS (
# MAGIC     SELECT *,
# MAGIC         ROW_NUMBER() OVER (
# MAGIC             PARTITION BY EventId, EventType, IsUnplannedEvent
# MAGIC             ORDER BY SnapshotTimestamp DESC
# MAGIC         ) AS rn
# MAGIC     FROM recentchanges_events
# MAGIC )
# MAGIC SELECT 
# MAGIC     eva_number,
# MAGIC     EventId,
# MAGIC     SnapshotTimestamp,
# MAGIC     EventType,
# MAGIC     CancellationTime,
# MAGIC     ChangedPlatform,
# MAGIC     ChangedTime,
# MAGIC     TrainName,
# MAGIC     EventStatus,
# MAGIC     IsUnplannedEvent,
# MAGIC     IsActive
# MAGIC FROM ranked 
# MAGIC WHERE rn = 1;

# COMMAND ----------

# DBTITLE 1,Create fact_changedtimetable and merge data
# MAGIC %sql
# MAGIC -- Step 1: Create the EVENT fact table (no message columns)
# MAGIC CREATE TABLE IF NOT EXISTS `bahn-data-ingestion`.s3_data.fact_changedtimetable (
# MAGIC     eva_number BIGINT,
# MAGIC     EventId STRING,
# MAGIC     SnapshotTimestamp TIMESTAMP,
# MAGIC     EventType STRING,
# MAGIC     CancellationTime BIGINT,
# MAGIC     ChangedPlatform STRING,
# MAGIC     ChangedTime BIGINT,
# MAGIC     TrainName STRING,
# MAGIC     EventStatus STRING,
# MAGIC     IsUnplannedEvent BOOLEAN,
# MAGIC     IsActive BOOLEAN
# MAGIC ) USING DELTA;
# MAGIC
# MAGIC -- Step 2: Deactivate existing records when newer records arrive in the same category
# MAGIC MERGE INTO `bahn-data-ingestion`.s3_data.fact_changedtimetable AS target
# MAGIC USING recentchanges_events_deduplicated AS source
# MAGIC ON target.EventId = source.EventId 
# MAGIC    AND target.EventType = source.EventType
# MAGIC    AND target.IsUnplannedEvent = source.IsUnplannedEvent
# MAGIC    AND target.IsActive = true
# MAGIC WHEN MATCHED THEN 
# MAGIC     UPDATE SET IsActive = false;
# MAGIC
# MAGIC -- Step 3: Insert all new event records as active
# MAGIC

# COMMAND ----------

# DBTITLE 1,Insert new event records
# MAGIC %sql
# MAGIC INSERT INTO `bahn-data-ingestion`.s3_data.fact_changedtimetable
# MAGIC SELECT eva_number, EventId, SnapshotTimestamp, EventType, CancellationTime, 
# MAGIC        ChangedPlatform, ChangedTime, TrainName, EventStatus, IsUnplannedEvent, IsActive
# MAGIC FROM recentchanges_events_deduplicated;