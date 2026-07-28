# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Messages Fact Pipeline
# MAGIC %md
# MAGIC # Messages Fact Pipeline
# MAGIC
# MAGIC This pipeline processes messages from recent train events and maintains the `fact_messages` table with SCD2 history.
# MAGIC
# MAGIC **Grain:** One row per (eva_number, EventId, EventType, MessageId, SnapshotTimestamp)
# MAGIC
# MAGIC **Prerequisites:** Must run AFTER RecentChangesFact creates the `recentchanges_messages_deduplicated` temp view

# COMMAND ----------

# DBTITLE 1,Read JSONL files from Volume
# Read ALL timestamped recentchanges_*.jsonl files from Volume
import glob
import os

volume_pattern = '/Volumes/workspace/default/raw_data/recentchanges_*.jsonl'

matching_files = glob.glob(volume_pattern)
if not matching_files:
    raise FileNotFoundError(f"No JSONL files found matching {volume_pattern}")

print(f"Found {len(matching_files)} timestamped file(s)")

spark.sql("DROP VIEW IF EXISTS recentchanges")
df = spark.read.json(volume_pattern)

record_count = df.count()
print(f"Loaded {record_count:,} records from {len(matching_files)} file(s)")

df.createOrReplaceTempView("recentchanges")
print("✓ Created temporary view 'recentchanges'")

# COMMAND ----------

# DBTITLE 1,Extract messages from events
# MAGIC %sql
# MAGIC -- MESSAGE-LEVEL FACT
# MAGIC CREATE OR REPLACE TEMPORARY VIEW recentchanges_messages AS
# MAGIC
# MAGIC -- Arrival event messages from ar.m
# MAGIC SELECT 
# MAGIC     CAST(eva AS BIGINT) as eva_number,
# MAGIC     id as EventId,
# MAGIC     'Arrival' as EventType,
# MAGIC     CAST(_collected_at AS TIMESTAMP) as SnapshotTimestamp,
# MAGIC     msg.id as MessageId,
# MAGIC     msg.t as MessageType,
# MAGIC     CAST(msg.ts AS BIGINT) as MessageTime,
# MAGIC     NULL as MessageCategory,
# MAGIC     CAST(msg.ts AS BIGINT) as MessageValidFrom,
# MAGIC     CAST(msg.ts AS BIGINT) as MessageValidTo,
# MAGIC     true as IsActive
# MAGIC FROM recentchanges
# MAGIC LATERAL VIEW EXPLODE(from_json(ar.m, 'array<struct<id:string,t:string,c:string,ts:string>>')) exploded_ar_msg AS msg
# MAGIC WHERE ar IS NOT NULL AND ar.m IS NOT NULL
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC -- Departure event messages from dp.m
# MAGIC SELECT 
# MAGIC     CAST(eva AS BIGINT) as eva_number,
# MAGIC     id as EventId,
# MAGIC     'Departure' as EventType,
# MAGIC     CAST(_collected_at AS TIMESTAMP) as SnapshotTimestamp,
# MAGIC     msg.id as MessageId,
# MAGIC     msg.t as MessageType,
# MAGIC     CAST(msg.ts AS BIGINT) as MessageTime,
# MAGIC     NULL as MessageCategory,
# MAGIC     CAST(msg.ts AS BIGINT) as MessageValidFrom,
# MAGIC     CAST(msg.ts AS BIGINT) as MessageValidTo,
# MAGIC     true as IsActive
# MAGIC FROM recentchanges
# MAGIC LATERAL VIEW EXPLODE(from_json(dp.m, 'array<struct<id:string,t:string,c:string,ts:string>>')) exploded_dp_msg AS msg
# MAGIC WHERE dp IS NOT NULL AND dp.m IS NOT NULL
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC -- Stop-level messages from main m field
# MAGIC SELECT 
# MAGIC     CAST(eva AS BIGINT) as eva_number,
# MAGIC     id as EventId,
# MAGIC     'Stop' as EventType,
# MAGIC     CAST(_collected_at AS TIMESTAMP) as SnapshotTimestamp,
# MAGIC     msg.id as MessageId,
# MAGIC     msg.t as MessageType,
# MAGIC     CAST(msg.ts AS BIGINT) as MessageTime,
# MAGIC     msg.cat as MessageCategory,
# MAGIC     CAST(msg.from AS BIGINT) as MessageValidFrom,
# MAGIC     CAST(msg.to AS BIGINT) as MessageValidTo,
# MAGIC     true as IsActive
# MAGIC FROM recentchanges
# MAGIC LATERAL VIEW EXPLODE(from_json(m, 'array<struct<id:string,t:string,c:string,ts:string,cat:string,from:string,to:string>>')) exploded_main_msg AS msg
# MAGIC WHERE m IS NOT NULL;

# COMMAND ----------

# DBTITLE 1,Deduplicate messages
# MAGIC %sql
# MAGIC -- Deduplicate MESSAGES: Keep only the LATEST snapshot per (EventId, EventType, MessageId)
# MAGIC CREATE OR REPLACE TEMPORARY VIEW recentchanges_messages_deduplicated AS
# MAGIC WITH ranked AS (
# MAGIC     SELECT *,
# MAGIC         ROW_NUMBER() OVER (
# MAGIC             PARTITION BY EventId, EventType, MessageId
# MAGIC             ORDER BY SnapshotTimestamp DESC
# MAGIC         ) AS rn
# MAGIC     FROM recentchanges_messages
# MAGIC )
# MAGIC SELECT 
# MAGIC     eva_number,
# MAGIC     EventId,
# MAGIC     EventType,
# MAGIC     SnapshotTimestamp,
# MAGIC     MessageId,
# MAGIC     MessageType,
# MAGIC     MessageTime,
# MAGIC     MessageCategory,
# MAGIC     MessageValidFrom,
# MAGIC     MessageValidTo,
# MAGIC     IsActive
# MAGIC FROM ranked 
# MAGIC WHERE rn = 1;

# COMMAND ----------

# DBTITLE 1,Create fact_messages table
# MAGIC %sql
# MAGIC -- Create the MESSAGES fact table with SCD2 (IsActive flag)
# MAGIC CREATE TABLE IF NOT EXISTS `bahn-data-ingestion`.s3_data.fact_messages (
# MAGIC     eva_number BIGINT,
# MAGIC     EventId STRING,
# MAGIC     EventType STRING,
# MAGIC     SnapshotTimestamp TIMESTAMP,
# MAGIC     MessageId STRING,
# MAGIC     MessageType STRING,
# MAGIC     MessageTime BIGINT,
# MAGIC     MessageCategory STRING,
# MAGIC     MessageValidFrom BIGINT,
# MAGIC     MessageValidTo BIGINT,
# MAGIC     IsActive BOOLEAN
# MAGIC ) USING DELTA;

# COMMAND ----------

# DBTITLE 1,Deactivate old message versions
# MAGIC %sql
# MAGIC -- Deactivate existing messages that have new versions in this snapshot
# MAGIC MERGE INTO `bahn-data-ingestion`.s3_data.fact_messages AS target
# MAGIC USING (
# MAGIC     SELECT DISTINCT EventId, EventType, MessageId
# MAGIC     FROM recentchanges_messages_deduplicated
# MAGIC ) AS source
# MAGIC ON target.EventId = source.EventId 
# MAGIC    AND target.EventType = source.EventType
# MAGIC    AND target.MessageId = source.MessageId
# MAGIC    AND target.IsActive = true
# MAGIC WHEN MATCHED THEN 
# MAGIC     UPDATE SET IsActive = false;

# COMMAND ----------

# DBTITLE 1,Insert new message records
# MAGIC %sql
# MAGIC -- Insert all new message records as active
# MAGIC INSERT INTO `bahn-data-ingestion`.s3_data.fact_messages
# MAGIC SELECT * FROM recentchanges_messages_deduplicated;

# COMMAND ----------

# DBTITLE 1,Archive JSONL files before deletion

# Archive all timestamped JSONL files to a backup folder before deletion
# This preserves raw data for auditing/debugging purposes

import glob
import os
import shutil
from datetime import datetime

volume_pattern = '/Volumes/workspace/default/raw_data/recentchanges_*.jsonl'
archive_dir = '/Volumes/workspace/default/raw_data/archive'

# Create archive directory if it doesn't exist
os.makedirs(archive_dir, exist_ok=True)

# Find all matching files
files_to_archive = glob.glob(volume_pattern)

if files_to_archive:
    # Add a job run timestamp to the archive folder name for this batch
    batch_timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    batch_archive_dir = os.path.join(archive_dir, f'batch_{batch_timestamp}')
    os.makedirs(batch_archive_dir, exist_ok=True)
    
    for file_path in files_to_archive:
        file_name = os.path.basename(file_path)
        archive_path = os.path.join(batch_archive_dir, file_name)
        shutil.copy2(file_path, archive_path)  # copy2 preserves metadata
    
    print(f"✓ Archived {len(files_to_archive)} file(s) to {batch_archive_dir}")
else:
    print("No files to archive")

# COMMAND ----------

# DBTITLE 1,Empty recentchanges.xml after successful merge

# After successful merge, delete all timestamped JSONL files
# This prepares the volume for the next collection cycle

import glob
import os

volume_pattern = '/Volumes/workspace/default/raw_data/recentchanges_*.jsonl'

# Find all matching files
files_to_delete = glob.glob(volume_pattern)

if files_to_delete:
    for file_path in files_to_delete:
        os.remove(file_path)
    print(f"✓ Deleted {len(files_to_delete)} timestamped file(s) - ready for next collection cycle")
else:
    print("No files to delete")