# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Full Messages Fact Pipeline
# MAGIC %md
# MAGIC # Full Messages Fact Pipeline
# MAGIC
# MAGIC This pipeline processes messages from full train event snapshots and maintains the `fact_messages` table with SCD2 history.
# MAGIC
# MAGIC **Grain:** One row per (eva_number, EventId, EventType, MessageId, SnapshotTimestamp)
# MAGIC
# MAGIC **Prerequisites:** Must run AFTER FullChangesFact creates the `fullchanges_messages_deduplicated` temp view

# COMMAND ----------

# DBTITLE 1,Import libraries
import requests
import xml.etree.ElementTree as ET
import os
import sys
from datetime import datetime

# COMMAND ----------

# DBTITLE 1,Get HBF stations
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

# DBTITLE 1,Fetch and create fullchanges view
open('/Volumes/workspace/default/raw_data/fullchanges.xml', 'w').close()

volume_path = '/Volumes/workspace/default/raw_data/fullchanges.xml'

total_requests = len(hbf_list) * 24
current_request = 0

all_elements = []

for station in hbf_list:
    current_request += 1
    url = f"https://apis.deutschebahn.com/db-api-marketplace/apis/timetables/v1/fchg/{station}"
    headers = {
        "DB-Client-ID": "519270aaffbddccafead0cb4337d98a1",
        "DB-Api-Key": "82c95ed227857ddc0307012ce6a1796e",
        "accept": "application/xml"
    }

    print(f"[{current_request}/{total_requests}] Fetching station {station}")

    try:
        response = requests.get(url, headers=headers, timeout=30)
        if response.status_code == 200:
            root = ET.fromstring(response.text)
            for s_element in root.findall('s'):
                all_elements.append(ET.tostring(s_element, encoding='unicode'))
            print(f"[OK] {len(root.findall('s'))} events")
        else:
            print(f"[ERROR] Status {response.status_code}")
    except Exception as e:
        print(f"[ERROR] {str(e)}")

# Write once
xml_content = '<?xml version="1.0" encoding="UTF-8"?>\n<timetable>\n'
xml_content += '\n'.join(all_elements)
xml_content += '\n</timetable>\n'

with open(volume_path, 'w', encoding='utf-8') as file:
    file.write(xml_content)


spark.sql("DROP VIEW IF EXISTS fullchanges")
            
df = spark.read \
    .format("xml") \
    .option("rowTag", "s") \
    .load(volume_path)
    
df.createOrReplaceTempView("fullchanges")

print(f"Written {len(all_elements)} elements to {volume_path}")

# COMMAND ----------

# DBTITLE 1,Extract messages from events
# MAGIC %sql
# MAGIC -- MESSAGE-LEVEL FACT
# MAGIC CREATE OR REPLACE TEMPORARY VIEW fullchanges_messages AS
# MAGIC
# MAGIC -- Arrival event messages from ar.m
# MAGIC SELECT 
# MAGIC     CAST(_eva AS BIGINT) as eva_number,
# MAGIC     _id as EventId,
# MAGIC     'Arrival' as EventType,
# MAGIC     current_timestamp() as SnapshotTimestamp,
# MAGIC     msg._id as MessageId,
# MAGIC     msg._t as MessageType,
# MAGIC     CAST(msg._ts AS BIGINT) as MessageTime,
# MAGIC     NULL as MessageCategory,
# MAGIC     CAST(msg._ts AS BIGINT) as MessageValidFrom,
# MAGIC     CAST(msg._ts AS BIGINT) as MessageValidTo,
# MAGIC     true as IsActive
# MAGIC FROM fullchanges
# MAGIC LATERAL VIEW EXPLODE(ar.m) exploded_ar_msg AS msg
# MAGIC WHERE ar IS NOT NULL AND ar.m IS NOT NULL
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC -- Departure event messages from dp.m
# MAGIC SELECT 
# MAGIC     CAST(_eva AS BIGINT) as eva_number,
# MAGIC     _id as EventId,
# MAGIC     'Departure' as EventType,
# MAGIC     current_timestamp() as SnapshotTimestamp,
# MAGIC     msg._id as MessageId,
# MAGIC     msg._t as MessageType,
# MAGIC     CAST(msg._ts AS BIGINT) as MessageTime,
# MAGIC     NULL as MessageCategory,
# MAGIC     CAST(msg._ts AS BIGINT) as MessageValidFrom,
# MAGIC     CAST(msg._ts AS BIGINT) as MessageValidTo,
# MAGIC     true as IsActive
# MAGIC FROM fullchanges
# MAGIC LATERAL VIEW EXPLODE(dp.m) exploded_dp_msg AS msg
# MAGIC WHERE dp IS NOT NULL AND dp.m IS NOT NULL
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC -- Stop-level messages from main m field
# MAGIC SELECT 
# MAGIC     CAST(_eva AS BIGINT) as eva_number,
# MAGIC     _id as EventId,
# MAGIC     'Stop' as EventType,
# MAGIC     current_timestamp() as SnapshotTimestamp,
# MAGIC     msg._id as MessageId,
# MAGIC     msg._t as MessageType,
# MAGIC     CAST(msg._ts AS BIGINT) as MessageTime,
# MAGIC     msg._cat as MessageCategory,
# MAGIC     CAST(msg._from AS BIGINT) as MessageValidFrom,
# MAGIC     CAST(msg._to AS BIGINT) as MessageValidTo,
# MAGIC     true as IsActive
# MAGIC FROM fullchanges
# MAGIC LATERAL VIEW EXPLODE(m) exploded_main_msg AS msg
# MAGIC WHERE m IS NOT NULL;

# COMMAND ----------

# DBTITLE 1,Deduplicate messages
# MAGIC %sql
# MAGIC -- Deduplicate MESSAGES: Keep only one record per (EventId, EventType, MessageId)
# MAGIC CREATE OR REPLACE TEMPORARY VIEW fullchanges_messages_deduplicated AS
# MAGIC WITH ranked AS (
# MAGIC     SELECT *,
# MAGIC         ROW_NUMBER() OVER (
# MAGIC             PARTITION BY EventId, EventType, MessageId
# MAGIC             ORDER BY SnapshotTimestamp DESC
# MAGIC         ) AS rn
# MAGIC     FROM fullchanges_messages
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
# MAGIC     FROM fullchanges_messages_deduplicated
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
# MAGIC SELECT * FROM fullchanges_messages_deduplicated;