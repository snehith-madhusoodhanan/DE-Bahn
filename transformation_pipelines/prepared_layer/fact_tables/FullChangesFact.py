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

# DBTITLE 1,Archive fullchanges.xml file
import shutil

# Define paths
source_file = '/Volumes/workspace/default/raw_data/fullchanges.xml'
archive_dir = '/Volumes/workspace/default/raw_data/archive'

# Create archive directory if it doesn't exist
os.makedirs(archive_dir, exist_ok=True)

# Generate timestamp-based filename
timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
archive_filename = f'fullchanges_{timestamp}.xml'
archive_path = os.path.join(archive_dir, archive_filename)

# Copy file to archive
shutil.copy2(source_file, archive_path)

print(f"Archived {source_file} to {archive_path}")

# COMMAND ----------

# DBTITLE 1,Transform fullchanges to fact table
# MAGIC %sql
# MAGIC -- EVENT-LEVEL FACT (no messages)
# MAGIC CREATE OR REPLACE TEMPORARY VIEW fullchanges_events AS
# MAGIC
# MAGIC -- Arrival events (event-level attributes only)
# MAGIC SELECT 
# MAGIC     CAST(_eva AS BIGINT) as eva_number,
# MAGIC     _id as EventId,
# MAGIC     current_timestamp() as SnapshotTimestamp,
# MAGIC     'Arrival' as EventType,
# MAGIC     CAST(ar._clt AS BIGINT) as CancellationTime,
# MAGIC     ar._cp as ChangedPlatform,
# MAGIC     CAST(ar._ct AS BIGINT) as ChangedTime,
# MAGIC     -- CAST(ar._pt AS BIGINT) as PlannedTime,
# MAGIC     ar._l as TrainName,
# MAGIC     COALESCE(ar._cs, NULL) as EventStatus,
# MAGIC     CASE WHEN ar._cs = 'a' THEN true ELSE false END as IsUnplannedEvent,
# MAGIC     true as IsActive
# MAGIC     -- ar._ppth
# MAGIC FROM fullchanges
# MAGIC WHERE ar IS NOT NULL
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC -- Departure events (event-level attributes only)
# MAGIC SELECT 
# MAGIC     CAST(_eva AS BIGINT) as eva_number,
# MAGIC     _id as EventId,
# MAGIC     current_timestamp() as SnapshotTimestamp,
# MAGIC     'Departure' as EventType,
# MAGIC     CAST(dp._clt AS BIGINT) as CancellationTime,
# MAGIC     dp._cp as ChangedPlatform,
# MAGIC     CAST(dp._ct AS BIGINT) as ChangedTime,
# MAGIC     -- CAST(dp._pt AS BIGINT) as PlannedTime,
# MAGIC     dp._l as TrainName,
# MAGIC     COALESCE(dp._cs, NULL) as EventStatus,
# MAGIC     CASE WHEN dp._cs = 'a' THEN true ELSE false END as IsUnplannedEvent,
# MAGIC     true as IsActive
# MAGIC     -- dp._ppth
# MAGIC FROM fullchanges
# MAGIC WHERE dp IS NOT NULL
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC -- Stop-level events (no event attributes - all NULL)
# MAGIC SELECT DISTINCT
# MAGIC     CAST(_eva AS BIGINT) as eva_number,
# MAGIC     _id as EventId,
# MAGIC     current_timestamp() as SnapshotTimestamp,
# MAGIC     'Stop' as EventType,
# MAGIC     NULL as CancellationTime,
# MAGIC     NULL as ChangedPlatform,
# MAGIC     NULL as ChangedTime,
# MAGIC     -- NULL as PlannedTime,
# MAGIC     NULL as TrainName,
# MAGIC     NULL as EventStatus,
# MAGIC     false as IsUnplannedEvent,
# MAGIC     true as IsActive
# MAGIC     -- null as ppth
# MAGIC FROM fullchanges
# MAGIC WHERE ar IS NULL AND dp IS NULL;
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %skip
# MAGIC %sql
# MAGIC select * from fullchanges
# MAGIC where _id = '8378187760869811763-2607110541-100'

# COMMAND ----------

# MAGIC %skip
# MAGIC %sql
# MAGIC select * from fullchanges_events
# MAGIC where eventid = '8378187760869811763-2607110541-100'

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
# MAGIC -- Step 2: Deduplicate source - keep latest record per (EventId, EventType, IsUnplannedEvent)
# MAGIC CREATE OR REPLACE TEMPORARY VIEW fullchanges_events_deduplicated AS
# MAGIC SELECT *
# MAGIC FROM (
# MAGIC     SELECT *,
# MAGIC            ROW_NUMBER() OVER (PARTITION BY EventId, EventType, IsUnplannedEvent 
# MAGIC                               ORDER BY SnapshotTimestamp DESC) as rn
# MAGIC     FROM fullchanges_events
# MAGIC )
# MAGIC WHERE rn = 1;
# MAGIC
# MAGIC -- Step 3: Deactivate existing records when newer records arrive in the same category
# MAGIC MERGE INTO `bahn-data-ingestion`.s3_data.fact_changedtimetable AS target
# MAGIC USING fullchanges_events_deduplicated AS source
# MAGIC ON target.EventId = source.EventId 
# MAGIC    AND target.EventType = source.EventType
# MAGIC    AND target.IsUnplannedEvent = source.IsUnplannedEvent
# MAGIC    AND target.IsActive = true
# MAGIC WHEN MATCHED THEN 
# MAGIC     UPDATE SET IsActive = false;
# MAGIC
# MAGIC -- Step 4: Insert all new event records as active
# MAGIC INSERT INTO `bahn-data-ingestion`.s3_data.fact_changedtimetable
# MAGIC SELECT eva_number, EventId, SnapshotTimestamp, EventType, CancellationTime, 
# MAGIC        ChangedPlatform, ChangedTime, TrainName, EventStatus, IsUnplannedEvent, IsActive
# MAGIC FROM fullchanges_events_deduplicated;