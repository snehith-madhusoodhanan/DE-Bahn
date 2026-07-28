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

# DBTITLE 1,Fetch all stations and hours
# MAGIC %skip
# MAGIC # Initialize: Create XML wrapper with root element
# MAGIC volume_path = '/Volumes/workspace/default/raw_data/planned.xml'
# MAGIC with open(volume_path, 'w', encoding='utf-8') as file:
# MAGIC     file.write('<?xml version="1.0" encoding="UTF-8"?>\n<timetable>\n')
# MAGIC
# MAGIC total_requests = len(hbf_list) * 24
# MAGIC current_request = 0
# MAGIC
# MAGIC for station in hbf_list:
# MAGIC     for hour in range(0, 24):
# MAGIC         current_request += 1
# MAGIC         hour_str = f"{hour:02d}"
# MAGIC       
# MAGIC         # API parameters
# MAGIC         station_id = station
# MAGIC         date = datetime.now().strftime('%y%m%d')  # YYMMDD format
# MAGIC
# MAGIC         url = f"https://apis.deutschebahn.com/db-api-marketplace/apis/timetables/v1/plan/{station_id}/{date}/{hour_str}"
# MAGIC
# MAGIC         headers = {
# MAGIC             "DB-Client-ID": "519270aaffbddccafead0cb4337d98a1",
# MAGIC             "DB-Api-Key": "82c95ed227857ddc0307012ce6a1796e",
# MAGIC             "accept": "application/xml"
# MAGIC         }
# MAGIC
# MAGIC         print(f"[{current_request}/{total_requests}] Fetching station {station_id}, hour {hour_str}...")
# MAGIC         
# MAGIC         try:
# MAGIC             response = requests.get(url, headers=headers, timeout=30)
# MAGIC
# MAGIC             if response.status_code == 200:
# MAGIC                 # Parse response and extract <s> elements only
# MAGIC                 root = ET.fromstring(response.text)
# MAGIC                 
# MAGIC                 # Append each <s> element to our accumulated XML file
# MAGIC                 with open(volume_path, 'a', encoding='utf-8') as file:
# MAGIC                     for s_element in root.findall('s'):
# MAGIC                         file.write(ET.tostring(s_element, encoding='unicode'))
# MAGIC                         file.write('\n')
# MAGIC                 
# MAGIC                 print(f"[OK] Station {station_id}, hour {hour_str} - {len(root.findall('s'))} events added")
# MAGIC                 
# MAGIC             else:
# MAGIC                 print(f"[ERROR] Station {station_id}, hour {hour_str} - Status code: {response.status_code}")
# MAGIC                 
# MAGIC         except Exception as e:
# MAGIC             print(f"[ERROR] Station {station_id}, hour {hour_str} - {str(e)}")
# MAGIC
# MAGIC # Close the root XML element
# MAGIC with open(volume_path, 'a', encoding='utf-8') as file:
# MAGIC     file.write('</timetable>\n')
# MAGIC
# MAGIC print(f"\n=== All data fetched. Creating temp view... ===")
# MAGIC
# MAGIC # Drop existing view and create new one with all accumulated data
# MAGIC spark.sql("DROP VIEW IF EXISTS planned")
# MAGIC             
# MAGIC df = spark.read \
# MAGIC     .format("xml") \
# MAGIC     .option("rowTag", "s") \
# MAGIC     .load(volume_path)
# MAGIC     
# MAGIC df.createOrReplaceTempView("planned")
# MAGIC
# MAGIC print(f"[DONE] Temp view 'planned' created with {df.count()} total events")    
# MAGIC

# COMMAND ----------

# DBTITLE 1,Cell 5
open('/Volumes/workspace/default/raw_data/planned.xml', 'w').close()

volume_path = '/Volumes/workspace/default/raw_data/planned.xml'

total_requests = len(hbf_list) * 24
current_request = 0

all_elements = []

for station in hbf_list:
    for hour in range(0, 24):
        current_request += 1
        hour_str = f"{hour:02d}"
        date = datetime.now().strftime('%y%m%d')
        url = f"https://apis.deutschebahn.com/db-api-marketplace/apis/timetables/v1/plan/{station}/{date}/{hour_str}"
        headers = {
            "DB-Client-ID": "519270aaffbddccafead0cb4337d98a1",
            "DB-Api-Key": "82c95ed227857ddc0307012ce6a1796e",
            "accept": "application/xml"
        }

        print(f"[{current_request}/{total_requests}] Fetching station {station}, hour {hour_str}...")

        try:
            response = requests.get(url, headers=headers, timeout=30)
            if response.status_code == 200:
                root = ET.fromstring(response.text)
                for s_element in root.findall('s'):
                    s_element.set('eva', str(station))  # Inject EVA number as attribute
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


spark.sql("DROP VIEW IF EXISTS planned")
            
df = spark.read \
    .format("xml") \
    .option("rowTag", "s") \
    .load(volume_path)
    
df.createOrReplaceTempView("planned")



print(f"Written {len(all_elements)} elements to {volume_path}")

# COMMAND ----------


volume_path = '/Volumes/workspace/default/raw_data/planned.xml'



spark.sql("DROP VIEW IF EXISTS planned")
            
df = spark.read \
    .format("xml") \
    .option("rowTag", "s") \
    .load(volume_path)
    
df.createOrReplaceTempView("planned")



# print(f"Written {len(all_elements)} elements to {volume_path}")

# COMMAND ----------

# DBTITLE 1,Create PlannedTimeTable
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY TABLE plannedTimeTable AS
# MAGIC SELECT 
# MAGIC     _eva AS EVA_Number,
# MAGIC     _id AS EventId,
# MAGIC     tl._c AS TrainType,
# MAGIC     ar._fb AS TrainName,
# MAGIC     
# MAGIC     -- Arrival fields
# MAGIC     CASE 
# MAGIC         WHEN ar._l IS NULL THEN 'NO'  -- No _l field means not a replacement (normal for ICE trains)
# MAGIC         WHEN ar._fb != ar._l THEN 'YES'  -- Different from planned means replacement
# MAGIC         ELSE 'NO' 
# MAGIC     END AS Arrival_IsReplaced,
# MAGIC     CASE 
# MAGIC         WHEN ar._l IS NOT NULL AND ar._fb != ar._l THEN get(split(ar._fb, ' '), 0)
# MAGIC         ELSE NULL
# MAGIC     END AS Arrival_ReplacementType,
# MAGIC     CASE 
# MAGIC         WHEN ar._l IS NOT NULL AND ar._fb != ar._l THEN get(split(ar._fb, ' '), 1)
# MAGIC         ELSE NULL
# MAGIC     END AS Arrival_ReplacementName,
# MAGIC     ar._pp AS Arrival_PlannedPlatform,
# MAGIC     ar._pt AS Arrival_PlannedTime,
# MAGIC     
# MAGIC     -- Departure fields
# MAGIC     CASE 
# MAGIC         WHEN dp._l IS NULL THEN 'NO'  -- No _l field means not a replacement (normal for ICE trains)
# MAGIC         WHEN dp._fb != dp._l THEN 'YES'  -- Different from planned means replacement
# MAGIC         ELSE 'NO' 
# MAGIC     END AS Departure_IsReplaced,
# MAGIC     CASE 
# MAGIC         WHEN dp._l IS NOT NULL AND dp._fb != dp._l THEN get(split(dp._fb, ' '), 0)
# MAGIC         ELSE NULL
# MAGIC     END AS Departure_ReplacementType,
# MAGIC     CASE 
# MAGIC         WHEN dp._l IS NOT NULL AND dp._fb != dp._l THEN get(split(dp._fb, ' '), 1)
# MAGIC         ELSE NULL
# MAGIC     END AS Departure_ReplacementName,
# MAGIC     dp._pp AS Departure_PlannedPlatform,
# MAGIC     dp._pt AS Departure_PlannedTime
# MAGIC FROM planned
# MAGIC

# COMMAND ----------

# DBTITLE 1,Deduplicate on EventId
# MAGIC %sql
# MAGIC -- Remove duplicate EventIds, keeping only the first occurrence
# MAGIC CREATE OR REPLACE TEMPORARY TABLE plannedTimeTable AS
# MAGIC SELECT 
# MAGIC     EVA_Number,
# MAGIC     EventId,
# MAGIC     TrainType,
# MAGIC     TrainName,
# MAGIC     Arrival_IsReplaced,
# MAGIC     Arrival_ReplacementType,
# MAGIC     Arrival_ReplacementName,
# MAGIC     Arrival_PlannedPlatform,
# MAGIC     Arrival_PlannedTime,
# MAGIC     Departure_IsReplaced,
# MAGIC     Departure_ReplacementType,
# MAGIC     Departure_ReplacementName,
# MAGIC     Departure_PlannedPlatform,
# MAGIC     Departure_PlannedTime
# MAGIC FROM (
# MAGIC     SELECT 
# MAGIC         *,
# MAGIC         ROW_NUMBER() OVER (PARTITION BY EventId ORDER BY COALESCE(Arrival_PlannedTime, Departure_PlannedTime)) as row_num
# MAGIC     FROM plannedTimeTable
# MAGIC )
# MAGIC WHERE row_num = 1;

# COMMAND ----------

# DBTITLE 1,Recreate fact_plannedtimetable
# MAGIC %skip
# MAGIC %sql
# MAGIC -- Drop existing table
# MAGIC DROP TABLE IF EXISTS `bahn-data-ingestion`.s3_data.fact_plannedtimetable;
# MAGIC
# MAGIC -- Create new table with current schema
# MAGIC CREATE TABLE `bahn-data-ingestion`.s3_data.fact_plannedtimetable (
# MAGIC     EVA_Number STRING,
# MAGIC     EventId STRING,
# MAGIC     TrainType STRING,
# MAGIC     TrainName STRING,
# MAGIC     Arrival_IsReplaced STRING,
# MAGIC     Arrival_ReplacementType STRING,
# MAGIC     Arrival_ReplacementName STRING,
# MAGIC     Arrival_PlannedPlatform STRING,
# MAGIC     Arrival_PlannedTime BIGINT,
# MAGIC     Departure_IsReplaced STRING,
# MAGIC     Departure_ReplacementType STRING,
# MAGIC     Departure_ReplacementName STRING,
# MAGIC     Departure_PlannedPlatform STRING,
# MAGIC     Departure_PlannedTime BIGINT
# MAGIC ) USING DELTA;

# COMMAND ----------

# DBTITLE 1,Merge into fact_plannedtimetable
# MAGIC %sql
# MAGIC MERGE INTO `bahn-data-ingestion`.s3_data.fact_plannedtimetable AS target
# MAGIC USING plannedTimeTable AS source
# MAGIC ON target.EventId = source.EventId
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (
# MAGIC     EVA_Number, EventId, TrainType, TrainName,
# MAGIC     Arrival_IsReplaced, Arrival_ReplacementType, Arrival_ReplacementName,
# MAGIC     Arrival_PlannedPlatform, Arrival_PlannedTime,
# MAGIC     Departure_IsReplaced, Departure_ReplacementType, Departure_ReplacementName,
# MAGIC     Departure_PlannedPlatform, Departure_PlannedTime
# MAGIC   )
# MAGIC   VALUES (
# MAGIC     source.EVA_Number, source.EventId, source.TrainType, source.TrainName,
# MAGIC     source.Arrival_IsReplaced, source.Arrival_ReplacementType, source.Arrival_ReplacementName,
# MAGIC     source.Arrival_PlannedPlatform, source.Arrival_PlannedTime,
# MAGIC     source.Departure_IsReplaced, source.Departure_ReplacementType, source.Departure_ReplacementName,
# MAGIC     source.Departure_PlannedPlatform, source.Departure_PlannedTime
# MAGIC   );