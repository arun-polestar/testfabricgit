# Fabric notebook source
 
# METADATA ********************
 
# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "07d66a6b-6006-4d50-af17-c3eeb1a862b6",
# META       "default_lakehouse_name": "LHDigitalAnalyticsSilver",
# META       "default_lakehouse_workspace_id": "85a32c46-45bf-453a-9d3b-2c35a5f14bf7",
# META       "known_lakehouses": [
# META         {
# META           "id": "07d66a6b-6006-4d50-af17-c3eeb1a862b6"
# META         },
# META         {
# META           "id": "f83fbd59-f899-4dee-a930-c1b83d7b14dd"
# META         }
# META       ]
# META     },
# META     "environment": {
# META       "environmentId": "43d27e85-e552-9643-4c17-f80709dab799",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }
 
# CELL ********************
 
from pyspark.sql.functions import col,length
import pandas as pd
env_df = pd.read_json(f"{notebookutils.nbResPath}/env/environment_config.json",typ="series")
from datetime import datetime
 
# METADATA ********************
 
# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
 
# PARAMETERS CELL ********************
 
Startdate='2024-04-01'
 
# METADATA ********************
 
# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": true
# META }
 
# CELL ********************


