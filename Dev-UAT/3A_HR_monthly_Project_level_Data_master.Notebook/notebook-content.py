# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "52c24218-5198-45c3-b36e-78d6c0d6e186",
# META       "default_lakehouse_name": "LHDev",
# META       "default_lakehouse_workspace_id": "85a32c46-45bf-453a-9d3b-2c35a5f14bf7",
# META       "known_lakehouses": [
# META         {
# META           "id": "02d6b5a3-6c6a-4bce-bf62-702d35265a0f"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.read.table("LHDev.actiongrouptest")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }


