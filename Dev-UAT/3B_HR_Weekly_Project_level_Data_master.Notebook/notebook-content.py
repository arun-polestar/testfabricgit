# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "f83fbd59-f899-4dee-a930-c1b83d7b14dd",
# META       "default_lakehouse_name": "LHDigitalAnalytics",
# META       "default_lakehouse_workspace_id": "85a32c46-45bf-453a-9d3b-2c35a5f14bf7",
# META       "known_lakehouses": [
# META         {
# META           "id": "07d66a6b-6006-4d50-af17-c3eeb1a862b6"
# META         },
# META         {
# META           "id": "781b6224-d23c-4ce0-88c2-545bcbfae89d"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

a=spark.sql(f"""
Select min(fiscal_year_period) as minfy,max(fiscal_year_period) as maxfy
from LhdigitalanalyticsSilver.cmr_fact_actuals
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

minfy=int(a.first()[0])
maxfy=int(a.first()[1])
print(minfy)
print(maxfy)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import pandas as pd
import datetime
from pyspark.sql.types import StructType, StructField, IntegerType

# Function to generate fiscal periods
def generate_fy_periods(minfy, maxfy):
    fy_periods = []
    current_fy = minfy
    while current_fy <= maxfy:
        fy_periods.append(current_fy)
        year = current_fy // 1000
        month = current_fy % 100
        if month < 12:
            current_fy += 1
        else:
            current_fy = (year + 1) * 1000 + 1
    return fy_periods

# Function to calculate Date_id
def calculate_date_id(fy_period):
    fiscal_year = fy_period // 1000
    fiscal_month = fy_period % 100
    
    # Adjust fiscal month to calendar month by adding 3 months
    adjusted_year = fiscal_year + (1 if fiscal_month + 3 > 12 else 0)
    adjusted_month = fiscal_month + 3 - 12 if fiscal_month + 3 > 12 else fiscal_month + 3
    
    # Generate the last day of the month
    last_day_of_month = (datetime.date(adjusted_year, adjusted_month, 1) 
                         + pd.offsets.MonthEnd(0)).day
    
    # Construct Date_id
    date_id = (adjusted_year * 10000) + (adjusted_month * 100) + last_day_of_month
    return date_id


# Generate fiscal periods
fy_periods = generate_fy_periods(minfy, maxfy)

# Create DataFrame
df = pd.DataFrame({'FYPeriod': fy_periods})

# Calculate Date_id for each FYPeriod
df['dateid'] = df['FYPeriod'].apply(calculate_date_id)
schema = StructType([
    StructField("FYPeriod", IntegerType(), True),
    StructField("dateid", IntegerType(), True)
])

df=spark.createDataFrame(df,schema)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.write.format("delta").option("mergeschema",'True').mode("overwrite").save("abfss://85a32c46-45bf-453a-9d3b-2c35a5f14bf7@onelake.dfs.fabric.microsoft.com/f83fbd59-f899-4dee-a930-c1b83d7b14dd/Tables/cmractualsloaddateid")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }


