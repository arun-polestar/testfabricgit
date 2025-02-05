# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "2ad7827c-d97b-4b65-bb00-4bb537924aa3",
# META       "default_lakehouse_name": "LHDigitalAnalyticsGold",
# META       "default_lakehouse_workspace_id": "85a32c46-45bf-453a-9d3b-2c35a5f14bf7",
# META       "known_lakehouses": [
# META         {
# META           "id": "07d66a6b-6006-4d50-af17-c3eeb1a862b6"
# META         },
# META         {
# META           "id": "f83fbd59-f899-4dee-a930-c1b83d7b14dd"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

import pandas as pd

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.types import StringType, DecimalType, DateType

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def extract_excel(file_name,module):
    
    import pandas as pd
    result =spark.sql(f"""
    SELECT DestinationFolderPath,DestinationFileName,LastLoadDate as lastdate
    FROM LHDigitalAnalytics.excelfiles
    where SourceFileName='{file_name}' and Module = '{module}'
    """)
    
    DestinationFolderPath=result.first()["DestinationFolderPath"]
    DestinationFileName=result.first()["DestinationFileName"]
    last_date = int(result.select("lastdate").first()[0].strftime('%Y%m%d'))
    try:
        year=str(last_date)[0:4]
        month=str(last_date)[4:6]
        day=str(last_date)[6:8]
    except:
        print("SQL table has wrong format for date")
    
    bronze_lakehouse_path="abfss://85a32c46-45bf-453a-9d3b-2c35a5f14bf7@onelake.dfs.fabric.microsoft.com/f83fbd59-f899-4dee-a930-c1b83d7b14dd/Files/Bronze"
    print("Year:",year,"Month:",month,"Day:",day)
    
    file_path = f"{bronze_lakehouse_path}/Excel/{module}/{DestinationFolderPath}/{year}/{month}/{day}/{DestinationFileName}"
    print("File Path : ",file_path)
    
    try:
        df = pd.ExcelFile(file_path)
    except Exception as e:
            print('Error:', e)
    return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

create_table_query = """
CREATE TABLE IF NOT EXISTS LHDigitalAnalyticsSilver.cmr_fact_actuals(
    ABFFlag STRING NOT NULL,
    Mode_group STRING,
    Profit_Center STRING,
    mode_code STRING,
    mode_desc String,
    Fact_Curr_Key STRING,
    Segment_for_Reporting STRING,
    Business_Area_PP String,
    Flag_CustProj STRING NOT NULL,
    Fiscal_year_period string,
    Key_Master STRING,
    COUNTRY String,
    CITY String,
    CATEGORY String,
    Cost_Center STRING,
    Overall_Project_Category STRING,
    ProjectType STRING,
    ProjectCode STRING,
    Component_Group STRING,
    OFF_ON String,
    Band STRING,
    Employee_Subgroup STRING,
    EmpGroup STRING NOT NULL,
    CustomerCode String,
    CustomerName STRING,
    Asset_Service_Classification STRING,
    Total_Revenue Decimal(38,18),
    UO_A_Transport_Cost Decimal(38,18),
    UO_A_Resource_Cost Decimal(38,18),
    Shared_Service Decimal(38,18),
    Service Decimal(38,18),
    Material Decimal(38,18),
    Total_BFTE DECIMAL(28, 2),
    Total_GFTE DECIMAL(28, 2),
    Salary_Cost Decimal(38,18),
    Fresher_Cost_Credit Decimal(38,18),
    Consulting_TP_Cost Decimal(38,18),
    Project_Expenses_others Decimal(38,18),
    BW_Cost Decimal(38,18),
    Travel_Cost Decimal(38,18),
    Transport_Cost Decimal(38,18),
    Bad_Debt Decimal(38,18),
    Facility_Cost Decimal(38,18),
    DC_Std_Cost_Proj Decimal(38,18),
    Domestic_Direct_Cost Decimal(38,18),
    SEZ_Booster Decimal(38,18),
    Solution_Adjustment_wbs Decimal(38,18),
    Delivery_OHs_Old Decimal(38,18),
    Pre_sales_solution_OHs Decimal(38,18), 
    SDU_RDU_Direct Decimal(38,18),
    LOB_OHs1 Decimal(38,18),
    L3_OHs Decimal(38,18),
    Recruitment_Cost_1 Decimal(38,18),
    Training_Cost Decimal(38,18),
    Practice_SBU_OHs Decimal(38,18),
    Vertical_Head_OHs Decimal(38,18),
    Horizontal_Head_OHs Decimal(38,18),
    Enabling_OHs Decimal(38,18),
    UO_A_BW_Cost Decimal(38,18),
    UO_A_Facility_Cost Decimal(38,18),
    LTI Decimal(38,18),
    Amortization_Cost Decimal(38,18),
    ce_category string,
    FYPeriod string,
    created_date date
) USING DELTA
"""

# Execute the create table query
spark.sql(create_table_query)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

is_incremental=spark.sql("""
Select count(*) as cnt from LHDigitalAnalyticsSilver.cmr_fact_actuals
""")
is_incremental = int(is_incremental.select("cnt").first()[0])
print(is_incremental)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": true
# META }

# CELL ********************

query = """
SELECT max(fiscal_year_period) as Fiscper
from LHDigitalAnalyticsSilver.integrated_pp_stg2_data
"""

result=spark.sql(query)
display(result)
var = result.select("Fiscper").first()[0]
print(var)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": true
# META }

# CELL ********************

d=spark.sql(f"""
Select Quarter from LHDigitalAnalyticsGold.dimcalendar
where fyperiod={var}
""")
qtr=d.first()[0]
print(qtr)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

try:
    if is_incremental==0:
        df=spark.sql("""
        Select * from LHDigitalAnalyticsSilver.integrated_pp_stg2_data
        """)
        df.createOrReplaceTempView("pp_data")
    else:
        df=spark.sql(f"""
        Select /*+ BROADCAST(cal) */ * from LHDigitalAnalyticsSilver.integrated_pp_stg2_data d
        Left join (SELECT Distinct Fyperiod,Quarter from LHDigitalAnalyticsGold.dimcalendar) cal 
        on d.fiscal_year_period=cal.Fyperiod
        where Quarter='{qtr}'
        """)
        df.createOrReplaceTempView("pp_data")
except Exception as e:
    # Handle the exception
    print(f"An error occurred: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": true
# META }

# CELL ********************

projectcat=extract_excel("ProjectCategoryGrouping.xlsx","CMR")
pandas_df=pd.read_excel(projectcat,sheet_name='Sheet1')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

try:
    ProjectT= spark.createDataFrame(pandas_df)
    ProjectT.createOrReplaceTempView("Project_T")
except Exception as e:
    print('Error:', e)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

bm=extract_excel("Band_Mapping.xlsx","CMR")
pandas_df=pd.read_excel(bm,sheet_name='Sheet1')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

try:
    #pandas_df = pd.read_excel("abfss://85a32c46-45bf-453a-9d3b-2c35a5f14bf7@onelake.dfs.fabric.microsoft.com/f83fbd59-f899-4dee-a930-c1b83d7b14dd/Files/Bronze/CMR/Excel/Band_Mapping/Band_Mapping_20241014.xlsx", sheet_name='Sheet1')  # Specify the sheet if needed
    band= spark.createDataFrame(pandas_df)
    band=band.withColumnRenamed("Employee Subgroup","Employee_Subgroup")
    band.createOrReplaceTempView("band_map")
except Exception as e:
    print('Error:', e)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

pp_actual_stage1 = spark.sql("""
SELECT
    -- 'A' as ABFFlag,
    CASE 
        WHEN LENGTH(TRIM(SUBSTRING_INDEX(customer_key, '|', -1))) = 0 THEN NULL 
        ELSE SUBSTRING_INDEX(customer_key, '|', -1) 
    END AS CustomerCode,
    
    CASE 
        WHEN LENGTH(TRIM(Employee)) > 0 AND Employee <> '00000000' THEN CONCAT(Employee, '|', 'Employee')
        WHEN LENGTH(TRIM(WBS_Element)) > 0 THEN CONCAT(REPLACE(WBS_Element, ' ', ''), '|', 'WBS/Project')
        WHEN LENGTH(TRIM(Cost_Center)) > 0 THEN CONCAT(Cost_Center, '|', 'CostCenter')
        ELSE 'Not Assigned' 
    END AS Key_BusinessAreaFlag,
    
    CASE 
        WHEN LENGTH(TRIM(Employee)) > 0 AND Employee <> '00000000' THEN CONCAT(Employee, '|', 'Employee') 
    END AS Key_BusinessAreaFlag1,
    
    CASE 
        WHEN LENGTH(TRIM(WBS_Element)) > 0 THEN CONCAT(REPLACE(WBS_Element, ' ', ''), '|', 'WBS/Project') 
    END AS Key_BusinessAreaFlag2,
    
    CASE 
        WHEN LENGTH(TRIM(Cost_Center)) > 0 THEN CONCAT(Cost_Center, '|', 'CostCenter')  
        ELSE 'Not Assigned' 
    END AS Key_BusinessAreaFlag3,
    
    Fact_Curr_Key,
    (Facility_Cost_blocked_seats + CASE WHEN Segment_for_Reporting = 'APPS' THEN under_over_absorption_of_bw_cost ELSE 0 END) AS U_O_A_Resource_Cost,
    CASE WHEN Segment_for_Reporting IN ('IG', 'Apps', 'APPS') THEN Domestic_Direct_Cost ELSE 0.00 END AS Shared_Service,
    Segment_for_Reporting,
    CASE 
        WHEN LEFT(TRIM(Project_Definition), 1) IN ('C', 'D', 'E', 'V', 'Y') THEN 'Customer Project' 
        ELSE 'Non-Customer Project' 
    END AS Flag_CustProj,
    Project_Definition AS ProjectCode,
    
    Cost_Center,
    Fiscal_year_period AS Fiscal_year_period,
    pp.Cost_Element,
    case when Fiscal_year_period<=2023009 then ce.CE_Category else pp.CE_Category end as CE_Category,
    case when pro.ProjectCategory is not null then pro.ProjectCategoryGroup else pp.Project_Category end AS ProjectType,
    Component_Group,
    Customer_Name AS CustomerName,
    Business_Area_PP AS Business_Area_PP,
    CASE WHEN pp.Employee_Subgroup <> 'TP' THEN 'FTE' ELSE 'TP' END AS EmpGroup,
    CASE 
        WHEN Project_Definition IS NULL OR TRIM(Project_Definition) = '' OR TRIM(Project_Definition) = '#' OR LENGTH(Project_Definition) <= 0 THEN CONCAT(Cost_Center, '|', 'CostCenter') 
        ELSE CONCAT(Project_Definition, '|', 'Project') 
    END AS Key_Master,
    case when band.Employee_Subgroup is not null then band.group  else null end AS Band,
    CASE 
        WHEN (case when pro.ProjectCategory is not null then pro.ProjectCategoryGroup else pp.Project_Category end) is null THEN NULL
        ELSE CASE 
            WHEN (case when pro.ProjectCategory is not null then pro.ProjectCategoryGroup else pp.Project_Category end) = 'T&M' THEN 'T&M' 
            ELSE 'Non-T&M' 
        END 
    END AS Overall_Project_Category,
    pp.Employee_Subgroup,
    Object_Profit_Center AS Profit_Center,
    mode,
    mode_desc,
    Pre_sales_solution_OHs,
    Service,
    Material,
    Revenue_Service,
    Billing_Material,
    Total_BFTE,
    Total_GFTE,
    Total_Revenue,
    Other_Direct_Cost,
    Asset_Service_Classification,
    Salary_Cost,
    Fresher_Cost_Credit,
    Consulting_TP_Cost,
    -- Under_Over_absorption_Resource_cost,
    Project_Expenses_others,
    BW_Cost,
    Travel_Cost,
    Transport_Cost,
    Bad_Debt,
    Facility_Cost,
    DC_Std_Cost_Proj,
    CASE WHEN Segment_for_Reporting NOT IN ('IG', 'Apps', 'APPS') THEN Domestic_Direct_Cost ELSE 0.00 END AS Domestic_Direct_Cost,
    SEZ_Booster,
    Solution_Adjustment_wbs,
    -- Shared_Service,
    under_Over_absorption_of_transport_cost as U_O_A_Transport_Cost,
    SDU_RDU_Direct,
    LOB_OHs1,
    L3_OHs,
    Recruitment_Cost_1,
    Training_Cost,
    Practice_SBU_OHs,
    Vertical_Head_OHs,
    Horizontal_Head_OHs,
    Enabling_OHs,
    CASE WHEN (Segment_for_Reporting <> 'APPS') THEN under_over_absorption_of_bw_cost END AS U_O_A_BW_Cost,
    under_Over_absorption_of_facility_cost as U_O_A_Facility_Cost,
    LTI,
    Amortization_Cost,
    Delivery_OHs_Old,
    Employee,
    WBS_Element
FROM
    pp_data pp
    LEFT join Project_T pro
    on pp.Project_Category=pro.ProjectCategory
    Left join band_map band
    on Trim(pp.Employee_Subgroup)=trim(band.Employee_Subgroup)
    Left join LhdigitalAnalyticsSilver.cecategory2023map ce
    on pp.Cost_Element=ce.Cost_Element
""")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

pp_actual_stage1.createOrReplaceTempView("pp_actual_stage")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

pp_actual_stage2=spark.sql("""
Select pp.*,
    case when ba1.Key_BusinessAreaFlag is not null then ba1.off_on
    else 'NA' end as off_on1,
    case when ba2.Key_BusinessAreaFlag is not null then ba2.off_on
    else 'NA' end as off_on2,
    case when ba3.Key_BusinessAreaFlag is not null then ba3.off_on
    else 'NA' end as off_on3,
    case when ba1.Key_BusinessAreaFlag is not null then ba1.city
    else 'NA' end as city1,
    case when ba2.Key_BusinessAreaFlag is not null then ba2.city
    else 'NA' end as city2,
    case when ba3.Key_BusinessAreaFlag is not null then ba3.city
    else 'NA' end as city3,
    case when ba1.Key_BusinessAreaFlag is not null then ba1.country
    else 'NA' end as country1,
    case when ba2.Key_BusinessAreaFlag is not null then ba2.country
    else 'NA' end as country2,
    case when ba3.Key_BusinessAreaFlag is not null then ba3.country
    else 'NA' end as country3,
    case when ba.Key_BusinessAreaFlag is not null then upper(ba.category)
    else 'Not Assigned' end as category
    from pp_actual_stage pp
    Left join LHDigitalAnalyticsSilver.businessarea ba
    on coalesce(ba.Key_BusinessAreaFlag,'')=coalesce(pp.Key_BusinessAreaFlag,'')
    Left join LHDigitalAnalyticsSilver.businessarea ba1
    on coalesce(ba1.Key_BusinessAreaFlag,'')=coalesce(pp.Key_BusinessAreaFlag1,'')
    Left join LHDigitalAnalyticsSilver.businessarea ba2
    on coalesce(ba2.Key_BusinessAreaFlag,'')=coalesce(pp.Key_BusinessAreaFlag2,'')
    Left join LHDigitalAnalyticsSilver.businessarea ba3
    on coalesce(ba3.Key_BusinessAreaFlag,'')=coalesce(pp.Key_BusinessAreaFlag3,'')
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

pp_actual_stage2.createOrReplaceTempView("pp_actual_stage")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

pp_actual_stage2=spark.sql("""
Select pp.*,
    case when Country1<>'NA' then upper(Country1)
    when Country2<>'NA' then upper(Country2)
    when Country3<>'NA'then upper(Country3) else null end as Country,
    case when city1<>'NA' then upper(city1)
    when city2<>'NA' then upper(city2)
    when city3<>'NA'then upper(city3) else null end as city,
    case when off_on1<>'NA' then upper(off_on1)
    when off_on2<>'NA' then upper(off_on2)
    when off_on3<>'NA'then upper(off_on3) else null end as off_on
    from pp_actual_stage pp
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

pp_actual_stage2.createOrReplaceTempView("pp_actual_stage")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("""
SELECT
    Profit_Center,
    Mode as mode_code,
    Mode_Desc,
    Fact_Curr_Key,
    Segment_for_Reporting,
    Business_Area_PP,
    Flag_CustProj,
    Fiscal_year_period,
    Key_Master,
    country,
    city,
    category,
    Cost_Center,
    Overall_Project_Category,
    ProjectType,
    ProjectCode,
   Component_Group,
    off_on,
    Band,
    Employee_Subgroup,
    EmpGroup,
    CustomerCode,
    CustomerName,
    Asset_Service_Classification,
    SUM(CAST(Total_Revenue AS DECIMAL(38,18))) AS Total_Revenue,
SUM(CAST(U_O_A_Transport_Cost AS DECIMAL(38,18))) AS UO_A_Transport_Cost,
SUM(CAST(U_O_A_Resource_Cost AS DECIMAL(38,18))) AS UO_A_Resource_Cost,
SUM(CAST(Shared_Service AS DECIMAL(38,18))) AS Shared_Service,
SUM(CAST(Service AS DECIMAL(38,18))) AS Service,
SUM(CAST(Material AS DECIMAL(38,18))) AS Material,
SUM(CAST(Total_BFTE AS DECIMAL(38,18))) AS Total_BFTE,
SUM(CAST(Total_GFTE AS DECIMAL(38,18))) AS Total_GFTE,
SUM(CAST(Salary_Cost AS DECIMAL(38,18))) AS Salary_Cost,
SUM(CAST(Fresher_Cost_Credit AS DECIMAL(38,18))) AS Fresher_Cost_Credit,
SUM(CAST(Consulting_TP_Cost AS DECIMAL(38,18))) AS Consulting_TP_Cost,
SUM(CAST(Project_Expenses_others AS DECIMAL(38,18))) AS Project_Expenses_others,
SUM(CAST(BW_Cost AS DECIMAL(38,18))) AS BW_Cost,
SUM(CAST(Travel_Cost AS DECIMAL(38,18))) AS Travel_Cost,
SUM(CAST(Transport_Cost AS DECIMAL(38,18))) AS Transport_Cost,
SUM(CAST(Bad_Debt AS DECIMAL(38,18))) AS Bad_Debt,
SUM(CAST(Facility_Cost AS DECIMAL(38,18))) AS Facility_Cost,
SUM(CAST(DC_Std_Cost_Proj AS DECIMAL(38,18))) AS DC_Std_Cost_Proj,
SUM(CAST(Domestic_Direct_Cost AS DECIMAL(38,18))) AS Domestic_Direct_Cost,
SUM(CAST(SEZ_Booster AS DECIMAL(38,18))) AS SEZ_Booster,
SUM(CAST(Solution_Adjustment_wbs AS DECIMAL(38,18))) AS Solution_Adjustment_wbs,
SUM(CAST(Delivery_OHs_Old AS DECIMAL(38,18))) AS Delivery_OHs_Old,
SUM(CAST(Pre_sales_solution_OHs AS DECIMAL(38,18))) AS Pre_sales_solution_OHs,
SUM(CAST(SDU_RDU_Direct AS DECIMAL(38,18))) AS SDU_RDU_Direct,
SUM(CAST(LOB_OHs1 AS DECIMAL(38,18))) AS LOB_OHs1,
SUM(CAST(L3_OHs AS DECIMAL(38,18))) AS L3_OHs,
SUM(CAST(Recruitment_Cost_1 AS DECIMAL(38,18))) AS Recruitment_Cost_1,
SUM(CAST(Training_Cost AS DECIMAL(38,18))) AS Training_Cost,
SUM(CAST(Practice_SBU_OHs AS DECIMAL(38,18))) AS Practice_SBU_OHs,
SUM(CAST(Vertical_Head_OHs AS DECIMAL(38,18))) AS Vertical_Head_OHs,
SUM(CAST(Horizontal_Head_OHs AS DECIMAL(38,18))) AS Horizontal_Head_OHs,
SUM(CAST(Enabling_OHs AS DECIMAL(38,18))) AS Enabling_OHs,
SUM(CAST(U_O_A_BW_Cost AS DECIMAL(38,18))) AS UO_A_BW_Cost,
SUM(CAST(U_O_A_Facility_Cost AS DECIMAL(38,18))) AS UO_A_Facility_Cost,
SUM(CAST(LTI AS DECIMAL(38,18))) AS LTI,
SUM(CAST(Amortization_Cost AS DECIMAL(38,18))) AS Amortization_Cost,
    CE_Category
FROM
    pp_actual_stage pp
GROUP BY
    Profit_Center,
    Mode,
    Mode_Desc,
    Fact_Curr_Key,
    Segment_for_Reporting,
    Business_Area_PP,
    Flag_CustProj,
    Fiscal_year_period,
    Key_Master,
    country,
    city,
    category,
    Cost_Center,
    Overall_Project_Category,
    ProjectType,
    ProjectCode,
    Component_Group,
    off_on,
    Band,
    Employee_Subgroup,
    EmpGroup,
    CustomerCode,
    CustomerName,
    CE_Category,
    Asset_Service_Classification
""")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

mm=extract_excel("Mode Grouping Master Data.xlsx","CMR")
pandas_df=pd.read_excel(mm,sheet_name='Sheet1')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

try:
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "false")
    #pandas_df = pd.read_excel("abfss://85a32c46-45bf-453a-9d3b-2c35a5f14bf7@onelake.dfs.fabric.microsoft.com/f83fbd59-f899-4dee-a930-c1b83d7b14dd/Files/Bronze/CMR/Excel/Mode Grouping Master Data/Mode Grouping Master Data_20241014.xlsx", sheet_name='Sheet1')  
    pandas_df = pandas_df.rename(columns={'Unnamed: 0': 'mode','Unnamed: 1': 'Mode_text' ,'Unnamed: 2': 'Mode_grouping'})
    mode_map= spark.createDataFrame(pandas_df)
    #mode_map=mode_map.withColumnRenamed("Mode grouping","Mode_grouping")
    mode_map.createOrReplaceTempView("mode_group")
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
except Exception as e:
    print('Error:', e)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.createOrReplaceTempView("actualss")
actuals=spark.sql("""
Select 'A' as ABFFlag,
case when mg.mode is not null then mg.Mode_grouping else 'NA' end as Mode_group
,act.*,Fiscal_year_period as FYPeriod,current_date() as created_date
From actualss act
Left join mode_group mg 
on act.mode_code=mg.mode
""")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fiscal_year_periods_to_delete = [row.fiscal_year_period for row in spark.sql("SELECT DISTINCT fiscal_year_period FROM pp_data").collect()]

for period in fiscal_year_periods_to_delete:
    print(period)
    spark.sql(f"""
        DELETE FROM LHDigitalAnalyticsSilver.cmr_fact_actuals
        WHERE fiscal_year_period = '{period}'
    """)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": true
# META }

# CELL ********************

actuals = actuals.withColumn("ABFFlag", F.col("ABFFlag").cast(StringType())) \
    .withColumn("Mode_group", F.col("Mode_group").cast(StringType())) \
    .withColumn("Profit_Center", F.col("Profit_Center").cast(StringType())) \
    .withColumn("mode_code", F.col("mode_code").cast(StringType())) \
    .withColumn("mode_desc", F.col("mode_desc").cast(StringType())) \
    .withColumn("Fact_Curr_Key", F.col("Fact_Curr_Key").cast(StringType())) \
    .withColumn("Segment_for_Reporting", F.col("Segment_for_Reporting").cast(StringType())) \
    .withColumn("Business_Area_PP", F.col("Business_Area_PP").cast(DecimalType(10, 0))) \
    .withColumn("Flag_CustProj", F.col("Flag_CustProj").cast(StringType())) \
    .withColumn("Fiscal_year_period", F.col("Fiscal_year_period").cast(StringType())) \
    .withColumn("Key_Master", F.col("Key_Master").cast(StringType())) \
    .withColumn("COUNTRY", F.col("COUNTRY").cast(StringType())) \
    .withColumn("CITY", F.col("CITY").cast(StringType())) \
    .withColumn("CATEGORY", F.col("CATEGORY").cast(StringType())) \
    .withColumn("Cost_Center", F.col("Cost_Center").cast(StringType())) \
    .withColumn("Overall_Project_Category", F.col("Overall_Project_Category").cast(StringType())) \
    .withColumn("ProjectType", F.col("ProjectType").cast(StringType())) \
    .withColumn("ProjectCode", F.col("ProjectCode").cast(StringType())) \
    .withColumn("Component_Group", F.col("Component_Group").cast(StringType())) \
    .withColumn("OFF_ON", F.col("OFF_ON").cast(StringType())) \
    .withColumn("Band", F.col("Band").cast(StringType())) \
    .withColumn("Employee_Subgroup", F.col("Employee_Subgroup").cast(StringType())) \
    .withColumn("EmpGroup", F.col("EmpGroup").cast(StringType())) \
    .withColumn("CustomerCode", F.col("CustomerCode").cast(StringType())) \
    .withColumn("CustomerName", F.col("CustomerName").cast(StringType())) \
    .withColumn("Asset_Service_Classification", F.col("Asset_Service_Classification").cast(StringType())) \
    .withColumn("Total_Revenue", F.col("Total_Revenue").cast(DecimalType(38, 18))) \
    .withColumn("UO_A_Transport_Cost", F.col("UO_A_Transport_Cost").cast(DecimalType(38, 18))) \
    .withColumn("UO_A_Resource_Cost", F.col("UO_A_Resource_Cost").cast(DecimalType(38, 18))) \
    .withColumn("Shared_Service", F.col("Shared_Service").cast(DecimalType(38, 18))) \
    .withColumn("Service", F.col("Service").cast(DecimalType(38, 18))) \
    .withColumn("Material", F.col("Material").cast(DecimalType(38, 18))) \
    .withColumn("Total_BFTE", F.col("Total_BFTE").cast(DecimalType(38, 18))) \
    .withColumn("Total_GFTE", F.col("Total_GFTE").cast(DecimalType(38, 18))) \
    .withColumn("Salary_Cost", F.col("Salary_Cost").cast(DecimalType(38, 18))) \
    .withColumn("Fresher_Cost_Credit", F.col("Fresher_Cost_Credit").cast(DecimalType(38, 18))) \
    .withColumn("Consulting_TP_Cost", F.col("Consulting_TP_Cost").cast(DecimalType(38, 18))) \
    .withColumn("Project_Expenses_others", F.col("Project_Expenses_others").cast(DecimalType(38, 18))) \
    .withColumn("BW_Cost", F.col("BW_Cost").cast(DecimalType(38, 18))) \
    .withColumn("Travel_Cost", F.col("Travel_Cost").cast(DecimalType(38, 18))) \
    .withColumn("Transport_Cost", F.col("Transport_Cost").cast(DecimalType(38, 18))) \
    .withColumn("Bad_Debt", F.col("Bad_Debt").cast(DecimalType(38, 18))) \
    .withColumn("Facility_Cost", F.col("Facility_Cost").cast(DecimalType(38, 18))) \
    .withColumn("DC_Std_Cost_Proj", F.col("DC_Std_Cost_Proj").cast(DecimalType(38, 18))) \
    .withColumn("Domestic_Direct_Cost", F.col("Domestic_Direct_Cost").cast(DecimalType(38, 18))) \
    .withColumn("SEZ_Booster", F.col("SEZ_Booster").cast(DecimalType(38, 18))) \
    .withColumn("Solution_Adjustment_wbs", F.col("Solution_Adjustment_wbs").cast(DecimalType(38, 18))) \
    .withColumn("Delivery_OHs_Old", F.col("Delivery_OHs_Old").cast(DecimalType(38, 18))) \
    .withColumn("Pre_sales_solution_OHs", F.col("Pre_sales_solution_OHs").cast(DecimalType(38, 18))) \
    .withColumn("SDU_RDU_Direct", F.col("SDU_RDU_Direct").cast(DecimalType(38, 18))) \
    .withColumn("LOB_OHs1", F.col("LOB_OHs1").cast(DecimalType(38, 18))) \
    .withColumn("L3_OHs", F.col("L3_OHs").cast(DecimalType(38, 18))) \
    .withColumn("Recruitment_Cost_1", F.col("Recruitment_Cost_1").cast(DecimalType(38, 18))) \
    .withColumn("Training_Cost", F.col("Training_Cost").cast(DecimalType(38, 18))) \
    .withColumn("Practice_SBU_OHs", F.col("Practice_SBU_OHs").cast(DecimalType(38, 18))) \
    .withColumn("Vertical_Head_OHs", F.col("Vertical_Head_OHs").cast(DecimalType(38, 18))) \
    .withColumn("Horizontal_Head_OHs", F.col("Horizontal_Head_OHs").cast(DecimalType(38, 18))) \
    .withColumn("Enabling_OHs", F.col("Enabling_OHs").cast(DecimalType(38, 18))) \
    .withColumn("UO_A_BW_Cost", F.col("UO_A_BW_Cost").cast(DecimalType(38, 18))) \
    .withColumn("UO_A_Facility_Cost", F.col("UO_A_Facility_Cost").cast(DecimalType(38, 18))) \
    .withColumn("LTI", F.col("LTI").cast(DecimalType(38, 18))) \
    .withColumn("Amortization_Cost", F.col("Amortization_Cost").cast(DecimalType(38, 18))) \
    .withColumn("ce_category", F.col("ce_category").cast(StringType())) \
    .withColumn("FYPeriod", F.col("FYPeriod").cast(StringType())) \
    .withColumn("created_date", F.col("created_date").cast(DateType()))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************

try:
    actuals.write.format("delta").mode("append").option("mergeSchema", "true").save("abfss://85a32c46-45bf-453a-9d3b-2c35a5f14bf7@onelake.dfs.fabric.microsoft.com/07d66a6b-6006-4d50-af17-c3eeb1a862b6/Tables/cmr_fact_actuals")
except Exception as e:
    print(f"An error occurred: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": true
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC INSERT INTO  LhdigitalAnalyticsSilver.cmr_fact_actuals(
# MAGIC     Fact_Curr_Key,
# MAGIC     Segment_for_Reporting,
# MAGIC     Business_Area_PP,
# MAGIC     Flag_CustProj,
# MAGIC     Fiscal_year_period,
# MAGIC     FYPeriod,
# MAGIC     Overall_Project_Category,
# MAGIC     ProjectType,
# MAGIC     ProjectCode,
# MAGIC     Key_Master,
# MAGIC     Component_Group,
# MAGIC     COUNTRY,
# MAGIC     OFF_ON,
# MAGIC     CITY,
# MAGIC     Band,
# MAGIC     Employee_Subgroup,
# MAGIC     EmpGroup,
# MAGIC     CustomerCode,
# MAGIC     CustomerName,
# MAGIC     ABFFlag,
# MAGIC     Asset_Service_Classification,
# MAGIC     Salary_Cost
# MAGIC ) VALUES
# MAGIC ('USD|Jan 2024', 'IG', NULL, 'Customer Project', '2023010', '2023010-PPInfraDRCCorrection', 'INTERNAL', 'NA', 'I230166', 'I230166|Project', NULL, NULL, NULL,NULL,NULL,NULL, 'Business Line FT', '0000107688', 'ORC Commission', 'A', 'Service', -13303.32),
# MAGIC ('USD|Jan 2024', 'IG', NULL, 'Customer Project', '2023010', '2023010-PPInfraDRCCorrection', 'APS/GLB: RECURRING FIXED BILLING', 'NA', 'C217467', 'C217467|Project', NULL, NULL, NULL,NULL,NULL,NULL, 'Business Line FT', '0000107688', 'ORC Commission', 'A', 'Service', 46578.75),
# MAGIC ('USD|Jan 2024', 'IG', NULL, 'Customer Project', '2023010', '2023010-PPInfraDRCCorrection', 'I013', 'NA', 'S210022', 'S210022|Project', NULL, NULL, NULL,NULL,NULL,NULL, 'Business Line FT', '0000100865', 'PRACTICE', 'A', 'Service', -61009.94),
# MAGIC ('USD|Jan 2024', 'IG', NULL, 'Customer Project', '2023010', '2023010-PPInfraDRCCorrection', 'I014', 'NA', 'E230065', 'E230065|Project', NULL, NULL, NULL,NULL,NULL,NULL, 'Business Line FT', '0000002200', 'HCL Technologies Ltd-IOMC DIVISION', 'A', 'Service', 352876.20),
# MAGIC ('USD|Jan 2024', 'IG', NULL, 'Customer Project', '2023010', '2023010-PPInfraDRCCorrection', 'I014', 'NA', 'E230065', 'E230065|Project', NULL, NULL, NULL,NULL,NULL,NULL, 'Business Line FT', '0000002200', 'HCL Technologies Ltd-IOMC DIVISION', 'A', 'Service', 528130.65),
# MAGIC ('USD|Jan 2024', 'IG', NULL, 'Customer Project', '2023010', '2023010-PPInfraDRCCorrection', 'I014', 'NA', 'E230065', 'E230065|Project', NULL, NULL, NULL,NULL,NULL,NULL, 'Business Line FT', '0000002200', 'HCL Technologies Ltd-IOMC DIVISION', 'A', 'Service', -752.64);

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }


