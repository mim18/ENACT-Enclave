import pandas as pd
from io import StringIO
import cx_Oracle
import datetime
import os
import sys
import re
import pandas as pd
#import modin.pandas as pd #3x faster
import numpy as np
from sqlalchemy import create_engine
from sqlalchemy import text

import csv as csv
import time
from string import Template


# Select statements per domain
dem_select = """
WITH CTE_COHORT_PATIENT_SET AS (
SELECT * FROM <schema_name>.qt_patient_set_collection where result_instance_id = :result_instance_id --and rownum < 20

)
SELECT   
      a.PATIENT_NUM as "PATIENT_NUM"
      ,a.BIRTH_DATE as "BIRTH_DATE"
      ,a.DEATH_DATE as "DEATH_DATE"
      ,a.SEX_CD as "GENDER"
      , a.AGE_IN_YEARS_NUM as "AGE_IN_YEARS"
      ,a.LANGUAGE_CD as "PRIMARY_SPOKEN_LANGUAGE"
      ,a.RACE_CD as "RACE"
      ,a.MARITAL_STATUS_CD as "MARTIAL_STATUS"
      ,a.RELIGION_CD as "RELIGION"
      ,a.ZIP_CD as "ZIP_CODE"
      ,a.STATECITYZIP_PATH as "STATE_CITY_ZIP"
      ,a.INCOME_CD as "INCOME"
      ,a.VITAL_STATUS_CD as "VITAL_STATUS"
  FROM <schema_name>.patient_dimension a, 
  CTE_COHORT_PATIENT_SET c
  where a.patient_num = c.patient_num
"""

enc_select = """
WITH CTE_COHORT_PATIENT_SET AS (
SELECT * FROM <schema_name>.qt_patient_set_collection where result_instance_id = :result_instance_id --and rownum < 20
)
SELECT 
     a.PATIENT_NUM as "PATIENT_NUM"
        ,a.encounter_num as "ENCOUNTER_NUM"
        ,a.start_date as "START_DATE"
        ,a.end_date as "END_DATE"
        ,A.inout_cd as "INOUT_CD"
        ,a.length_of_stay as "LENGTH_OF_STAY"   
        ,'<SITE_NAME>' as "SITE"
    FROM <schema_name>.visit_dimension  a 
    JOIN CTE_COHORT_PATIENT_SET c on a.patient_num = c.patient_num
"""

dx_select = """
WITH CTE_COHORT_PATIENT_SET AS (
SELECT * FROM <schema_name>.qt_patient_set_collection where result_instance_id = :result_instance_id --and rownum < 20


),
CTE_CODES AS (
    SELECT DISTINCT CONCEPT_CD, NAME_CHAR FROM <schema_name>.concept_dimension 
    WHERE concept_path like '\ACT\Diagnosis\%'
),
CTE_FACTS AS (
    SELECT A.PATIENT_NUM, START_DATE, END_DATE, A.CONCEPT_CD,
           INSTANCE_NUM, MODIFIER_CD, LOCATION_CD, PROVIDER_ID, ENCOUNTER_NUM
    FROM <schema_name>.observation_fact  a 
    JOIN CTE_COHORT_PATIENT_SET c on a.patient_num = c.patient_num
    INNER JOIN CTE_CODES b on a.concept_cd = b.concept_cd
)
SELECT 
     a.PATIENT_NUM as "PATIENT_NUM"
        ,a.start_date as "START_DATE"
        ,a.end_date as "END_DATE"
        --,b.name_char as "NAME_CHAR" --removed creates huge export files
        ,COALESCE(L.ACT_STANDARD_CODE, A.concept_cd ) as "CONCEPT_CD"
        --,a.units_cd as "UNITS_CD"
        --,a.NVAL_NUM as "DOSE_QUANTITY"     
        ,a.instance_num as "INSTANCE_NUM"   
        ,a.modifier_cd as "MODIFIER_CD"
        --,m.name_char as "MODIFIER_NAME" -- not harmonized across i2b2s and not included in ENACT
        ,a.location_cd as "LOCATION_CD" 
        --,case v.inout_cd  when 'O' then 'Outpatient' when 'I' then 'Inpatient' when 'E' then 'Emergency' else 'Unknown' end as "ENCOUNTER_TYPE"
        ,a.provider_id as "PROVIDER"
        ,a.encounter_num as "ENCOUNTER_NUM"
        ,'<SITE_NAME>' as "SITE"
    FROM CTE_FACTS  a 
    JOIN CTE_COHORT_PATIENT_SET c on a.patient_num = c.patient_num
    --WHERE CONCEPT_CD LIKE 'ICD9CM:%' OR  CONCEPT_CD LIKE 'ICD10CM:%' --all icds
    INNER  JOIN CTE_CODES b on a.concept_cd = b.concept_cd --ontology only codes
    LEFT OUTER JOIN ENACT_MAP_LOCAL_TO_STANDARD l on l.local_concept_cd = a.concept_cd --UNCOMMENT IF LOCAL CODE MAP TO STANDARD



"""

px_select = """
WITH CTE_COHORT_PATIENT_SET AS (
 SELECT * FROM <schema_name>.qt_patient_set_collection where result_instance_id = :result_instance_id --and rownum < 20
),
CTE_CODES AS (
    SELECT DISTINCT CONCEPT_CD, NAME_CHAR FROM  <schema_name>.concept_dimension 
    WHERE concept_path like '\ACT\Procedures\%' 
),
CTE_FACTS AS (
    SELECT A.PATIENT_NUM, START_DATE, END_DATE, A.CONCEPT_CD,
           INSTANCE_NUM, MODIFIER_CD, LOCATION_CD, PROVIDER_ID, ENCOUNTER_NUM
    FROM <schema_name>.observation_fact  a 
    JOIN CTE_COHORT_PATIENT_SET c on a.patient_num = c.patient_num
    INNER JOIN CTE_CODES b on a.concept_cd = b.concept_cd
),
CTE_SELECT AS (
SELECT 
       a.PATIENT_NUM as "PATIENT"
        ,a.start_date as "START_DATE"
        ,a.end_date as "END_DATE"
        --,b.name_char as "NAME_CHAR" --removed creates huge export files
        ,COALESCE(L.ACT_STANDARD_CODE, A.concept_cd ) as "CONCEPT_CD"
        --,a.units_cd as "UNITS_CD"
        --,a.NVAL_NUM as "DOSE_QUANTITY"     
        ,a.instance_num as "INSTANCE_NUM"   
        ,a.modifier_cd as "MODIFIER_CD"
        --,m.name_char as "MODIFIER_NAME"  -- not harmonized across i2b2s not include in ENACT
        ,a.location_cd as "LOCATION_CD" 
        --,case v.inout_cd  when 'O' then 'Outpatient' when 'I' then 'Inpatient' when 'E' then 'Emergency' else 'Unknown' end as "ENCOUNTER_TYPE"
        ,a.provider_id as "PROVIDER"
        ,a.encounter_num as "ENCOUNTER_NUM"
        ,'<SITE_NAME>' as "SITE"
    FROM CTE_FACTS  a 
    JOIN CTE_COHORT_PATIENT_SET c on a.patient_num = c.patient_num
    --WHERE CONCEPT_CD LIKE 'ICD9PROC:%' OR  CONCEPT_CD LIKE 'ICD10PCS:%' OR  CONCEPT_CD LIKE 'HCPCS:%' OR  CONCEPT_CD LIKE 'CPT4:%' --all codes
    INNER  JOIN CTE_CODES b on a.concept_cd = b.concept_cd --ontology only
    LEFT OUTER JOIN ENACT_MAP_LOCAL_TO_STANDARD l on l.local_concept_cd = a.concept_cd --UNCOMMENT IF LOCAL CODE MAP TO STANDARD

)
select * from cte_select
"""


lab_select = """
WITH CTE_COHORT_PATIENT_SET AS (
SELECT * FROM <schema_name>.qt_patient_set_collection where result_instance_id = :result_instance_id --and rownum < 20
),
CTE_CODES AS (
    SELECT DISTINCT CONCEPT_CD, NAME_CHAR FROM  <schema_name>.concept_dimension 
    WHERE concept_path like '\ACT\Lab\LOINC\%' or concept_path like '\ACT\SDOH\%'
),
CTE_FACTS AS (
    SELECT A.PATIENT_NUM, START_DATE, END_DATE, A.CONCEPT_CD, A.UNITS_CD, A.VALTYPE_CD,
           A.NVAL_NUM, A.TVAL_CHAR, A.VALUEFLAG_CD, 
           INSTANCE_NUM, MODIFIER_CD, LOCATION_CD, PROVIDER_ID, ENCOUNTER_NUM
    FROM <schema_name>.observation_fact  a 
    JOIN CTE_COHORT_PATIENT_SET c on a.patient_num = c.patient_num
    INNER JOIN CTE_CODES b on a.concept_cd = b.concept_cd
),
CTE_SELECT AS (
SELECT 
     a.PATIENT_NUM as "PATIENT_NUM"
        ,a.start_date as "START_DATE"
        ,a.end_date as "END_DATE"
        --,b.name_char as "NAME_CHAR" --removed creates huge export files
        ,COALESCE(L.ACT_STANDARD_CODE, A.concept_cd ) as "CONCEPT_CD"
        ,a.units_cd as "UNITS_CD"   
        ,DECODE(a.VALTYPE_CD,'N', a.TVAL_CHAR, NULL) AS "OPERATOR"
        ,a.NVAL_NUM as "NUMERIC_VALUE"
        ,DECODE(a.VALTYPE_CD,'N', NULL,substr(A.TVAL_CHAR,1,10)) AS "STRING_VALUE"
--        ,a.TVAL_CHAR AS "STRING_VALUE"
        ,a.VALUEFLAG_CD AS "VALUEFLAG_CD"
        ,a.instance_num as "INSTANCE_NUM"   
        ,a.modifier_cd as "MODIFIER_CD"
        --,m.name_char as "MODIFIER_NAME" -- not harmonized across i2b2s and not included in ENACT
        ,a.location_cd as "LOCATION_CD" 
        --,case v.inout_cd  when 'O' then 'Outpatient' when 'I' then 'Inpatient' when 'E' then 'Emergency' else 'Unknown' end as "ENCOUNTER_TYPE"
        ,a.provider_id as "PROVIDER_ID"
        ,a.encounter_num as "ENCOUNTER_NUM"
        ,'<SITE_NAME>' as "SITE"
    FROM CTE_FACTS  a 
    JOIN CTE_COHORT_PATIENT_SET c on a.patient_num = c.patient_num
    INNER  JOIN CTE_CODES b on a.concept_cd = b.concept_cd -- only labs in lab and sdoh ontologies
    LEFT OUTER JOIN ENACT_MAP_LOCAL_TO_STANDARD l on l.local_concept_cd = a.concept_cd 
    --WHERE CONCEPT_CD LIKE 'LOINC:%' OR CONCEPT_CD LIKE 'SMOKE:%' --all codes including smoking sdoh
    )
select * from cte_select
"""

med_select = """
WITH CTE_COHORT_PATIENT_SET AS (
SELECT * FROM <schema_name>.qt_patient_set_collection where result_instance_id = :result_instance_id --and rownum < 20
),
--select count(*) from (
CTE_CODES AS (
    SELECT DISTINCT CONCEPT_CD, NAME_CHAR FROM  <schema_name>.concept_dimension 
    WHERE concept_path like '\ACT\Medications\MedicationsByAlpha\%'
),
CTE_FACTS AS (
    SELECT A.PATIENT_NUM, START_DATE, END_DATE, A.CONCEPT_CD,
           INSTANCE_NUM, MODIFIER_CD, LOCATION_CD, PROVIDER_ID, ENCOUNTER_NUM
    FROM <schema_name>.observation_fact  a 
    JOIN CTE_COHORT_PATIENT_SET c on a.patient_num = c.patient_num
    INNER JOIN CTE_CODES b on a.concept_cd = b.concept_cd
),
CTE_SELECT AS (
SELECT 
     a.PATIENT_NUM as "PATIENT_NUM"
        ,a.start_date as "START_DATE"
        ,a.start_date as "END_DATE"
        --,b.name_char as "NAME_CHAR" removed creates huge export files
        ,COALESCE(L.ACT_STANDARD_CODE, A.concept_cd ) as "CONCEPT_CD"
        --,a.units_cd as "UNITS_CD"
        --,a.NVAL_NUM as "DOSE_QUANTITY"     -- not harmonized across i2b2s
        ,a.instance_num as "INSTANCE_NUM"   
        ,a.modifier_cd as "MODIFIER_CD"
        --,m.name_char as "MODIFIER_NAME" --not harmonized across i2b2s and ENACT does not use modifiers currently
        ,a.location_cd as "LOCATION_CD" 
        --,case v.inout_cd  when 'O' then 'Outpatient' when 'I' then 'Inpatient' when 'E' then 'Emergency' else 'Unknown' end as "ENCOUNTER_TYPE"
        ,a.provider_id as "PROVIDER"
        ,a.encounter_num as "ENCOUNTER_NUM"
        ,'<SITE_NAME>' as "SITE"
    FROM CTE_FACTS  a 
    JOIN CTE_COHORT_PATIENT_SET c on a.patient_num = c.patient_num
    INNER  JOIN CTE_CODES b on a.concept_cd = b.concept_cd -- only meds in ontologies
    --WHERE CONCEPT_CD LIKE 'RXNORM:%' OR  CONCEPT_CD LIKE 'NDC:%' OR CONCEPT_CD LIKE 'HCPCS:%' --all meds and hcpcs(which is too much but workable for now)
    LEFT OUTER JOIN ENACT_MAP_LOCAL_TO_STANDARD l on l.local_concept_cd = a.concept_cd 
    )
select * from cte_select
"""

vitalsigns_select = """
WITH CTE_COHORT_PATIENT_SET AS (
SELECT * FROM <schema_name>.qt_patient_set_collection where result_instance_id = :result_instance_id --and rownum < 20

),
CTE_CODES AS (
    SELECT DISTINCT CONCEPT_CD, NAME_CHAR FROM  <schema_name>.concept_dimension 
    WHERE concept_path like '\ACT\Lab\LOINC\%' 
),
CTE_FACTS AS (
    SELECT A.PATIENT_NUM, START_DATE, END_DATE, A.CONCEPT_CD, A.UNITS_CD, A.VALTYPE_CD,
           A.NVAL_NUM, A.TVAL_CHAR, A.VALUEFLAG_CD, 
           INSTANCE_NUM, MODIFIER_CD, LOCATION_CD, PROVIDER_ID, ENCOUNTER_NUM
    FROM <schema_name>.observation_fact  a 
    JOIN CTE_COHORT_PATIENT_SET c on a.patient_num = c.patient_num
    INNER JOIN CTE_CODES b on a.concept_cd = b.concept_cd
),
CTE_SELECT AS (
SELECT 
     a.PATIENT_NUM as "PATIENT_NUM"
        ,a.start_date as "START_DATE"
        ,a.end_date as "END_DATE"
        --,b.name_char as "NAME_CHAR" --removed creates huge export files
        ,COALESCE(L.ACT_STANDARD_CODE, A.concept_cd ) as "CONCEPT_CD"
        ,a.units_cd as "UNITS_CD"   
        ,DECODE(a.VALTYPE_CD,'N', a.TVAL_CHAR, NULL) AS "OPERATOR"
        ,a.NVAL_NUM as "NUMERIC_VALUE"
        ,DECODE(a.VALTYPE_CD,'N', NULL,substr(A.TVAL_CHAR,1,10)) AS "STRING_VALUE"
--        ,a.TVAL_CHAR AS "STRING_VALUE"
        ,a.VALUEFLAG_CD AS "VALUEFLAG_CD"
        ,a.instance_num as "INSTANCE_NUM"   
        ,a.modifier_cd as "MODIFIER_CD"
        --,m.name_char as "MODIFIER_NAME" -- not harmonized across i2b2s and not included in ENACT
        ,a.location_cd as "LOCATION_CD" 
        --,case v.inout_cd  when 'O' then 'Outpatient' when 'I' then 'Inpatient' when 'E' then 'Emergency' else 'Unknown' end as "ENCOUNTER_TYPE"
        ,a.provider_id as "PROVIDER_ID"
        ,a.encounter_num as "ENCOUNTER_NUM"
        ,'<SITE_NAME>' as "SITE"
    FROM CTE_FACTS  a 
    JOIN CTE_COHORT_PATIENT_SET c on a.patient_num = c.patient_num
    INNER  JOIN CTE_CODES b on a.concept_cd = b.concept_cd
    LEFT OUTER JOIN ENACT_MAP_LOCAL_TO_STANDARD l on l.local_concept_cd = a.concept_cd 
    --WHERE CONCEPT_CD LIKE 'LOINC:%'

    )
select * from cte_select
"""

vax_select = """
WITH CTE_COHORT_PATIENT_SET AS (
SELECT * FROM <schema_name>.qt_patient_set_collection where result_instance_id = :result_instance_id --and rownum < 20
),
CTE_CODES AS (
    SELECT DISTINCT CONCEPT_CD, NAME_CHAR FROM  <schema_name>.concept_dimension 
    WHERE concept_path like '\ACT\Vaccination\%' 
),
CTE_FACTS AS (
    SELECT A.PATIENT_NUM, START_DATE, END_DATE, A.CONCEPT_CD,
           INSTANCE_NUM, MODIFIER_CD, LOCATION_CD, PROVIDER_ID, ENCOUNTER_NUM
    FROM <schema_name>.observation_fact  a 
    JOIN CTE_COHORT_PATIENT_SET c on a.patient_num = c.patient_num
    INNER JOIN CTE_CODES b on a.concept_cd = b.concept_cd
),
CTE_SELECT AS (
SELECT 
     a.PATIENT_NUM as "PATIENT_NUM"
        ,a.start_date as "START_DATE"
        ,a.end_date as "END_DATE"
        --,b.name_char as "NAME_CHAR" --removed creates huge export files
        ,COALESCE(L.ACT_STANDARD_CODE, A.concept_cd ) as "CONCEPT_CD"
        --,a.units_cd as "UNITS_CD"
        --,a.NVAL_NUM as "DOSE_QUANTITY"     
        ,a.instance_num as "INSTANCE_NUM"   
        ,a.modifier_cd as "MODIFIER_CD"
        --,m.name_char as "MODIFIER_NAME" -- not harmonized across i2b2s and not included in ENACT
        ,a.location_cd as "LOCATION_CD" 
        --,case v.inout_cd  when 'O' then 'Outpatient' when 'I' then 'Inpatient' when 'E' then 'Emergency' else 'Unknown' end as "ENCOUNTER_TYPE"
        ,a.provider_id as "PROVIDER"
        ,a.encounter_num as "ENCOUNTER_NUM"
        ,'<SITE_NAME>' as "SITE"
    FROM CTE_FACTS  a 
    JOIN CTE_COHORT_PATIENT_SET c on a.patient_num = c.patient_num
    INNER  JOIN CTE_CODES b on a.concept_cd = b.concept_cd
    LEFT OUTER JOIN ENACT_MAP_LOCAL_TO_STANDARD l on l.local_concept_cd = a.concept_cd 

)
select * from CTE_SELECT
"""


zip_select = """
WITH CTE_COHORT_PATIENT_SET AS (
SELECT * FROM <schema_name>.qt_patient_set_collection where result_instance_id = :result_instance_id --and rownum < 20
)
SELECT 
        a.PATIENT_NUM as "PATIENT_NUM"
        ,a.start_date as "START_DATE"
        ,a.end_date as "END_DATE"
        --,b.name_char as "NAME_CHAR" --removed creates huge export files
        ,A.concept_cd as "CONCEPT_CD"
        --,a.units_cd as "UNITS_CD"
        --,a.NVAL_NUM as "DOSE_QUANTITY"     
        ,a.instance_num as "INSTANCE_NUM"   
        ,a.modifier_cd as "MODIFIER_CD"
        --,m.name_char as "MODIFIER_NAME" -- not harmonized across i2b2s and not included in ENACT
        ,a.location_cd as "LOCATION_CD" 
        --,case v.inout_cd  when 'O' then 'Outpatient' when 'I' then 'Inpatient' when 'E' then 'Emergency' else 'Unknown' end as "ENCOUNTER_TYPE"
        ,a.provider_id as "PROVIDER"
        ,a.encounter_num as "ENCOUNTER_NUM"
        ,'<SITE_NAME>' as "SITE"
    FROM  <schema_name>.observation_fact  a 
    JOIN CTE_COHORT_PATIENT_SET c on a.patient_num = c.patient_num
    WHERE CONCEPT_CD LIKE 'DEM|ZIP%' 
"""


def export_domain(domain_name, select_name):

    cnt = 0
    batch_file_name = project_name + '_' + domain_name + '_' + export_version
    print(batch_file_name)
    print(select_name)
   
    params = {"result_instance_id": result_instance_id}
    query = text(select_name)

    for data in pd.read_sql(query, engine, params=params, chunksize=50000):
        cnt = cnt + 1
        #print('Data Frame Batch ', cnt)
        if cnt == 1:
            data.to_csv(batch_file_name+'.csv', mode='w', index=False, header=True, quotechar='"', escapechar='"', quoting=csv.QUOTE_ALL)
        else:
            data.to_csv(batch_file_name+'.csv', mode='a', index=False, header=False, quotechar='"', escapechar='"', quoting=csv.QUOTE_ALL)
    print( 'Finished', domain_name)
    print(time.strftime("%c"))
    return


# Execute select statements and export to csv files
def main():
    print( 'Start Time')
    print(time.strftime("%c"))
    print("is this variable global ",pw) #global
    export_domain('dem', dem_select)
    export_domain('enc', enc_select)
    export_domain('diagnosis', dx_select)
    export_domain('procedures', px_select)
    export_domain('meds', med_select)
    export_domain('labs', lab_select)
    export_domain('vitalsigns', vitalsigns_select)
    export_domain('vax', vax_select)
    export_domain('zip', zip_select)

if __name__ == "__main__":
    print( 'Number of arguments:', len(sys.argv), 'arguments.')
    print( 'Argument List:', str(sys.argv))

    # set pparameters
    user_name = sys.argv[1]
    pw = sys.argv[2]
    project_name = sys.argv[3]
    export_version = sys.argv[4]
    result_instance_id = sys.argv[5]

    # Currently the SQL seems to be RDB agnostic so separate scripts does not seem necessary
    # Oracle connection
    oracle_connection_string = 'oracle+cx_oracle://{username}:{password}@{hostname}:{port}/{database}'

    '''
    connection_url = oracle_connection_string.format(
        username=user_name,
        password=pw, 
        hostname=hostname,
        port='1521',
        database=database,
    )
    '''
    # MSSQL connection
    # Construct the connection URL
    
    connection_url = sa.engine.URL.create(
        "mssql+pyodbc",
        username=user_name,
        password=pw,
        host=server_name,
        database=database_name,
        query={"driver": driver_name}
    )
    
    
    # Create the engine
    engine = create_engine(connection_url)
    
    main()

exit(0)


    
    

