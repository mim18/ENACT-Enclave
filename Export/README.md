Instructions
1. Find the query using the given  name and shrine query id in the local i2b2
2. Re-run the query/queries with "Patient Set" option turned on
3. Capture the Patient Set number from the qt_patient_set_collection
4. Create a nonstandard code map. If your observation_fact and concept_dimension  table contands all standard prefixes the table will be empty (it may have a few random top level codes)  
	- read through script and modify where appropriate. Check the fact prefixes and make sure they match your facts.
	- build a local to standard code map by running ENACT_MAP_LOCAL_TO_STANDARD_MSSQL.sql
5. Edit export script
	- Substitute <schema_name> to your schema name.
	- Edit your database url
    - Substitute '<SITE_NAME> to your site name
7. Use the patient_set number as a parameter to the export script
	- python ExportEnclaveEHRMSSQL.py username password projectname exportnumber patientsetnumber

