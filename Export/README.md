Instructions
1. Find the query using the name and shrine query id in the local i2b2
2. Rerun the query/queries with Patient Set
3. Capture the Patient Set number from the qt_patient_set_collection
4. Use that patient_set number as a parameter to the export script
5. Edit export script
	a. Add project name to export
	b. add export version
	c. edit database connection information
5. If your facts have non-standard local codes 
	a. build a local to standard code map by running 
	b. modify the script to translate the codes to standard codes when possible.
	   search the export script for "MAP LOCAL CODES"
