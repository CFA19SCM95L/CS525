Project Name: Record Manager


Chen En Lee 	(A20435695)
Jingeun Jung	(A20437470)



How to run:

1) Go to the project root.

2) Type "make test" to create test file "test_expr"

3) Type "make defaultTest" to create test file "test"

4) Type ./test_expr to run the test

5) Type ./test to run the test

6) Type "make clean" to delete the test file


1. TABLE AND MANAGER FUNCTIONS
______________________________


initRecordManager:
Initializes the record manager.

shutdownRecordManager:
Shutsdown the record manager and de-allocates all the resources allocated to the record manager.

createTable:
Opens the table having name specified by the parameter 'name.
It initializes all the values of the table and also sets the attributes (name, datatype and size) of the table.
It then creates a page file, opens that page file, writes the block containing the table in the page file and closes the page file.

openTable:
Creates a table with name as specified in the parameter 'name' in the schema specified in the parameter 'schema'.

closeTable:
Closes the table as pointed by the parameter 'rel'.
Before shutting the buffer pool, the buffer manager writes the changes made to the table in the page file.

deleteTable:
Deletes the table with name specified by the parameter 'name'.
It calls the Storage Manager's function destroyPageFile(...).
destroyPageFile(...) function deletes the page from disk and de-allocates ane memory space allocated for that mechanism.

getNumTuples:
Returns the number of tuples in the table referenced by parameter 'rel'.
It returns the value of the variable which is defined in our custom data structure which we use for storing table's meta-data.



2. RECORD FUNCTIONS
___________________

insertRecord:
Inserts a record in the table and updates the 'record' parameter with the record ID passed in the insertRecord() function.

deleteRecord:
Deletes a record having Record ID 'id' passed through the parameter from the table referenced by the parameter 'rel'.


updateRecord:
Updates a record referenced by the parameter "record" in the table referenced by the parameter "rel".


getRecord:
Retrieves a record having Record ID "id" passed in the paramater in the table referenced by "rel" which is also passed in the parameter. 
The result record is stored in the location referenced by the parameter "record".


3. SCAN FUNCTIONS
_________________

The Scan related functions are used to retreieve all tuples from a table that fulfill a certain condition (represented as an Expr). 

startScan()
- Initialized all related variable to scan function.
- If condition are NULL, we return 'RC_ERROR'

next()
- We will scan every record. With reading record it checks the condition against the scan passed as parameter. 

closeScan() 
- This function will close the scan operation and reset all information in RM_SCAN_MGMT


4. SCHEMA FUNCTIONS
___________________

These functions are used to return the size in bytes of records for a given schema and create a new schema. 

getRecordSize()
- This function returns the size of a record based on data type in the specified schema.
- The value of the variable 'size' is the size of the record.

createSchema()
- This method creates a new schema with the specified parameters in memory(allocating)
- We finally set te schema's parameters to the parameters passed in the createSchema(...)

freeSchema()
- This function removes the schema specified by the parameter 'schema' from the memory.





5. ATTRIBUTE FUNCTIONS
______________________

These functions are used to get or set the attribute values of a record and create a new record for a given schema. 

createRecord()
- This function creates a new record in the schema.
- We allocate proper memory space to the new record and give memory space for the data of the record which is the record size.

freeRecord()
- Checks if the record is free. If the record is free then it returns record free
- If not free then it frees the record by removing the data from the record.

getAttr()
- Return value of attribute pointed by attrnum in value(integer or string or float type)

setAttr() & strRepInt()
- Set the attribute value in the record in the specified schema. 
- the attribute in a record is set to the specified value(integer or string or float type)

getAtrOffsetInRec()
- returns offset/string posing of perticular attribute
- Atrnum is off set that we are looking for


