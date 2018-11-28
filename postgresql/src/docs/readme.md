#Cutover Documentation

##Overview
Cutover preparation is performed offline.

The basic process for cutover is supported by:

* **database views** to identify the data in scope for cutover, i.e. ACTIVE measures and regulations

* **stored procedures**:
    * **end date** measures for a given:
        - regulation
        - measure type
        - (specify the justification regulation and date)
        
    * **clone** (and *end date*) measures for a given:
        - regulation
        - measure type
        - (specify the generating (&justification) regulation, start date)
    
    * **XML generation** from the database
        - Standalone Java program
        - Uses records created in the _oplog tables
        - Generates XML from the data returned in the queries
        - Quite manual process but only needs to be simple
        - XML validated using xmllint
        
        
        
        
 