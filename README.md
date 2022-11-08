# End-to-End-ETL-using-snowflake



## Tools
* SQS
* snowflake pipe
* snowflake stream
* SQL
* snowflake database



## create the required objects

#### run the following script to create the required objects in the snowflake environment

### create warehouse
```bash
create warehouse ayo_warehouse;
```

### create database
```bash
create database demo;
```

### create schemas
```bash
create or replace schema raw;
create or replace schema stg;
create or replace schema transformed;
```

### create file format
```bash
create or replace file format csv
type = 'csv' 
compression = 'auto' 
field_delimiter = ',' 
record_delimiter = '\n' 
skip_header = 1
field_optionally_enclosed_by = '\042' 
null_if = ('\\N');
```

### create storage integration

