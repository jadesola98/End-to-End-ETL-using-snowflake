# End-to-End-ETL-using-snowflake

The aim of this project is to understand the ETL workflow in snowflake. In the project, data in csv files are put into s3, an sqs publishes the event to the external stage which tiggers a pipe to copy this data into a stage table and a stream created on the table, the series of tasks created to put the data in the destination are triggered after a minute after which the data is persisted in the destination table.


## Tools
* SQS
* snowflake pipe
* snowflake stream
* SQL
* snowflake database



## create the required objects

#### run the following commands to create the required objects in the snowflake environment

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
#### setup storage integrations to allow Snowflake to read data from and write data to an Amazon S3 bucket. The steps involved in setting up a storage integration can be found in the following link https://docs.snowflake.com/en/user-guide/data-load-s3-config-storage-integration.html


### create a table for each entity in each schema

### create a snowpipe that reads data from the stage area to the raw tables

### create a stream on the raw table

### create a stream on the stage table

## To achieve the desired workflow some tasks are created.
* the first task is created to pause the pipe, this task is the parent task and is triggered after data is put in the s3 bucket,reflects in the external stage and is put into the stage table by the pipe.
* the second task, a task that reads from the stage stream is immediately triggered(because it is the first child task i.e it depends on the parent task) to put data into the raw table based on the conditions defined in the task
* the third task carries out the same action as the previous task but is only triggered after the previous task and reads from the raw stream to the transformed table
* the fourth task is triggered after the third task and it truncates the staging table to prepare it for the next run
* the last task is triggered after the fourth task and it resumes the pipe in other to extract the new data in the external stage into the staging table.

#### N.B:start child task before parent task
####     cross schema task linking is not possible

