-- all tasks should be created in one schema

create or replace table raw.raw_customer (
    customer_id varchar,
    salutation varchar,
    first_name varchar,
    last_name varchar,
    birth_day varchar,
    birth_month varchar,
    birth_year varchar,
    birth_country varchar,
    email_address varchar,
    is_active varchar
);

create or replace table stg.stg_customer (
    customer_id varchar,
    salutation varchar,
    first_name varchar,
    last_name varchar,
    birth_day varchar,
    birth_month varchar,
    birth_year varchar,
    birth_country varchar,
    email_address varchar,
    is_active varchar
);



create or replace table transformed.dim_customer (
customer_dim_key number autoincrement,
customer_id varchar(18),
salutation varchar(10),
first_name varchar(20),
last_name varchar(30),
birth_day number,
birth_month number,
birth_year number,
birth_country varchar(20),
email_address varchar(50),
added_timestamp timestamp default current_timestamp() ,
updated_timestamp timestamp default current_timestamp() ,
is_active varchar(1)
);


create or replace stream stg.stg_customer_stm on table stg.stg_customer;
create or replace stream raw.raw_customer_stm on table raw.raw_customer;

create or replace pipe stg.stg_customer_pipe 
auto_ingest=true 
as
copy into stg.stg_customer
from @landing/landing/customer
file_format = (type = 'CSV', skip_header=1, error_on_column_count_mismatch=false);




--create customer pipeline    
create or replace task transformed.pause_pipe_customer
  warehouse = ayo_warehouse 
  schedule  = '1 minute'
when
  system$stream_has_data('stg.stg_customer_stm')
as
  alter pipe stg.stg_customer_pipe set pipe_execution_paused = true;
  
  
create or replace task transformed.customer_raw_tsk
  warehouse = ayo_warehouse 
  after transformed.pause_pipe_customer
when
  system$stream_has_data('stg.stg_customer_stm')
as
merge into raw.raw_customer 
using stg.stg_customer_stm on
raw_customer.customer_id = stg_customer_stm.customer_id
when matched
--covers updates and deletes
 then update set 
    raw_customer.salutation = stg_customer_stm.salutation,
    raw_customer.first_name = stg_customer_stm.first_name,
    raw_customer.last_name = stg_customer_stm.last_name,
    raw_customer.birth_day = stg_customer_stm.birth_day,
    raw_customer.birth_month = stg_customer_stm.birth_month,
    raw_customer.birth_year = stg_customer_stm.birth_year,
    raw_customer.birth_country = stg_customer_stm.birth_country,
    raw_customer.email_address = stg_customer_stm.email_address,
    raw_customer.is_active = stg_customer_stm.is_active
when not matched then 
insert (
  customer_id ,
  salutation ,
  first_name ,
  last_name ,
  birth_day ,
  birth_month ,
  birth_year ,
  birth_country ,
  email_address,
  is_active) 
values (
  stg_customer_stm.customer_id ,
  stg_customer_stm.salutation ,
  stg_customer_stm.first_name ,
  stg_customer_stm.last_name ,
  stg_customer_stm.birth_day ,
  stg_customer_stm.birth_month ,
  stg_customer_stm.birth_year ,
  stg_customer_stm.birth_country ,
  stg_customer_stm.email_address,
  stg_customer_stm.is_active);
  
  
  
create or replace task transformed.dim_customer_tsk
    warehouse = ayo_warehouse 
after transformed.customer_raw_tsk
when
  system$stream_has_data('raw.raw_customer_stm')
as
  merge into transformed.dim_customer 
  using raw.raw_customer_stm 
  on
  dim_customer.customer_id = raw_customer_stm.customer_id 
when matched
  then update set 
      dim_customer.salutation = raw_customer_stm.salutation,
      dim_customer.first_name = raw_customer_stm.first_name,
      dim_customer.last_name = raw_customer_stm.last_name,
      dim_customer.birth_day = raw_customer_stm.birth_day,
      dim_customer.birth_month = raw_customer_stm.birth_month,
      dim_customer.birth_year = raw_customer_stm.birth_year,
      dim_customer.birth_country = raw_customer_stm.birth_country,
      dim_customer.email_address = raw_customer_stm.email_address,
      dim_customer.is_active = raw_customer_stm.is_active,
      dim_customer.updated_timestamp = current_timestamp()
when not matched 
then 
  insert (
    customer_id ,
    salutation ,
    first_name ,
    last_name ,
    birth_day ,
    birth_month ,
    birth_year ,
    birth_country ,
    email_address,
    is_active
  ) 
  values (
    raw_customer_stm.customer_id ,
    raw_customer_stm.salutation ,
    raw_customer_stm.first_name ,
    raw_customer_stm.last_name ,
    raw_customer_stm.birth_day ,
    raw_customer_stm.birth_month ,
    raw_customer_stm.birth_year ,
    raw_customer_stm.birth_country ,
    raw_customer_stm.email_address,
    raw_customer_stm.is_active
   );
  
  
create or replace task transformed.truncate_staging_table_customer
  warehouse = ayo_warehouse 
  after transformed.dim_customer_tsk
as
  truncate table if exists stg.stg_customer;  
  

create or replace task transformed.play_pipe_customer
  warehouse = ayo_warehouse 
  after transformed.truncate_staging_table
as
  select system$pipe_force_resume('stg.stg_customer_pipe');
  
  
alter task pause_pipe resume;
alter task pause_pipe suspend;


alter task customer_raw_tsk resume;
alter task dim_customer_tsk resume;
alter task truncate_staging_table_customer resume;
alter task play_pipe_customer resume;


alter task customer_raw_tsk suspend;
alter task dim_customer_tsk suspend;
alter task truncate_staging_table_customer suspend;
alter task play_pipe_customer suspend;


select *  from table(information_schema.task_history()) 
where name in ('PAUSE_PIPE_CUSTOMER','CUSTOMER_RAW_TSK','DIM_CUSTOMER_TSK','TRUNCATE_STAGING_TABLE_CUSTOMER','PLAY_PIPE_CUSTOMER')
--and scheduled_time = current_date()
order by scheduled_time desc;


list @landing;
show tasks;
show streams;
show pipes;

select system$pipe_status('stg.stg_customer_pipe');


select * from stg.stg_customer; --stage table
select * from stg.stg_customer_stm; --raw stream
select * from raw.raw_customer; -- raw table
select * from raw.raw_customer_stm; --transformed stream
select * from transformed.dim_customer; --transformed table



