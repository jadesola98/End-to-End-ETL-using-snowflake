--create tables
--create file format
-- create storage integration
--create staging area
--create task for initial load using copy statement



--N.B;start child task before parent task

-- s3 - stage area - stage table(using pipe) - stream table(using stream) - raw table(task) - stream table(using stream) - dim table(using task)

--created all the tasks in transformed schema
--created pipe in staging schema
--conditions satisfied : insert, update, delete, picks the latest change for the same id in a stream


create or replace table raw.raw_item (
        item_id varchar,
        item_desc varchar,
        start_date varchar,
        end_date varchar,
        price varchar,
        item_class varchar,
        item_CATEGORY varchar,
        is_active varchar
);


create or replace table stg.stg_item (
        item_id varchar,
        item_desc varchar,
        start_date varchar,
        end_date varchar,
        price varchar,
        item_class varchar,
        item_CATEGORY varchar,
        is_active varchar
);


  create or replace table transformed.dim_item (
        item_dim_key number autoincrement,
        item_id varchar(16),
        item_desc varchar,
        start_date date,
        end_date date,
        price number(7,2),
        item_class varchar(50),
        item_category varchar(50),
        added_timestamp timestamp default current_timestamp() ,
        updated_timestamp timestamp default current_timestamp() ,
        is_active varchar(1)
    );
    
    
create or replace stream stg.stg_item_stm on table stg.stg_item;
create or replace stream raw.raw_item_stm on table raw.raw_item;

use schema stg;
create or replace pipe stg.stg_item_pipe 
auto_ingest=true 
as
copy into stg.stg_item
from @landing/landing/item
file_format = (type = 'CSV', skip_header=1, error_on_column_count_mismatch=false);


--create item pipeline 
use schema transformed;

create or replace task transformed.pause_pipe_item
  warehouse = ayo_warehouse 
  schedule  = '1 minute'
when
  system$stream_has_data('stg.stg_item_stm')
as
  alter pipe stg.stg_customer_pipe set pipe_execution_paused = true;



create or replace task transformed.item_raw_tsk
  warehouse = ayo_warehouse 
  after transformed.pause_pipe_item
when
  system$stream_has_data('stg.stg_item_stm')
as
merge into raw.raw_item 
using 
(select item_id, item_desc, start_date, end_date, price, item_class, item_category, is_active from 
(select *, row_number() over(partition by item_id order by start_date desc) as rownum from stg.stg_item_stm) a
where rownum = 1) as stg_item_stm
on
raw_item.item_id = stg_item_stm.item_id
when matched 
 then update set
    raw_item.item_desc = stg_item_stm.item_desc, 
    raw_item.start_date = stg_item_stm.start_date,
    raw_item.end_date = stg_item_stm.end_date,
    raw_item.price = stg_item_stm.price,
    raw_item.item_class = stg_item_stm.item_class,
    raw_item.item_category = stg_item_stm.item_category,
    raw_item.is_active = stg_item_stm.is_active
when not matched then 
insert (
  item_id,
  item_desc,
  start_date,
  end_date,
  price,
  item_class,
  item_category,
  is_active) 
values (
  stg_item_stm.item_id,
  stg_item_stm.item_desc,
  stg_item_stm.start_date,
  stg_item_stm.end_date,
  stg_item_stm.price,
  stg_item_stm.item_class,
  stg_item_stm.item_category,
  stg_item_stm.is_active);





create or replace task transformed.dim_item_tsk
  warehouse = ayo_warehouse 
  after transformed.item_raw_tsk
when
    system$stream_has_data('raw.raw_item_stm')
as
  merge into transformed.dim_item
  using raw.raw_item_stm 
  on
  dim_item.item_id = raw_item_stm.item_id
when matched 
  then update set
      dim_item.item_desc = raw_item_stm.item_desc,
      dim_item.start_date = raw_item_stm.start_date,
      dim_item.end_date = raw_item_stm.end_date,
      dim_item.price = raw_item_stm.price,
      dim_item.item_class = raw_item_stm.item_class,
      dim_item.item_category = raw_item_stm.item_category,
      dim_item.is_active = raw_item_stm.is_active,
      dim_item.updated_timestamp = current_timestamp()
when not matched 
then 
  insert (
    item_id,
    item_desc,
    start_date,
    end_date,
    price,
    item_class,
    item_category,
    is_active
  ) 
  values (
    raw_item_stm.item_id,
    raw_item_stm.item_desc,
    raw_item_stm.start_date,
    raw_item_stm.end_date,
    raw_item_stm.price,
    raw_item_stm.item_class,
    raw_item_stm.item_category,
    raw_item_stm.is_active);
    
    
    
create or replace task transformed.truncate_staging_table_item
  warehouse = ayo_warehouse 
  after transformed.dim_item_tsk
as
  truncate table if exists stg.stg_item;  
  

create or replace task transformed.play_pipe_item
  warehouse = ayo_warehouse 
  after transformed.truncate_staging_table_item
as
  select system$pipe_force_resume('stg.stg_item_pipe');
    
    
    
    
  

alter task pause_pipe_item resume;
alter task pause_pipe_item suspend;


alter task item_raw_tsk resume;
alter task dim_item_tsk resume;
alter task truncate_staging_table_item resume;
alter task play_pipe_item resume;


alter task item_raw_tsk suspend;
alter task dim_item_tsk suspend;
alter task truncate_staging_table_item suspend;
alter task play_pipe_item suspend;


select *  from table(information_schema.task_history()) 
where name in ('PAUSE_PIPE_ITEM','ITEM_RAW_TSK','DIM_ITEM_TSK_ITEM','TRUNCATE_STAGING_TABLE_ITEM','PLAY_PIPE_ITEM')
--and scheduled_time = current_date()
order by scheduled_time desc;


use schema stg;
list @landing;
use schema transformed;


show tasks;
show streams;
show pipes;

select system$pipe_status('stg.stg_item_pipe');


select * from stg.stg_item; --stage table
select * from stg.stg_item_stm; --raw stream
select * from raw.raw_item; -- raw table
select * from raw.dim_item_stm; --transformed stream
select * from transformed.dim_item; --transformed table
