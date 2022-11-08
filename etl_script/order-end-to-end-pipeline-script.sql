create or replace table raw.raw_order (
    order_date varchar,
    order_time varchar,
    item_id varchar,
    item_desc varchar,
    customer_id varchar,
    salutation varchar,
    first_name varchar,
    last_name varchar,
    store_id varchar,
    store_name varchar,
    order_quantity varchar,
    sale_price varchar,
    disount_amt varchar,
    coupon_amt varchar,
    net_paid varchar,
    net_paid_tax varchar,
    net_profit varchar
);






create or replace table stg.stg_order (
    order_date varchar,
    order_time varchar,
    item_id varchar,
    item_desc varchar,
    customer_id varchar,
    salutation varchar,
    first_name varchar,
    last_name varchar,
    store_id varchar,
    store_name varchar,
    order_quantity varchar,
    sale_price varchar,
    disount_amt varchar,
    coupon_amt varchar,
    net_paid varchar,
    net_paid_tax varchar,
    net_profit varchar
);



    create or replace table transformed.fact_order (
      order_fact_key number autoincrement,
      order_date date,
      customer_dim_key number,
      item_dim_key number,
      order_count number,
      order_quantity number,
      sale_price number(20,2),
      disount_amt number(20,2),
      coupon_amt number(20,2),
      net_paid number(20,2),
      net_paid_tax number(20,2),
      net_profit number(20,2)
    );
    


--create streams 
create or replace stream stg.stg_order_stm on table stg.stg_order;
create or replace stream raw.fact_order_stm on table raw.raw_order;



use schema stg;
create or replace pipe stg.stg_order_pipe 
auto_ingest=true 
as
copy into stg.stg_order
from @landing/landing/order
file_format = (type = 'CSV', skip_header=1, error_on_column_count_mismatch=false);







-- task to insert and update record into order table

        
        
        


-- s3 - stage area - stage table(using pipe) - stream table(using stream) - raw table(task) - stream table(using stream) - dim table(using task)
--create a task to pause pipe when stream has data
use schema transformed;
create or replace task transformed.pause_pipe_order
  warehouse = ayo_warehouse 
  schedule  = '1 minute'
when
  system$stream_has_data('stg.stg_order_stm')
as
  alter pipe stg.stg_order_pipe set pipe_execution_paused = true;
  


create or replace task transformed.raw_order_tsk
  warehouse = ayo_warehouse 
  after transformed.pause_pipe_order
when
  system$stream_has_data('stg.stg_order_stm')
as
merge into raw.raw_order
using stg.stg_order_stm on
raw_order.order_date = stg_order_stm.order_date and 
raw_order.order_time = stg_order_stm.order_time and 
raw_order.item_id = stg_order_stm.item_id and
raw_order.item_desc = stg_order_stm.item_desc 
when matched 
 then update set 
    raw_order.customer_id = stg_order_stm.customer_id,
    raw_order.salutation = stg_order_stm.salutation,
    raw_order.first_name = stg_order_stm.first_name,
    raw_order.last_name = stg_order_stm.last_name,
    raw_order.store_id = stg_order_stm.store_id,
    raw_order.store_name = stg_order_stm.store_name,
    raw_order.order_quantity = stg_order_stm.order_quantity,
    raw_order.sale_price = stg_order_stm.sale_price,
    raw_order.disount_amt = stg_order_stm.disount_amt,
    raw_order.coupon_amt = stg_order_stm.coupon_amt,
    raw_order.net_paid = stg_order_stm.net_paid,
    raw_order.net_paid_tax = stg_order_stm.net_paid_tax,
    raw_order.net_profit = stg_order_stm.net_profit
  when not matched then 
  insert (
    order_date ,
    order_time ,
    item_id ,
    item_desc ,
    customer_id ,
    salutation ,
    first_name ,
    last_name ,
    store_id ,
    store_name ,
    order_quantity ,
    sale_price ,
    disount_amt ,
    coupon_amt ,
    net_paid ,
    net_paid_tax ,
    net_profit ) 
  values (
    stg_order_stm.order_date ,
    stg_order_stm.order_time ,
    stg_order_stm.item_id ,
    stg_order_stm.item_desc ,
    stg_order_stm.customer_id ,
    stg_order_stm.salutation ,
    stg_order_stm.first_name ,
    stg_order_stm.last_name ,
    stg_order_stm.store_id ,
    stg_order_stm.store_name ,
    stg_order_stm.order_quantity ,
    stg_order_stm.sale_price ,
    stg_order_stm.disount_amt ,
    stg_order_stm.coupon_amt ,
    stg_order_stm.net_paid ,
    stg_order_stm.net_paid_tax ,
    stg_order_stm.net_profit );
    
 


create or replace task transformed.fact_order_tsk
warehouse = ayo_warehouse 
after transformed.raw_order_tsk
when
  system$stream_has_data('raw.fact_order_stm')
as
insert overwrite into transformed.fact_order (
order_date,
customer_dim_key ,
item_dim_key ,
order_count,
order_quantity ,
sale_price ,
disount_amt ,
coupon_amt ,
net_paid ,
net_paid_tax ,
net_profit) 
select 
      ro.order_date,
      dc.customer_dim_key ,
      di.item_dim_key,
      count(1) as order_count,
      sum(ro.order_quantity) ,
      sum(ro.sale_price) ,
      sum(ro.disount_amt) ,
      sum(ro.coupon_amt) ,
      sum(ro.net_paid) ,
      sum(ro.net_paid_tax),
      sum(ro.net_profit)  
  from raw.raw_order ro 
    join dim_customer dc on dc.customer_id = ro.customer_id
    join dim_item di on di.item_id = ro.item_id 
    --and di.item_desc = ro.item_desc 
    and di.end_date is null
    group by 
        ro.order_date,
        dc.customer_dim_key ,
        di.item_dim_key
        order by ro.order_date; 
        
        
 create or replace task transformed.truncate_staging_table_order
  warehouse = ayo_warehouse 
  after transformed.fact_order_tsk
as
  truncate table if exists stg.stg_order; 
         
  

create or replace task transformed.play_pipe_order
  warehouse = ayo_warehouse 
  after transformed.truncate_staging_table_order
when not
  system$stream_has_data('stg.stg_order_stm')
as
  select system$pipe_force_resume('stg.stg_order_pipe');
  
  
  
  
alter task pause_pipe_order resume;
alter task pause_pipe_order suspend;

alter task raw_order_tsk resume;
alter task fact_order_tsk resume;
alter task truncate_staging_table_order resume;
alter task play_pipe_order resume;

alter task raw_order_tsk suspend;
alter task fact_order_tsk suspend;
alter task truncate_staging_table_order suspend;
alter task play_pipe_order suspend;


select *  from table(information_schema.task_history()) 
where name in ('PAUSE_PIPE_ORDER','RAW_ORDER_TSK','FACT_ORDER_TSK','TRUNCATE_STAGING_TABLE_ORDER','PLAY_PIPE_ORDER')
order by scheduled_time desc;

show tasks;
show streams;



select * from stg.stg_order; --stage table
select * from stg.stg_order_stm; --raw stream
select * from raw.raw_order; -- raw table
select * from raw.fact_order_stm; --transformed stream
select * from transformed.fact_order; --transformed table

list @landing;

TRUNCATE table stg.stg_customer;

--resume pipe
select system$pipe_force_resume('stg.stg_order_pipe')


--check status of pipe
select system$pipe_status('stg.stg_order_pipe'); 




/****
create or replace table stg.control_order (
    order_date varchar,
    order_time varchar,
    item_id varchar,
    item_desc varchar,
    customer_id varchar,
    salutation varchar,
    first_name varchar,
    last_name varchar,
    store_id varchar,
    store_name varchar,
    order_quantity varchar,
    sale_price varchar,
    disount_amt varchar,
    coupon_amt varchar,
    net_paid varchar,
    net_paid_tax varchar,
    net_profit varchar
);

create or replace task transformed.truncate_control_table_order
  warehouse = ayo_warehouse 
  after transformed.fact_order_tsk
as
  truncate table if exists stg.control_order; 
  
  
  
insert into transformed.fact_order (
order_date,
customer_dim_key ,
item_dim_key ,
order_count,
order_quantity ,
sale_price ,
disount_amt ,
coupon_amt ,
net_paid ,
net_paid_tax ,
net_profit) 
select 
      ro.order_date,
      dc.customer_dim_key ,
      di.item_dim_key,
      count(1) as order_count,
      sum(ro.order_quantity) ,
      sum(ro.sale_price) ,
      sum(ro.disount_amt) ,
      sum(ro.coupon_amt) ,
      sum(ro.net_paid) ,
      sum(ro.net_paid_tax),
      sum(ro.net_profit)  
  from raw.raw_order ro 
    join dim_customer dc on dc.customer_id = ro.customer_id
    join dim_item di on di.item_id = ro.item_id 
    --and di.item_desc = ro.item_desc 
    and di.end_date is null
    group by 
        ro.order_date,
        dc.customer_dim_key,
        di.item_dim_key
        order by ro.order_date; 
        
select * from fact_order;        
        
select
      ro.order_date,
      dc.customer_dim_key,
      di.item_dim_key,
      sum(ro.order_quantity) ,
      sum(ro.sale_price) ,
      sum(ro.disount_amt) ,
      sum(ro.coupon_amt) ,
      sum(ro.net_paid) ,
      sum(ro.net_paid_tax),
      sum(ro.net_profit)  
  from raw.raw_order ro 
    join dim_customer dc on dc.customer_id = ro.customer_id
    join dim_item di on di.item_id = ro.item_id 
    --and di.item_desc = ro.item_desc 
    and di.end_date is null
    group by
        ro.order_date,    
        dc.customer_dim_key,
        di.item_dim_key
        
        
select * from dim_customer
where customer_id in
(select customer_id from raw.raw_order)

select * from dim_item
where item_id in
(select item_id from raw.raw_order)


select * from dim_item
select * from raw.raw_order

update raw.raw_order
set customer_id = 'AAAAAAAALKBJCPAA'
where first_name = 'Sarah'

update raw.raw_order
set item_id = 'AAAAAAAAABHKEAAA'
where first_name = 'Sarah'

update dim_item
set end_date = null
where item_id = 'AAAAAAAAABHKEAAA';

truncate table fact_order;
/***