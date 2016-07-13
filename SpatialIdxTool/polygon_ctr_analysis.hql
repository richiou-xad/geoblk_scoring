use can;
add file /home/canliang/nise_spatial_index_reader;
add file /home/canliang/landuse_SpatialIdx;


set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=10000;
set hive.mapred.supports.subdirectories=true;
set mapred.input.dir.recursive=true;
set hive.auto.convert.join=false;

drop table if exists temp;

create table temp AS
select
D.latitude as latitude, 
D.longitude as longitude, 
D.zipcode as zipcode, 
C.request_id as request_id, 
cast(get_json_object(C.info, '$.ctr') as float) as ctr, 
cast(get_json_object(C.info, '$.sar') as float) as sar, 
C.country as country,
C.dt as dt, 
C.hour as hour
from
(
select
A.request_id AS request_id,
B.info AS info,
A.country as country,
A.dt as dt,
A.hour as hour
from 
(  
  select
    header.request_id AS request_id,
    country as country,
    dt as dt,
    hour as hour,
    prod_type as prod_type
  from default.ex_adtracking_hrly 
  where country='${hiveconf:country}' and dt='${hiveconf:compare_date}'  and action_type='AD_IMPRESSION'
) A
JOIN
(
  select 
    header.request_id AS request_id,
    country as country,
    dt as dt,
    hour as hour,
    prod_type as prod_type,
    science_info.info as info
  from default.ex_addetails_hrly
    where country='${hiveconf:country}' and dt='${hiveconf:compare_date}' and science_info is not null and science_info.info is not null
) B
on A.country=B.country and A.dt=B.dt and A.hour=B.hour and A.prod_type=B.prod_type and A.request_id=B.request_id
) C
inner join
(
  select
    header.request_id AS request_id,
    sl_location.location.latitude AS latitude,
    sl_location.location.longitude as longitude,
    sl_location.location.zip as zipcode,
    prod_type AS prod_type
  from default.ex_adrequest_hrly_2
  where country='${hiveconf:country}' and dt='${hiveconf:compare_date}' and fill='fill' and location_score='95+' and sl_location is not null and sl_location.location is not null
) D
on C.request_id=D.request_id;


drop table if exists zipmap;

create table zipmap as
select
 A.dt as dt,
 A.hour as hr,
 A.zipcode as zipcode,
 A.latitude as latitude,
 A.longitude as longitude,
 B.timezone as timezone,
 A.request_id as request_id,
 A.ctr as ctr,
 A.sar as sar,
 A.country as country
 from
  (
    select zipcode, timezone
    from
    zip_timezone_map where timezone is not null
 ) B
 join
 (
    select dt, hour, latitude, longitude, zipcode, request_id, ctr, sar, country
    from temp
    where dt='${hiveconf:compare_date}'
 ) A
 on A.zipcode=B.zipcode;

create table if not exists spatial_tracking_hrly
(
ctr double,
sar double,
cat string
)
partitioned by (country string, dt string,  hour string);


set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

insert into table spatial_tracking_hrly
partition(country, dt, hour)
select B.ctr, B.sar, B.cat, B.country, B.dt, B.hour
from
(
select
transform(
A.latitude, 
A.longitude, 
A.request_id,
A.ctr,
A.sar,
A.country, 
date(from_utc_timestamp(from_unixtime(unix_timestamp(cast('${hiveconf:compare_date}' as date))+cast(A.hr as int) * 3600), A.timezone)),
hour(from_utc_timestamp(from_unixtime(unix_timestamp(cast('${hiveconf:compare_date}' as date))+cast(A.hr as int) * 3600), A.timezone))
) using './nise_spatial_index_reader -i ./landuse_SpatialIdx -z -n 1'
AS (latitude, longtitude, request_id, ctr, sar, country, dt, hour, cat)
from zipmap A
) B;


create table if not exists ctr_pattern(
cat string,
imps bigint,
ctr double,
sar double
)
partitioned by (country string, hour string);

insert into table ctr_pattern
partition (country,  hour)
select
D.cat as cat,
count(1) as imps,
avg(ctr) as ctr,
avg(sar) as sar,
D.country as country,
D.hour as hour
from
spatial_tracking_hrly D
where D.cat<>-1 and D.cat is not null group by D.country, D.hour, D.cat;


--drop table if exists temp;
--drop table if exists zipmap;
