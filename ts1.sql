
explain  analyse WITH labels as (
   SELECT road_id as link_id, __id__
     FROM indicator_label_1
     WHERE road_id = 'R10001001'
   ),
  data as (
   SELECT raw.__value__, raw.composite_value_id, step1.instant AS instant1, ( step1.instant ) AS __timestamp__
   FROM indicator_value_1 raw ,
    instants(1612108800000, 120000, 86400000 / 120000) step1
   WHERE  ( raw.__timestamp__ BETWEEN 1612108800000 - 86400000 - 120000 AND 1612108800000) AND  (raw.__timestamp__  BETWEEN step1.instant + 1 - 120000 AND step1.instant )
   )
  SELECT __value__, __timestamp__ , link_id
  FROM  (
   SELECT __value__, __timestamp__, instant1, link_id
   FROM   (
     SELECT  lb.link_id, raw.__value__, raw.__timestamp__, raw.instant1
     FROM data raw
     JOIN labels AS lb ON raw.composite_value_id = lb.__id__ ) AS ts1 ) AS ts1;

-- 计算的方式去重
explain  analyse
WITH
  data as (
    SELECT raw.composite_value_id as __id__,
           time_group(__timestamp__, 1612108800000 - 86400000::bigint * 30 - 120000, 60000 * 10),
           last(raw.__value__, raw.__timestamp__) AS v,
           last(raw.__timestamp__, raw.__timestamp__) as t 
      FROM indicator_value_1 raw
      JOIN indicator_label_1 lb on lb.__id__ = raw.composite_value_id
     WHERE raw.__timestamp__ BETWEEN 1612108800000 - 86400000::bigint * 30  - 120000 AND 1612108800000
       and district_id = 'D10010'
    group by 1, 2
   )
  SELECT  __id__, v, to_timestamp(t/1000) as l_t
  FROM data raw
  order by __id__, t;

select lava_timestamp('2020-03-01 23:58:00') - lava_timestamp('2020-03-01 00:04:00');

select lava_timestamp('2020-03-01 00:04:00');

-- [7d#4]:[1d] 计算的方式去重
explain  analyse
  with data as (
    SELECT raw.composite_value_id,
           ((lava_timestamp('2020-03-01 23:58:00') - raw.__timestamp__) % lava_day(1))/120000 as t,
           to_timestamp(last(raw.__timestamp__, raw.__timestamp__)/1000) as raw_t,
           -- count(*) as c,
           avg(__value__) as __value__
      FROM indicator_value_0 raw
             JOIN indicator_label_1 lb on lb.__id__ = raw.composite_value_id
     WHERE
       district_id = 'D10006'
       and  road_id = 'R10006130'
       and (raw.__timestamp__ BETWEEN  lava_timestamp('2020-03-01 23:58:00')  - 86400000 + 1
            AND lava_timestamp('2020-03-01 23:58:00') 
            or raw.__timestamp__ BETWEEN  lava_timestamp('2020-03-01 23:58:00')  - 86400000 - 86400000 * 7 + 1
            AND lava_timestamp('2020-03-01 23:58:00') - 86400000 * 7 
            or raw.__timestamp__ BETWEEN  lava_timestamp('2020-03-01 23:58:00')  - 86400000 - 86400000 * 14 + 1
            AND lava_timestamp('2020-03-01 23:58:00') - 86400000 * 14
            or raw.__timestamp__ BETWEEN  lava_timestamp('2020-03-01 23:58:00') - 86400000 - 86400000 * 21 + 1
            AND lava_timestamp('2020-03-01 23:58:00') - 86400000 * 21)
       -- and road_id = 'R10003020' or road_id = 'R10003021'  or road_id = 'R10003030'
     group by 1, 2
  )
  select d.composite_value_id, d.__value__, d.raw_t as __timestamp__, lb2.district_id, lb2.road_id
  from data d join indicator_label_1 lb2 on d.composite_value_id = lb2.__id__
  order by d.composite_value_id, __timestamp__
;

explain analyse

with data as (
    SELECT raw.composite_value_id,
           lava_align(raw.__timestamp__, lava_timestamp('2020-03-01 23:58:00'), 120000*30) as t,
           first(raw.__value__, raw.__timestamp__) as __value__
      FROM indicator_value_1 raw
             JOIN indicator_label_1 lb on lb.__id__ = raw.composite_value_id
     WHERE
       district_id = 'D10006'
       and  road_id = 'R10006130'
       and (raw.__timestamp__ BETWEEN  lava_timestamp('2020-03-01 23:58:00')  - 86400000 + 1
            AND lava_timestamp('2020-03-01 23:58:00') 
            or raw.__timestamp__ BETWEEN  lava_timestamp('2020-03-01 23:58:00')  - 86400000 - 86400000 * 7 + 1
            AND lava_timestamp('2020-03-01 23:58:00') - 86400000 * 7 
            or raw.__timestamp__ BETWEEN  lava_timestamp('2020-03-01 23:58:00')  - 86400000 - 86400000 * 14 + 1
            AND lava_timestamp('2020-03-01 23:58:00') - 86400000 * 14
            or raw.__timestamp__ BETWEEN  lava_timestamp('2020-03-01 23:58:00') - 86400000 - 86400000 * 21 + 1
            AND lava_timestamp('2020-03-01 23:58:00') - 86400000 * 21)
       -- and road_id = 'R10003020' or road_id = 'R10003021'  or road_id = 'R10003030'
     group by 1, 2
  ),
  aggr as (
      select d.composite_value_id,
             ((lava_timestamp('2020-03-01 23:58:00') - d.t) % lava_day(1)) as t,
             avg(__value__) as __value__,
             last(d.t, d.t) as t2
        from data d
        group by 1, 2
        )
  select d.composite_value_id,
         to_timestamp(d.t2/1000) as __timestamp__,
         d.__value__,
         lb2.district_id,
         lb2.road_id
  from aggr d join indicator_label_1 lb2 on d.composite_value_id = lb2.__id__
  order by d.composite_value_id, __timestamp__
;

explain analyse
  with data as (
    SELECT raw.composite_value_id,
           lava_align(raw.__timestamp__, lava_timestamp('2020-03-01 23:58:00'), 120000) as t,
           last(raw.__value__, raw.__timestamp__) as __value__
           -- count(*) as c,
           -- avg(__value__) as __value__
      FROM indicator_value_1 raw
             JOIN indicator_label_1 lb on lb.__id__ = raw.composite_value_id
     WHERE
       (district_id = 'D10006'  or district_id = 'D10001' or district_id = 'D10008') and
       -- and  road_id = 'R10006130'
       (raw.__timestamp__ BETWEEN  lava_timestamp('2020-03-01 23:58:00')  - 86400000 + 1
            AND lava_timestamp('2020-03-01 23:58:00') 
            or raw.__timestamp__ BETWEEN  lava_timestamp('2020-03-01 23:58:00')  - 86400000 - 86400000 * 7 + 1
            AND lava_timestamp('2020-03-01 23:58:00') - 86400000 * 7 
            or raw.__timestamp__ BETWEEN  lava_timestamp('2020-03-01 23:58:00')  - 86400000 - 86400000 * 14 + 1
            AND lava_timestamp('2020-03-01 23:58:00') - 86400000 * 14
            or raw.__timestamp__ BETWEEN  lava_timestamp('2020-03-01 23:58:00') - 86400000 - 86400000 * 21 + 1
            AND lava_timestamp('2020-03-01 23:58:00') - 86400000 * 21)
       -- and road_id = 'R10003020' or road_id = 'R10003021'  or road_id = 'R10003030'
     group by 1, 2
  ),
  aggr as (
      select d.composite_value_id,
             ((lava_timestamp('2020-03-01 23:58:00') - d.t) % lava_day(1)) as t,
             avg(__value__) as __value__,
             last(d.t, d.t) as t2
        from data d
        group by 1, 2
        )
  select to_timestamp(d.t2/1000) as __timestamp__,
         max(d.__value__),
         lb2.district_id
  from aggr d join indicator_label_1 lb2 on d.composite_value_id = lb2.__id__
  group by lb2.district_id, d.t2
;


SELECT raw.composite_value_id as __id__,
       (lava_timestamp('2020-03-01 23:58:00') - raw.__timestamp__) % lava_day(1) as t,
       to_timestamp(raw.__timestamp__/1000) as raw_t
       -- to_timestamp(last(raw.__timestamp__, raw.__timestamp__)/1000) as raw_t,
       -- count(*) as c,
       -- avg(__value__) as v
  FROM indicator_value_3 raw
         JOIN indicator_label_1 lb on lb.__id__ = raw.composite_value_id
 WHERE
   district_id = 'D10006'
   and  road_id = 'R10006130'
   and (raw.__timestamp__ BETWEEN  lava_timestamp('2020-03-01 23:58:00')  - 86400000 + 1
        AND lava_timestamp('2020-03-01 23:58:00') 
        or raw.__timestamp__ BETWEEN  lava_timestamp('2020-03-01 23:58:00')  - 86400000 - 86400000 * 7 + 1
        AND lava_timestamp('2020-03-01 23:58:00') - 86400000 * 7 
        or raw.__timestamp__ BETWEEN  lava_timestamp('2020-03-01 23:58:00')  - 86400000 - 86400000 * 14 + 1
        AND lava_timestamp('2020-03-01 23:58:00') - 86400000 * 14
        or raw.__timestamp__ BETWEEN  lava_timestamp('2020-03-01 23:58:00') - 86400000 - 86400000 * 21 + 1
        AND lava_timestamp('2020-03-01 23:58:00') - 86400000 * 21)
 order by __id__, t;


WITH
  data as (
    SELECT raw.composite_value_id as __id__,
           (1580486400000 - raw.__timestamp__) % (86400000) as t,
           first(to_timestamp(raw.__timestamp__/1000), raw.__timestamp__) as raw_t,
           count(*) as c,
           avg(__value__) as v
      FROM indicator_value_3 raw
             JOIN indicator_label_1 lb on lb.__id__ = raw.composite_value_id
     WHERE
       district_id = 'D10006'
       and  road_id = 'R10006130'
       and (raw.__timestamp__ BETWEEN  1580486400000  - 86400000 AND 1580486400000
       or raw.__timestamp__ BETWEEN  1580486400000  - 86400000 - 86400000 * 7 AND 1580486400000 - 86400000 * 7 -1 
       or raw.__timestamp__ BETWEEN  1580486400000  - 86400000 - 86400000 * 14 AND 1580486400000 - 86400000 * 14 - 1
       or raw.__timestamp__ BETWEEN  1580486400000 - 86400000 - 86400000 * 21 AND 1580486400000 - 86400000 * 21 - 1)
       -- and road_id = 'R10003020' or road_id = 'R10003021'  or road_id = 'R10003030'
     group by 1, 2
  )
  select *
  from data
  order by __id__, t

  -- SELECT  __id__, instant, to_timestamp(instant/1000) as t, avg(v), count(*) as c
  -- FROM data raw
  -- group by __id__, instant
  -- order by __id__, instant;



-- 当前SQL带去重
explain analyse WITH labels as (
   SELECT road_id as link_id, __id__
     FROM indicator_label_1
    WHERE road_id = 'R10001001'
   ),
  data as (
   SELECT raw.__value__, raw.composite_value_id, step1.instant AS instant1, ( step1.instant ) AS __timestamp__
   FROM indicator_value_1 raw ,
    instants(1612108800000, 120000, 86400000 / 120000) step1
    WHERE  ( raw.__timestamp__ BETWEEN 1612108800000 - 86400000 - 120000 AND 1612108800000)
      AND  ( raw.__timestamp__  BETWEEN step1.instant + 1 - 120000 AND step1.instant )
   )
  SELECT __value__, __timestamp__, dense_rank() over (  order by link_id ) AS __id__ , link_id
  FROM  (
   SELECT __value__, __timestamp__, row_number() over (partition by instant1, link_id ORDER BY __timestamp__ ASC ) AS score, instant1, link_id
   FROM   (
     SELECT  lb.link_id, raw.__value__, raw.__timestamp__, raw.instant1
     FROM data raw
     JOIN labels AS lb ON raw.composite_value_id = lb.__id__ ) AS ts1 ) AS ts1
  WHERE score <= 1;


explain analyse WITH labels as (
   SELECT road_id as link_id, __id__
   FROM indicator_label_1
  WHERE road_id = 'R10001001'
   ),
  data as (
   SELECT raw.__value__, raw.composite_value_id, ( raw.__timestamp__ + (1612108800000 - raw.__timestamp__)%120000 ) AS instant1
   FROM indicator_value_1 raw
   WHERE  raw.__timestamp__ BETWEEN 1612108800000 - 86400000 - 120000 AND 1612108800000
   )
  SELECT __value__, __timestamp__, dense_rank() over (  order by link_id ) AS __id__ , link_id
  FROM  (
   SELECT __value__, __timestamp__, row_number() over (partition by instant1, link_id ORDER BY __timestamp__ ASC ) AS score, instant1, link_id
   FROM   (
     SELECT  lb.link_id, raw.__value__, raw.instant1 as __timestamp__, raw.instant1
     FROM data raw
     JOIN labels AS lb ON raw.composite_value_id = lb.__id__ ) AS ts1 ) AS ts1
  WHERE score <= 1;


explain analyse


select  __timestamp__ / (86400000 * 7) as k,
  count(*),
  -- to_timestamp(time_bucket(86400000 * 7, __timestamp__)/1000) as d,
  avg(__value__) as avg_v,
  to_timestamp(first(__timestamp__, __timestamp__)/1000) as first_,
  to_timestamp(last(__timestamp__, __timestamp__)/1000) as last_
  from indicator_value_1 r
 where r.__timestamp__ between 1618099200000::bigint - 86400000::bigint * 7 AND (1618099200000 - 1)::bigint
  group by composite_value_id, k
  order by composite_value_id, k
  limit 20;


select to_timestamp(1612108800000/1000);
select to_timestamp((1612108800000::bigint - 86400000::bigint * 7)/1000);

select extract(epoch from  (date '2021-01-01') + 100 * Interval '1 days') * 1000;

WITH labels as (
 SELECT code, city, county, __id__
 FROM indicator_label_1
 WHERE county = 'Togo'
 ),
data as (
 SELECT raw.__value__, raw.composite_value_id, step1.instant AS instant1, step2.instant AS instant2, ( step1.instant + step2.instant ) AS __timestamp__
 FROM test_value_table_name raw ,
  instants(1592378130213, 120000, 86400000 / 120000) step1,
  instants(0, 604800000, 4) step2
 WHERE  ( raw.__timestamp__ BETWEEN 1592378130213 - 2505600000 - 120000 AND 1592378130213 ) AND  ( raw.__timestamp__  BETWEEN step1.instant + step2.instant + 1 - 120000 AND step1.instant + step2.instant )
 )
SELECT dense_rank() over (  order by code, city, county ) AS __id__ ,  ( instant1 ) AS __timestamp__ , code, city, county, avg( __value__ ) AS __value__
FROM (
SELECT __value__, __timestamp__, instant1, instant2, code, city, county
FROM  (
 SELECT __value__, __timestamp__, row_number() over (partition by instant1, instant2, code, city, county ORDER BY __timestamp__ ASC ) AS score, instant1, instant2, code, city, county
FROM   (
 SELECT  lb.code, lb.city, lb.county, raw.__value__, raw.__timestamp__, raw.instant1, raw.instant2
 FROM data raw
 JOIN labels AS lb ON raw.composite_value_id = lb.__id__ ) AS ts2 ) AS ts2
WHERE score <= 1 ) AS ts1
GROUP BY instant1, code, city, county

-- max_over_time(indicator_xx[1h])
CREATE VIEW indicator_value_3_hourly
WITH (timescaledb.continuous) AS
SELECT time_bucket(BIGINT '3600000', __timestamp__) as __time_bucket__,
       composite_value_id,
       to_timestamp(first(__timestamp__, __timestamp__)/1000) as __timestamp__, 
       max(__value__) as __value__
  FROM indicator_value_3
 GROUP BY 1, 2;

-- max_over_time(indicator_xx[1d:1h])
CREATE VIEW indicator_value_3_hourly
WITH (timescaledb.continuous) AS
SELECT time_bucket(BIGINT '3600000', __timestamp__) as __time_bucket__,
       composite_value_id,
       to_timestamp(first(__timestamp__, __timestamp__)/1000) as __timestamp__, 
       max(__value__) as __value__
  FROM indicator_value_3
 GROUP BY 1, 2;

-- max_over_time((max by (district_id) (indicator_xx)) [1d])

-- max by (district_id) (max_over_time((indicator_xx)[1d]))
-- min by (district_id) (min_over_time((indicator_xx)[1d]))
-- avg by (district_id) (avg_over_time((indicator_xx)[1d])) 
-- sum by (district_id) (sum_over_time((indicator_xx)[1d]))

CREATE VIEW indicator_value_2_district_4hour
WITH (timescaledb.continuous) AS
select time_bucket(bigint '28800000', __timestamp__) as __timestamp__,
       district_id,
       to_timestamp(last(d.__timestamp__, d.__timestamp__)/1000) as __ts__,
       avg(d.__value__) as __value__
  FROM indicator_value_2 d
 GROUP by 1, 2
;


SELECT * 
  FROM indicator_value_1_hourly
  ORDER by __timestamp__ desc
  LIMIT 100;


select to_timestamp(time_bucket(lava_hour(8), lava_timestamp('2020-02-01 09:34:01'))/1000);
