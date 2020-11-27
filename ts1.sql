
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
     WHERE raw.__timestamp__ BETWEEN 1612108800000 - 86400000 - 120000 AND 1612108800000
       and district_id = 'D10010'
    group by 1, 2
   )
  SELECT  __id__, v, to_timestamp(t/1000) as l_t
  FROM data raw
  order by __id__, t;




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
   WHERE  ( raw.__timestamp__ BETWEEN 1612108800000 - 86400000 - 120000 AND 1612108800000) AND  ( raw.__timestamp__  BETWEEN step1.instant + 1 - 120000 AND step1.instant )
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
