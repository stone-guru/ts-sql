
CREATE TABLE indicator_value_1 (
  composite_value_id  INTEGER NOT NULL,
  __timestamp__  BIGINT NOT NULL,
  __sys_timestamp__  BIGINT NOT NULL,
  __value__  DOUBLE PRECISION  NOT NULL,
  __real__ INTEGER NOT NULL DEFAULT 1,
  PRIMARY KEY(composite_value_id, __timestamp__)
);

select create_hypertable('indicator_value_1', '__timestamp__',
                         chunk_time_interval => 1000 * 60 * 60 * 24 * 7,
                         if_not_exists => TRUE);

--create index on indicator_value_1(__timestamp__, composite_value_id) with (timescaledb.transaction_per_chunk);
create index on indicator_value_1(__timestamp__);
create index on indicator_value_1(__sys_timestamp__);
create index on indicator_value_1(__value__);
create index on indicator_value_1(composite_value_id);


CREATE TABLE indicator_label_1 (
 __id__ integer NOT NULL,
 district_id text,
 road_id text ,
 primary key(__id__)
);

create unique index on indicator_label_1(road_id, district_id);
create index on indicator_label_1(district_id);
create index on indicator_label_1(road_id);


insert into indicator_label_1 (__id__, district_id, road_id) 
select r_id as __id__, ('D' || cast(d_id as varchar)) as district_id,  ('R' || cast(r_id as varchar)) as road_id
from (select d.id as d_id , d.id * 1000 + r.id as r_id
  from generate_series(10001, 10010, 1) as d(id),
       generate_series(1, 200, 1) as r(id)) as basic;

select s.id,
       floor(random() * 21) + 1 :: integer as label_id,
       date '1980-01-01' + random_int(30 * 365) + hours(random_int(12)) as st,
       random_int(100) as v1,
       random_int(100) as v2,
       random_int(100) as v3
  from generate_series(10*10000*10000, 10*11000*10000, 1) as s(id);


select s.id,
       floor(random() * 21) + 1 :: integer as label_id,
       date '1980-01-01' + random_int(30 * 365) + hours(random_int(12)) as st,
       random_int(100) as v1,
       random_int(100) as v2,
       random_int(100) as v3
  from generate_series(1, 365, 1) as y(id);


select count(*), to_timestamp(min(ms)/1000), to_timestamp(max(ms)/1000)
  from 
(select extract(epoch from  date '2021-01-01' + y.v - (interval '8 hour') + (m.v * interval '1 minute')) * 1000 as ms
  from generate_series(0, 364, 1) as y(v),
       generate_series(0, 1438, 2) as m(v)) as tm;


insert into indicator_value_1(composite_value_id, __timestamp__, __sys_timestamp__, __value__, __real__)
select  lb.__id__ as composite_value_id,
        ms as __timestamp__,
        ms as __sys_timestamp__,
        floor(random()*(1000))+1 as __value__,
        1 as __real__
from indicator_label_1 lb,
     (select extract(epoch from
                     date '2021-01-01' + y.v - (interval '8 hour')
                     + (m.v * interval '1 minute')) * 1000 as ms
        from generate_series(0, 364, 1) as y(v),
             generate_series(0, 1438, 2) as m(v)) as tm
limit 100;


CREATE OR REPLACE  FUNCTION instants(i0  bigint, step bigint, n bigint)
RETURNS TABLE(instant bigint) AS $$
BEGIN
    RETURN QUERY WITH RECURSIVE t(i, k) AS (
       VALUES (i0, 1)
     UNION ALL
       SELECT i - step , k + 1 FROM t WHERE k < n
   )
   SELECT i
   FROM t;
END; $$ LANGUAGE plpgsql;

CREATE OR REPLACE  FUNCTION time_group(t bigint, t0  bigint, step bigint)
RETURNS  bigint AS $$
BEGIN
  RETURN (t - t0)/step;
END; $$ LANGUAGE plpgsql;


