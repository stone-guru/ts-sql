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
                     date '2020-01-01' + y.v - (interval '8 hour')
                     + (m.v * interval '1 minute')) * 1000 as ms
        from generate_series(0, 365*2-1, 1) as y(v),
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

CREATE OR REPLACE  FUNCTION time_group2(t bigint, t0  bigint, step1 bigint)
RETURNS  bigint AS $$
BEGIN
  RETURN t + (t - t0)/step;
END; $$ LANGUAGE plpgsql;

CREATE TABLE indicator_value_2 (
  __timestamp__  BIGINT NOT NULL,
  __sys_timestamp__  BIGINT NOT NULL,
  district_id INTEGER ,
  road_id  INTEGER ,
  __value__  DOUBLE PRECISION  NOT NULL,
  __real__ INTEGER NOT NULL DEFAULT 1,
  PRIMARY KEY(__timestamp__, district_id, road_id)
);

select create_hypertable('indicator_value_2', '__timestamp__',
                         chunk_time_interval => 1000 * 60 * 60 * 24 * 7,
                         if_not_exists => TRUE);

insert into indicator_value_2(
  __timestamp__,
  __sys_timestamp__,
  district_id,
  road_id,
  __value__,
  __real__)
select ms as __timestamp__,
       ms as __sys_timestamp__,
       dim.d_id as district_id,
       dim.r_id as road_id,
        floor(random()*(1000))+1 as __value__,
        1 as __real__
from (select extract(epoch from
                     date '2020-01-01' + y.v - (interval '8 hour')
                     + (m.v * interval '1 minute')) * 1000 as ms
        from generate_series(0, 100-1, 1) as y(v),
             generate_series(0, 1438, 2) as m(v)) as tm,
     (select d.id as d_id , d.id * 1000 + r.id as r_id
        from generate_series(10001, 10010, 1) as d(id),
             generate_series(1, 200, 1) as r(id)) as dim


SELECT
    relname AS "relation",
    pg_size_pretty (
        pg_relation_size (C.oid)
    ) AS "data_size",
    pg_size_pretty (
        pg_total_relation_size (C .oid)
    ) AS "total_size"
FROM
    pg_class C
LEFT JOIN pg_namespace N ON (N.oid = C .relnamespace)
WHERE
    nspname NOT IN (
        'pg_catalog',
        'information_schema'
    )
AND C .relkind <> 'i' and relname like '_hy%'
AND nspname !~ '^pg_toast'
ORDER BY
    pg_total_relation_size (C .oid) DESC;


CREATE TABLE indicator_value_4 (
  composite_value_id  INTEGER NOT NULL,
  __timestamp__  bigint NOT NULL,
  __sys_timestamp__  bigint  NOT NULL,
  __value__  DOUBLE PRECISION  NOT NULL,
  __real__ INTEGER NOT NULL DEFAULT 1,
  PRIMARY KEY(composite_value_id, __timestamp__)
);

CREATE TABLE indicator_value_0 (
  composite_value_id  INTEGER NOT NULL,
  __timestamp__  bigint NOT NULL,
  __sys_timestamp__  bigint NOT NULL,
  __value__  DOUBLE PRECISION  NOT NULL,
  __real__ INTEGER NOT NULL DEFAULT 1,
  PRIMARY KEY(composite_value_id, __timestamp__)
);

select create_hypertable('indicator_value_3', '__timestamp__',
                         chunk_time_interval => 1000 * 60 * 60 * 24 * 7,
                         if_not_exists => TRUE);


ALTER TABLE indicator_value_3 SET (
  timescaledb.compress,
  timescaledb.compress_segmentby = 'composite_value_id',
  timescaledb.compress_orderby = '__timestamp__ DESC'
);
select set_integer_now_func('indicator_value_3', 'unix_now');

SELECT add_compress_chunks_policy('indicator_value_3', 1000 :: bigint * 60 * 60 * 24 * 56);


CREATE OR REPLACE FUNCTION unix_now()
  returns BIGINT
  LANGUAGE SQL STABLE as
$$
  SELECT extract(epoch from now())::BIGINT * 1000;
$$;

CREATE OR REPLACE FUNCTION lava_timestamp(varchar)
  returns bigint
  LANGUAGE SQL STABLE as
$$
  SELECT extract(epoch from to_timestamp($1, 'YYYY-MM-DD HH24:MI:SS'))::BIGINT * 1000;
$$;

CREATE OR REPLACE FUNCTION lava_day(bigint)
  returns bigint
  LANGUAGE SQL STABLE as
$$
  SELECT $1 * 86040000;
$$;

CREATE OR REPLACE FUNCTION lava_hour(bigint)
  returns bigint
  LANGUAGE SQL STABLE as
$$
  SELECT $1 * 60 * 60 * 1000;
$$;


CREATE OR REPLACE FUNCTION lava_minute(bigint)
  returns bigint
  LANGUAGE SQL STABLE as
$$
  SELECT $1 * 60 * 1000;
$$;

CREATE OR REPLACE FUNCTION lava_align(bigint, bigint, bigint)
  returns bigint
  LANGUAGE SQL STABLE as
$$
  SELECT $2 - $3 * (($2 - $1)/$3)
$$;


SELECT h.table_name, c.interval_length
  FROM _timescaledb_catalog.dimension c
         JOIN _timescaledb_catalog.hypertable h ON h.id = c.hypertable_id;


insert into indicator_value_0(composite_value_id, __timestamp__, __sys_timestamp__, __value__, __real__)
select  lb.__id__ as composite_value_id,
        ms as __timestamp__,
        ms as __sys_timestamp__,
        floor(random()*(1000))+1 as __value__,
        1 as __real__
from indicator_label_1 lb,
     (select extract(epoch from
                     date '2020-01-01' + y.v - (interval '8 hour')
                     + (m.v * interval '1 minute')) * 1000 as ms
        from generate_series(0, 188, 1) as y(v),
             generate_series(0, 1438, 2) as m(v)) as tm;


select extract(epoch from
                     date '2020-01-01' + 100 - (interval '8 hour')
                     + (1 * interval '1 minute')) * 1000 as ms;


select timestamp '2020-01-01 12:00:23';

select *
  from indicator_value_3
 where composite_value_id = 10001010
   and __timestamp__ >= lava_timestamp('2020-04-01') and __timestamp__ < lava_timestamp('2020-04-02');



SELECT composite_value_id,
       time_bucket(lava_hour(1), __timestamp__) as d, 
       to_timestamp(first(__timestamp__, __timestamp__)/1000) as __timestamp__, 
       max(__value__) as __value__
  FROM indicator_value_3
  WHERE composite_value_id = 10005071
 GROUP BY 1, 2
 order by 3 asc
 limit 10;


CREATE VIEW indicator_value_1_hourly 
WITH (timescaledb.continuous) AS
SELECT time_bucket(BIGINT '86400000', __timestamp__) as d,
       composite_value_id,
       to_timestamp(first(__timestamp__, __timestamp__)/1000) as __timestamp__, 
       max(__value__) as __value__
  FROM indicator_value_3
  WHERE composite_value_id = 10005071
 GROUP BY 1, 2;

