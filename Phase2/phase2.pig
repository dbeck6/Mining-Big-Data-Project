--David Beck
--CSC555
--Project Phase 2
--6/12/19

-- Load lineorder, part, & customer data
lineorder = LOAD '/user/ec2-user/lineorder.tbl' USING PigStorage('|') 
AS (lo_orderkey:int, lo_linenumber:int, lo_custkey:int, lo_partkey:int, lo_suppkey:int, lo_orderdate:int,  lo_orderpriority:chararray,  lo_shippriority:chararray,  lo_quantity:int,  lo_extendedprice:int,  lo_ordertotalprice:int, lo_discount:int, lo_revenue:int, lo_supplycost:int, lo_tax:int, lo_commitdate:int, lo_shipmode:chararray);

part = LOAD '/user/ec2-user/part.tbl' USING PigStorage('|') 
AS (p_partkey :int,  p_name:chararray,  p_mfgr:chararray, p_category:chararray,  p_brand1:chararray,  p_color:chararray, p_type:chararray,  p_size:int, p_container:chararray);

customer = LOAD '/user/ec2-user/customer.tbl' USING PigStorage('|') 
AS (c_custkey :int,  c_name:chararray,  c_address:chararray,  c_city:chararray,  c_nation:chararray,  c_region:chararray,  c_phone:chararray,  c_mktsegment:chararray);

-- Part 1. Save part table as '~' delimited txt file
p1 = FOREACH part GENERATE $1, $0, $2, $3, $4, $5, $6, $7, $8;
STORE p1 INTO 'pig_part.txt' using PigStorage('~');
DUMP p1;

-- command to see output file:
-- hdfs dfs -cat /user/ec2-user/pig_part.txt/part-m-00000

-- Part 2.
--select c_nation, sum(lo_revenue)
--from customer, lineorder
--where lo_custkey = c_custkey
--  and c_region = 'AMERICA' 
--  and lo_discount BETWEEN 4 and 6 
--group by c_nation;

p2_customer = FILTER customer BY (c_region == 'AMERICA');
p2_lineorder = FILTER lineorder BY (lo_discount >= 4) AND (lo_discount <= 6);
p2_join = JOIN p2_customer BY (c_custkey), p2_lineorder BY (lo_custkey);
p2_group = GROUP p2_join BY c_nation;
p2sum_rev = FOREACH p2_group GENERATE p2_join.c_nation, SUM(p2_join.lo_revenue);
DUMP p2sum_rev;

-- command to run script:
-- bin/pig -f phase2.pig