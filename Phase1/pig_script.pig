--David Beck
--CSC555
--Project Phase 1
--5/19/19

-- Load lineorder data
lineorder = LOAD '/user/ec2-user/lineorder.tbl' USING PigStorage('|') 
AS (lo_orderkey:int, lo_linenumber:int, lo_custkey:int, lo_partkey:int, lo_suppkey:int, lo_orderdate:int,  lo_orderpriority:chararray,  lo_shippriority:chararray,  lo_quantity:int,  lo_extendedprice:int,  lo_ordertotalprice:int, lo_discount:int, lo_revenue:int, lo_supplycost:int, lo_tax:int, lo_commitdate:int, lo_shipmode:chararray);

--Q0.1 Added simple test query
--SELECT AVG(lo_revenue)
--FROM lineorder;

lo_key = GROUP lineorder ALL;
rev_avg = FOREACH lo_key GENERATE AVG(lineorder.lo_revenue);
DUMP rev_avg;

--Q0.2 Added simple test query
--SELECT lo_discount, COUNT(lo_extendedprice)
--FROM lineorder
--GROUP BY lo_discount;

discount = GROUP lineorder BY lo_discount;
count_ext = FOREACH discount GENERATE lineorder.lo_discount, COUNT(lineorder.lo_extendedprice);
DUMP count_ext;

--Q0.3 Added simple test query
--SELECT lo_quantity, SUM(lo_revenue)
--FROM lineorder
--WHERE lo_discount < 3
--GROUP BY lo_quantity;

less_than3 = FILTER lineorder by lo_discount < 3;
quant = GROUP less_than3 BY lo_quantity;
sum_rev = FOREACH quant GENERATE less_than3.lo_quantity, SUM(less_than3.lo_revenue);
DUMP sum_rev;