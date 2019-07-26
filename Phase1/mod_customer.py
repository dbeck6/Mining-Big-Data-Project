#David Beck
#CSC555
#Project Phase 1
#5/19/19

#!/usr/bin/python
import sys

for line in sys.stdin:
	line = line.strip()
    vals = line.split('\t')
    cad = vals[1]
	if len(cad) > 8:
		vals[1] = cad[0:8]
	cci = vals[2]
    vals[2] = cci[0:-2] + ' #' + cci[-1]
    print ('\t'.join(vals))
	
"""
create table customer_address(c_custkey int, short_address varchar(10), mod_city varchar(15)) ROW FORMAT DELIMITED FIELDS 
TERMINATED BY '\t';

add FILE ../mod_customer.py;

insert overwrite table customer_address select transform 
(c_custkey, c_address, c_city) 
using 'python mod_customer.py' as 
(c_custkey, short_address, mod_city) from customer;
"""