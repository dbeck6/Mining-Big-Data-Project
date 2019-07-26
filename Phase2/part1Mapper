#David Beck
#CSC555
#Project Phase 1
#5/19/19

#!/usr/bin/python
import sys

for line in sys.stdin:
	line = line.strip()
	vals = line.split("|")
	#map lo_quantity, lo_discount, lo_revenue
	print "%s\t%s\t%s" % (vals[8], vals[11], vals[12])
	#print ('\t'.join(vals))
	
# to test mapreduce
#cat lineorder.tbl | python myMapper.py | sort | python myReducer.py
