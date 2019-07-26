#David Beck
#CSC555
#Project Phase 2
#6/12/19

#!/usr/bin/python
import sys

for line in sys.stdin:
	line = line.strip()
	vals = line.split("|")
	print ('\t'.join(vals))
	
# to test mapreduce
#cat part.tbl | python part1Mapper.py | sort | python part1Reducer.py
