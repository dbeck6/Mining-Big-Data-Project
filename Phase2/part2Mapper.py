#David Beck
#CSC555
#Project Phase 2
#6/12/19

#!/usr/bin/python
import sys

for line in sys.stdin:
	line = line.strip()
	if line.endswith('group'): # Group Mapper
		print(line)
	else:
		vals = line.split('|')
		
		if vals[1].isdigit(): #Mapper 1 for lineorder.tbl
			# filter values between 4 & 6
			if int(vals[11]) >= 4 and int(vals[11]) <= 6:
				# map lo_custkey and lo_revenue
				print '%s\t%s\tlineorder' % (vals[2], vals[12])
		else: #Mapper 2 for customer.tbl
			# filter not 'AMERICA'
			if vals[5] == 'AMERICA':
				# map c_custkey and c_nation
				print '%s\t%s\tcustomer' % (vals[0], vals[4])
	
# to test mapreduce
#cat lineorder.tbl.sample | python part2Mapper.py | sort | python part2Reducer.py
#cat customer.tbl.sample | python part2Mapper.py | sort | python part2Reducer.py
