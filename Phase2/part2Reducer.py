#Davkey Beck
#CSC555
#Project Phase 2
#6/12/19

#!/usr/bin/python
import sys

curr_key = None
key = None
valsCust = ''
valsLine = ''
valsGroup = None
sum_rev = 0

# The input comes from standard input (line by line)
for line in sys.stdin:
	line = line.strip()
	# parse the line and split it by '\t'
	ln = line.split('\t')
	# grab the key
	key = ln[0]
	value = ln[1]
	
	if curr_key == key:
		if ln[2] == 'lineorder':
			valsLine = value
		if ln[2] == 'customer':
			valsCust = value
		if ln[2] == 'group':
			rev = int(value[0])
			sum_rev += rev
	else:
		if curr_key: 
			if sum_rev > 0:
				print "%s\t%s" % (curr_key, sum_rev)
			else:
				# Join means that there have to be rows on each side
				if (valsCust != '' and valsLine != ''):
					# output the key & values with c_nation and lo_revenue
					c_nation = valsCust
					lo_revenue = valsLine
					print '%s\t%s\tgroup' % (c_nation, lo_revenue)
		curr_key = key
		if ln[2] == 'lineorder':
			valsLine = value
		if ln[2] == 'customer':
			valsCust = value
		if ln[2] == 'group':
			rev = int(value[0])
			sum_rev += rev
	
# output the last key
if curr_key == key:
	if curr_key: 
		if sum_rev > 0:
			print "%s\t%s" % (curr_key, sum_rev)
		else:
			if (valsCust != '' and valsLine != ''):
					# output the key & values with c_nation and lo_revenue
					c_nation = valsCust
					lo_revenue = valsLine
					print '%s\t%s\tgroup' % (c_nation, lo_revenue)
	
# command to set up mapreduce job
#time hadoop jar hadoop-streaming-2.6.4.jar -D mapred.map.tasks=50 -D mapred.reduce.tasks=3 -D mapred.text.key.comparator.options=-n --input /data/join_tbl -output /data/cust_line -mapper part2Mapper.py -reducer part2Reducer.py -file part2Reducer.py -file part2Mapper.py
#time hadoop jar hadoop-streaming-2.6.4.jar -D mapred.map.tasks=50 -D mapred.reduce.tasks=3 -D mapred.text.key.comparator.options=-n -input /data/cust_line -output /data/grouping -mapper part2Mapper.py -reducer part2Reducer.py -file part2Reducer.py -file part2Mapper.py