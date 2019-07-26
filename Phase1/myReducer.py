#David Beck
#CSC555
#Project Phase 1
#5/19/19

#!/usr/bin/python
import sys

curr_id = None
curr_sum = 0
id = None
dis = None
rev = None
filter = 3

# The input comes from standard input (line by line)
for line in sys.stdin:
	line = line.strip()
	# parse the line and split it by '\t'
	ln = line.split('\t')
	# grab the key (int)
	id = int(ln[0])
	# grab value to filter by
	dis = int(ln[1])
	# grab revenue value
	rev = int(ln[2])
	
	if dis < filter:
		if curr_id == id:
			curr_sum += rev
		else:
			if curr_id: # output the count, single key completed
				# NOTE: Change this to '%s\t%d' if your key is a string
				print '%d\t%d' % (curr_id, curr_sum)
		
			curr_id = id
			curr_sum = 0
	
# output the last key
if dis < filter:
	if curr_id == id:
		print '%d\t%d' % (curr_id, curr_sum)
		#print ('\t'.join(vals))
	
# command to set up mapreduce job
#time hadoop jar hadoop-streaming-2.6.4.jar -D mapred.map.tasks=50 -D mapred.reduce.tasks=3 -D mapred.text.key.comparator.options=-n -input /data/lineorder.tbl -output /data/reduce_lineorder -mapper myMapper.py -reducer myReducer.py -file myReducer.py -file myMapper.py