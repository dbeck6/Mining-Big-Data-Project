#David Beck
#CSC555
#Project Phase 2
#6/12/19

#!/usr/bin/python
import sys

curr_id = None
id = None

# The input comes from standard input (line by line)
for line in sys.stdin:
	line = line.strip()
	# parse the line and split it by '\t'
	ln = line.split('\t')
	# grab the key (int)
	id = int(ln[0])
	# swap columns 1 & 2
	tmp = ln[0]
	ln[0] = ln[1]
	ln[1] = tmp
	
	if curr_id == id:
		continue
	else:
		if curr_id: # output the line with swapped columns 1 & 2
			# NOTE: Change this to '%s\t%d' if your key is a string
			#print "%s~%s~%s~%s~%s~%s~%s~%s~%s" % (ln[0], ln[1], ln[2], ln[3], ln[4], ln[5], ln[6], ln[7], ln[8])		
			print ('~'.join(ln))
		curr_id = id
	
# output the last key
if curr_id == id:
	#print "%s~%s~%s~%s~%s~%s~%s~%s~%s" % (ln[0], ln[1], ln[2], ln[3], ln[4], ln[5], ln[6], ln[7], ln[8])
	print ('~'.join(ln))
	
# command to set up mapreduce job
#time hadoop jar hadoop-streaming-2.6.4.jar -D mapred.map.tasks=50 -D mapred.reduce.tasks=3 -D mapred.text.key.comparator.options=-n -input /user/ec2-user/part.tbl -output /user/ec2-user/mod_parts -mapper part1Mapper.py -reducer part1Reducer.py -file part1Reducer.py -file part1Mapper.py