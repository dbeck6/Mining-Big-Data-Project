#Davkey Beck
#CSC555
#Project Phase 2
#6/12/19

#!/usr/bin/python
import sys, math

curr_key = None
key = None
center = None
total = 0

# The input comes from standard input (line by line)
for line in sys.stdin:
	line = line.strip()
	# parse the line and split it by '\t'
	ln = line.split('\t')
	# grab the key
	key = ln[0]
	points = [int(x) for x in ln[1:]] # get int point values
	#print("points" + points)
	
	if curr_key == key:
		# running totals for calculating new centers
		center = [a + b for a, b in zip(points, center)]
		total += 1
	else:
		if curr_key: 
			# calculate the floor division value for the new center point
			center = [y // total for y in center]
			#print("center" + center)
			print "%s\t%d\t%d\t%d" % (curr_key, center[0], center[1], center[2])
			
		curr_key = key
		center = points
		total += 1
	
# output the last key
if curr_key == key:
	if curr_key: 
		# calculate the floor division value for the new center point
		center = [y // total for y in zip(points, center)]
		print "%s\t%d\t%d\t%d" % (curr_key, center[0], center[1], center[2])
	
# command to set up mapreduce job
#time hadoop jar hadoop-streaming-2.6.4.jar -D mapred.map.tasks=50 -D mapred.reduce.tasks=1 -D mapred.text.key.comparator.options=-n --input /kmeans/Numbers.txt -output /kmeans/centers2.txt -mapper kmeansMapper.py -reducer kmeansReducer.py -file kmeansReducer.py -file kmeansMapper.py -file centers.txt
# command to copy center2.txt output to local file system for next pass
# hadoop fs -copyToLocal /kmeans/centers2.txt/part-00000  /home/ec2-user/hadoop-2.6.4/centers2.txt
#time hadoop jar hadoop-streaming-2.6.4.jar -D mapred.map.tasks=50 -D mapred.reduce.tasks=1 -D mapred.text.key.comparator.options=-n --input /kmeans/Numbers.txt -output /kmeans/centers3.txt -mapper kmeansMapper.py -reducer kmeansReducer.py -file kmeansReducer.py -file kmeansMapper.py -file centers2.txt
# hadoop fs -copyToLocal /kmeans/centers3.txt/part-00000  /home/ec2-user/hadoop-2.6.4/centers3.txt