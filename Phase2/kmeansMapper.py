#David Beck
#CSC555
#Project Phase 2
#6/12/19

#!/usr/bin/python
import sys, math

# if given centers.txt with -file centers.txt at command line. must be changed for other inputs
fd = open('centers.txt', 'r')
centers = fd.readlines()
fd.close()

keys = [] # create keys list
for i in range(len(centers)):
	center = centers[i].strip().split()
	center_pts = [int(x) for x in center[1:]] # a list of int points
	keys.append(center[0]) # add center key to list
	centers[i] = center_pts

for line in sys.stdin:
	line = line.strip().split()
	points = [int(x) for x in line] # a list of int points
		
	p_min = 10000
	key = None
	# compute the closest center using euclidean distance in 3-dimensional space
	for i in range(len(centers)):
		# find the euclidean distance from next center point
		tmp_key = keys[i]
		tmp_min = math.sqrt(sum([(a - b) ** 2 for a, b in zip(points, centers[i][1:])]))
		if tmp_min <  p_min: # if center point is closer to point, update
			p_min = tmp_min
			key = tmp_key
	print '%s\t%d\t%d\t%d' % (key, points[0], points[1], points[2])
	
# to test mapreduce
#cat Numbers.txt.sample | python kmeansMapper.py | sort | python kmeansReducer.py

