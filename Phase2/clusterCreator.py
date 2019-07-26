#!/usr/bin/python

import random

fdx = open('Numbers.txt', 'w')

numbers = int(random.random() * 1000 + 100)
for x in range(125000):
    for y in range(3):
        r = str(int(random.random() * numbers))
        fdx.write(r)
        fdx.write(" ")
    fdx.write("\n")

fdx.close()