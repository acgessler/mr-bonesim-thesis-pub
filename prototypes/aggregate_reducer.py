#!/usr/bin/env python

# Input:
#  (motionid_elementid_gaussid, nameid, value)
#
# Output:
#  (motionid, elementid, gaussid, mean(NS0), mean(SIG_V), mean(CNUF))

import sys
import itertools

mean = [0] * 3

lines = (line.split() for line in sys.stdin)
for key, values in itertools.groupby(lines, lambda x : x[0]):

  # Iterative mean calculation
  # See The Art of Computer Programming, Volume 2, section 4.2.2 [Knu97]

  mean[0] = mean[1] = mean[2] = 0.0
  for index, value in enumerate(values):
    assert len(value) == 3
    
    nameid = int(value[1])
    mean[nameid] += (float(value[2]) - mean[nameid]) / (index + 1)

  # Emit the average values with the original (split) key
  print '%s\t%s\t%s\t%f\t%f\t%f' % tuple(key.split('_') + mean)