#!/usr/bin/env python

# Input:
#  (motionid, elementid, gaussid, mean(NS0), mean(SIG_V), mean(CNUF))
#
# Output:
#  (elementid_gaussid, hour, mean(NS0), mean(SIG_V), mean(CNUF))

import sys

# This table contains 24 entries corresponding to 24 hours in a day.
# For each hour, it specifies the index of the motion sequence
# that describes the activity during the hour.
hour_motionids = [
	0,0,0,1,1,1,0,1,
	0,0,0,2,2,1,0,1,
	0,0,0,1,1,1,0,1
]

for line in sys.stdin:
  tuple = line.strip().split()
  assert len(tuple) == 6
  
  motionid = int(tuple[0])

  # Again, emit a composite key to enable reduction on pairs of
  # elementid, gaussid. Each time the motionid is used during
  # the day, a new entry is emitted using the number of the
  # hour as additional tuple element.
  key = tuple[1] + '_' + tuple[2]
  for hour, hour_motionid in enumerate(hour_motionids):
  	if hour_motionid != motionid:
  		continue
  	print '%s\t%i\t%s\t%s\t%s' % (key, hour, tuple[3], tuple[4], tuple[5])
  	
  	
  