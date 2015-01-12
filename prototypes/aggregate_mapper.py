#!/usr/bin/env python

# Input:
#  (motionid, elementid, gaussid, timestep, name, value)
#
# Output:
#  (motionid_elementid_gaussid, nameid, value)

import sys

# This table maps variable names to a simple numeric index
namemap = {
  'NS0'   : 0,
  'SIG_V' : 1,
  'CNUF'  : 2
}

for line in sys.stdin:
  tuple = line.strip().split(',')
  assert len(tuple) == 6

  # Replace the variable name with a numeric index
  # to reduce the size of intermediate HDFS storage.
  nameid = namemap[tuple[4]]

  # Emit the concatenation of the primary key 3-tuple as key
  # using _ to separate the parts. This has the effect that
  # values with the same primary key are mapped to the
  # same Reducer shard while we can still reconstruct the
  # original key later.
  key = '_'.join(tuple[:3])
  print '%s\t%i\t%s' % (key, nameid, tuple[5])
  