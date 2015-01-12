#!/usr/bin/env python

# Input:
#  (elementid_gaussid, hour, mean(NS0), mean(SIG_V), mean(CNUF))
#
# Output:
#  (elementid, gaussid, NSTS, HCF)

import sys
import itertools
import random
import subprocess

# Runner inserts the Octave script's source code here
octave_src = OCTAVE_SOURCE_CODE_INSERT

# Write the octave script to a temporary file
octave_unpack_file = str(random.randint(0, 1000)) + 'o.m'
with open(octave_unpack_file, 'wt') as octout:
	octout.write(octave_src)
                
# Start octave given the script, setup a bi-directional pipe
# on stdin and stdout. bufsize=1 prevents unwanted line
# buffering.
proc = subprocess.Popen(['octave', '--silent', octave_unpack_file],
	stdout=subprocess.PIPE,
	stdin=subprocess.PIPE,
	bufsize=1)

# Incoming elements are grouped by elementid_gaussid, but not
# not ordered with respect to the hour column. Thus, sort
# first. This is sometimes referred to as a "Secondary Sort"
# and can outside streaming mode also be achieved by using a
# combination of a custom Hadoop partitioner with a custom
# equality predicate.
lines = (line.split() for line in sys.stdin)
for key, values in itertools.groupby(lines, lambda x : x[0]):
	ordered_values = sorted(values, key=lambda x : int(x[1]))
	
	# For some reason, two linefeeds are needed to have
	# Octave's fgetl() call return.
	proc.stdin.write(' '.join([s[-1] for s in ordered_values]) + '\r\n\r\n')
	proc.stdin.flush()
	outvars = proc.stdout.readline().split()
		
	# Emit the result with the original (split) key
  	print '%s,%s,%s,%s' % tuple(key.split('_') + outvars)

# Just let the pipe (and thus Octave) die

  	
  	
  