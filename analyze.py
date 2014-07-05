import statistics
import json
import sys

data = json.loads(sys.stdin.read())

counts = data.values()
print(data)
print(counts)
print("Mean:\t%s" % (statistics.mean(counts)))
print("StdDev:\t%s" % (statistics.stdev(counts)))
print("Median:\t%s" % (statistics.median(counts)))
print("Max:\t%s" % (max(counts)))
print("Min:\t%s" % (min(counts)))

