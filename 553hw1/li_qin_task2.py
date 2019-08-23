import json
import os, sys
import time
from pyspark import SparkContext


sc = SparkContext("local[*]", "Simple App")
# file:///
dataFile1 = sc.textFile(sys.argv[1])
dataFile2 = sc.textFile(sys.argv[2])
dataJson1 = dataFile1.map(lambda x: json.loads(x))
dataJson2 = dataFile2.map(lambda x: json.loads(x))

# qA = dataJson1.map(lambda x: (x["business_id"], (x["stars"], 1)))
# reduceRdd = qA.reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
# business_stars = reduceRdd.mapValues(lambda x: x[0]/x[1])
business_stars = dataJson1.map(lambda x: (x["business_id"], x["stars"]))


business_state = dataJson2.map(lambda x: (x["business_id"], x["state"]))
newRdd = business_state.join(business_stars).map(lambda x: (x[1][0], (x[1][1], 1))) \
    .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1])) \
    .mapValues(lambda x: x[0]/x[1]).sortBy(lambda x: (-x[1], x[0]))


task2ARes = newRdd.collect()
output = open(sys.argv[3], "w")
output.write("state,stars\n")
for line in task2ARes:
    output.write((str)(line[0]) + ',' + (str)(line[1]) + '\n')



#method1
time_start = time.time()
i=0
task2B1 = newRdd.collect()
for line in task2B1:
    i += 1
    print((str)(line))
    if i == 5:
        break

time_end1 = time.time()
#method2
print(newRdd.take(5))
time_end2 = time.time()

m1 = time_end1 - time_start
m2 = time_end2 - time_end1

print(m1)
print(m2)
output = open(sys.argv[4], "w")
output.write("{\n")
output.write(" \"m1\":"+(str)(m1) + ',\n')
output.write(" \"m2\":"+(str)(m2) + ',\n')
output.write(" \"explanation\":" + "\" Method2 is faster than Method1 because method1 coverts the whole rdd to list, method2 only take the fist five lines. So method2 is faster\"")
output.write("\n}")




