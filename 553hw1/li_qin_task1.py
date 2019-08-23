import json
import os, sys
from pyspark import SparkContext


def g(x):
    print(x)


sc = SparkContext("local[*]", "Simple App")
# file:///
dataFile = sc.textFile(sys.argv[1])
dataJson = dataFile.map(lambda x: json.loads(x))
# A. The number of reviews that people think are useful (The value of tag ‘useful’ > 0)
qA = dataJson.map(lambda x: x["useful"]).filter(lambda x: x > 0).count()
print(qA)
# qA.foreach(g)
# B. The number of reviews that have 5.0 stars rating (1 point)
qB = dataJson.map(lambda x: x["stars"]).filter(lambda x: x > 4.0).count()
print(qB)
# C. How many characters are there in the ‘text’ of the longest review
qC = dataJson.map(lambda x: x["text"]).sortBy(lambda x: len(x), False).first()
print(len(qC))
# D. The number of distinct users who wrote reviews (1 point)
qD = dataJson.map(lambda x: x["user_id"]).distinct().count()
# qD.foreach(g)
print(qD)
# E. The top 20 users who wrote the largest numbers of reviews and the number of reviews they wrote (1 point)
qE = dataJson.map(lambda x: (x["user_id"], 1)).reduceByKey(lambda x, y: x+y).sortBy(lambda x: x[1], False).take(20)
print(qE)
# F. The number of distinct businesses that have been reviewed (1 point)
qF = dataJson.map(lambda x: x["business_id"]).distinct().count()
print(qF)
# G. The top 20 businesses that had the largest numbers of reviews and the number of reviews they had (1 point)
qG = dataJson.map(lambda x: (x["business_id"], 1)).reduceByKey(lambda x, y: x+y).sortBy(lambda x: x[1], False).take(20)
print(qG)

output = open(sys.argv[2], "w")
output.write("{\n")
output.write(" \"n_review_useful\":"+(str)(qA) + ',\n')
output.write(" \"n_review_5_star\":"+(str)(qB) + ',\n')
output.write(" \"n_characters\":"+(str)(len(qC)) + ',\n')
output.write(" \"n_user\":"+(str)(qD) + ',\n')
output.write(" \"top20_user\": [")
i = 0
for line in qE:
    i += 1
    output.write("[\"" +(str)(line[0]) + "\"," + (str)(line[1]) + "]")
    if i < 20:
        output.write(",")
output.write("],\n")

output.write(" \"n_business\":"+(str)(qF) + ',\n')
output.write(" \"top20_business\": [")
i = 0
for line in qG:
    i += 1
    output.write("[\"" + (str)(line[0]) + "\"," + (str)(line[1]) + "]")
    if i < 20:
        output.write(",")
output.write("],\n}")

#
#
#sc.saveAsTextFile("file://")



# where to use    coalesce(numPartitions, shuffle=False)[source]