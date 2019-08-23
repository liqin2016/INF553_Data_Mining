import sys
from pyspark import SparkContext
from itertools import combinations
import time

# spark-submit firstname_lastname_task1.py <case number> <support> <input_file_path> <output_file_path>
# caseNum = int(sys.argv[1])
caseNum = int(sys.argv[1])
supportThreshold = int(sys.argv[2])

time_start = time.time()

sc = SparkContext("local[*]", "Simple App")
# file:///
# dataFile = sc.textFile(sys.argv[3])
dataFile = sc.textFile(sys.argv[3])
dataFile = dataFile.repartition(2)
#
# remove duplicate
dataFile1 = dataFile.map(lambda x: x.split(","))
header = dataFile1.first()
dataFile1 = dataFile1.filter(lambda x: x != header)
if caseNum == 1:
    dataFile2 = dataFile1.map(lambda x: (x[0], x[1])).distinct()
elif caseNum == 2:
    dataFile2 = dataFile1.map(lambda x: (x[1], x[0])).distinct()
# dataFile3 = dataFile2.groupByKey().map(lambda x: (x[0], list(x[1]))).collect()
dataFile3 = dataFile2.groupByKey().map(lambda x: list(x[1]))
totalBasketNum = dataFile3.count()
# print(dataFile3.collect())
print(totalBasketNum)


def pass1(chunk):
    result = []
    chunk = list(chunk)
    localBasketNum = len(chunk)
    print("localBasketNum")
    print(localBasketNum)
    # get the local threshold
    localThreshold = (float)(supportThreshold * localBasketNum) / totalBasketNum
    print(localThreshold)

    # sigle part
    myCount = {}

# count every item
    for line in chunk:
        for item in line:
            if item not in myCount:
                myCount[item] = 1
            else:
                myCount[item] += 1
    freq_sets = set()
    for k in list(myCount.keys()):
        if myCount[k] >= localThreshold:
            # convert str to list then to frozenset
            freq_sets.add(frozenset([k]))

    # add freq single to final list
    for item in freq_sets:
        result.append(item)

    for chunkindex in range(0, localBasketNum):
        chunk[chunkindex] = frozenset(chunk[chunkindex])
    size = 2
    while (True):
        localCandidates = set()
        # create k+1 size
        freq_sets = list(freq_sets)
        for i in range(0, len(freq_sets)):
            for j in range(i + 1, len(freq_sets)):
                temp = freq_sets[i].union(freq_sets[j])
                if len(temp) == size:
                    localCandidates.add(temp)



        #check  localCandidates
        # reuse freq_sets to save k+1 freq
        myCount2 = {}
        for i in localCandidates:
            myCount2[i] = 0

        for a in chunk:
            for b in list(myCount2.keys()):
                if b.issubset(a):
                    myCount2[b] += 1
        # ini
        freq_sets =set()
        for k in list(myCount2.keys()):
            if myCount2[k] >= localThreshold:
                freq_sets.add(k)

        if len(freq_sets) == 0: break
        for item in freq_sets:
            result.append(item)

        # next loop generate k+1 size freq items
        size += 1

    return result


localFreqCandidate = dataFile3.mapPartitions(pass1).distinct().collect()
# print("1234567")
# print(localFreqCandidate)


def pass2(chunk):
    chunk = list(chunk)
    myCount3 ={}

    for i in localFreqCandidate:
        myCount3[i] = 0

    for b in list(myCount3.keys()):
        for a in chunk:
            if set(b).issubset(set(a)):
                myCount3[b] += 1

    return myCount3.items()


finalFrequent = dataFile3.mapPartitions(pass2).reduceByKey(lambda x, y: x + y).filter(lambda x: x[1] >= supportThreshold).map(lambda x: x[0]).collect()
# print(finalFrequent)


op = open(sys.argv[4], 'w')


def opListWrite(myList):
    maxLen = 0
    # sort part
    for i in range(len(myList)):
        myList[i] = list(myList[i])
        myList[i].sort()
        if len(myList[i]) > maxLen:
            maxLen = len(myList[i])

    myList.sort()

    for mySize in range(1, maxLen+1):
        firstElementFlag = 0
        for li in myList:
            if len(li) == mySize:
                firstElementFlag += 1
                if firstElementFlag == 1:
                    op.write("('")
                else:
                    op.write(",('")
                index = 0
                for j in li:
                    op.write(j)
                    index += 1
                    if index != mySize:
                        op.write("', '")

                op.write("')")

        op.write("\n\n")


op.write("Candidates:\n")
opListWrite(localFreqCandidate)
op.write("Frequent Itemsets:\n")
opListWrite(finalFrequent)


time_end1 = time.time()
m1 = time_end1 - time_start
print("Duration:"+str(m1))




