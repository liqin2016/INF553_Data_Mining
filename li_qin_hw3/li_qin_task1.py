import sys
from pyspark import SparkContext
from itertools import combinations
import time
import random

time_start = time.time()

sc = SparkContext("local[*]", "Assignment 3 LSH Task 1")

dataFile = sc.textFile(sys.argv[1])
# dataFile = sc.textFile(sys.argv[1])
# num_chunk = 2  # 4
#     lines = sc.textFile(sys.argv[1], num_chunk)
# use this statement when run so slow
# dataFile = dataFile.repartition(2)
#
# remove duplicate
dataFile1 = dataFile.map(lambda x: x.split(","))
header = dataFile1.first()
dataFile1 = dataFile1.filter(lambda x: x != header)



unique_user_id = dataFile1.map(lambda row: row[0]).distinct().collect()
unique_user_id.sort()

# index_user_map = {}
user_index_map = {}
user_index = 0
for user_item in unique_user_id:
    # index_user_map[i] = user_item
    user_index_map[user_item] = user_index
    user_index += 1
# use index to represent business_id string in rdd  (business_id,int for user)
dataFile2 = dataFile1.map(lambda x: (x[1], user_index_map[x[0]])).distinct()
dataFile3 = dataFile2.groupByKey().map(lambda x: (x[0], list(x[1])))
movie_user_list = dataFile3.collect()
print(movie_user_list)
totalBusiness = dataFile3.count()
totalUser = len(unique_user_id)
print("totalBusiness")
print(totalBusiness)
print("totalU")
print(totalUser)

# get hash index
hashing = {}
hash_fuc_num = 150
band_num =50
row_num=3
# for i in range(totalUser):
#     hashing[i] = list()
#     for hashIndex in range(hash_fuc_num):
#         hashing[i].append((5 * i + 13 * hashIndex) % totalUser)
for hashIndex in range(hash_fuc_num):
    hashing[hashIndex] = list()
    tempRandomSet = set()
    for i in range(totalUser):
        randIndex = random.randint(0, totalUser)
        while randIndex in tempRandomSet:
            randIndex = random.randint(0, totalUser)
        tempRandomSet.add(randIndex)
        hashing[hashIndex].append(randIndex)

#40 hash fuction   8 bands  5 rows
print(hashing.items())


def getSignature(iteration):
    iteration = list(iteration)
    signatureMap = {}
    for line in iteration:
        signatureMap[line[0]] = list()

    for hashIndex in range(hash_fuc_num):
        for line in iteration:
            bussinessStr = line[0]
            tempList = list()
            # int in line to represent user
            for eachUser in line[1]:
                tempList.append(hashing[hashIndex][eachUser])
            minVal = tempList[0]
            for val in tempList:
                if val < minVal:
                    minVal = val
            signatureMap[bussinessStr].append(minVal)
    signatureList = list()
    for bussinessStr in signatureMap.keys():
        signatureList.append((bussinessStr, signatureMap[bussinessStr]))
    return signatureList

def lsh(sigList):
    sigListLen = len(sigList)
    candidate_pair_res =[]
    for i in range(0, sigListLen):
        for j in range(i + 1, sigListLen):
            for b in range(0, band_num):
                # list[tuple(str,list[])]
                # check if bands are identical
                print("lsh compare")
                if sigList[i][1][b*row_num] == sigList[j][1][b*row_num]:
                    if sigList[i][1][b*row_num+1] == sigList[j][1][b*row_num+1]:
                        if sigList[i][1][b*row_num+2] == sigList[j][1][b*row_num+2]:
                            candidate_pair_res.append((sigList[i][0], sigList[j][0]))
                            break
    return candidate_pair_res


def band_partition(x):
    band_list = []
    for band_index in range(band_num):
        band_list.append((str(band_index) + "@" + str(x[1][band_index*row_num]) + "@"+str(x[1][band_index*row_num+1]) + "@"+str(x[1][band_index*row_num+2]), x[0]))
    return band_list


def findPairs(same_band_list):
    pairList = []
    for i in range(len(same_band_list)):
        for j in range(i + 1, len(same_band_list)):
            # sort in alpha order
            if same_band_list[i] <= same_band_list[j]:
                pairList.append((same_band_list[i], same_band_list[j]))
            else:
                pairList.append((same_band_list[j], same_band_list[i]))
    return pairList



temp = dataFile3.mapPartitions(getSignature).flatMap(band_partition).groupByKey().mapValues(list).filter(lambda x: len(x[1]) > 1)
temp1 = temp.flatMap(lambda x: findPairs(x[1])).distinct().collect()

print(temp.collect())
print(temp1)
movie_map = {}

for line in movie_user_list:
    movie_map[line[0]] = set(line[1])


business_pair_final_list = []
for line in temp1:
    JacSim = float(len(movie_map[line[0]] & movie_map[line[1]])) / float(len(movie_map[line[0]] | movie_map[line[1]]))
    if JacSim >= 0.5:
        business_pair_final_list.append((line[0], line[1], JacSim))

business_pair_final_list.sort()
print(business_pair_final_list)

op = open(sys.argv[2], 'w')
# op = open("hw4task1ans.csv", 'w')
op.write("business_id_1, business_id_2, similarity\n")
for line in business_pair_final_list:
    op.write(str(line[0]))
    op.write(",")
    op.write(str(line[1]))
    op.write(",")
    op.write(str(line[2]))
    op.write("\n")

time_end1 = time.time()
m1 = time_end1 - time_start
print("Duration:"+str(m1))




