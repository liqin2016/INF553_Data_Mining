import sys
from pyspark import SparkContext, SparkConf
from pyspark.mllib.recommendation import ALS
import time
import math

train_file_name = sys.argv[1]
test_file_name = sys.argv[2]
case_id = sys.argv[3]
output_file_name = sys.argv[4]

time_start = time.time()
sc = SparkContext("local[*]", "inf553hw3task2")

dataFile = sc.textFile(train_file_name)
# dataFile = dataFile.repartition(2)

dataFile1 = dataFile.map(lambda x: x.split(","))
header = dataFile1.first()
dataFile1 = dataFile1.filter(lambda x: x != header)

# 4 dict  str-int
unique_user_id = dataFile1.map(lambda row: row[0]).distinct().collect()
unique_user_id.sort()
user_index_map = {}
index_user_map = {}
user_index = 0
for j in unique_user_id:
    user_index_map[j] = user_index
    index_user_map[user_index] = j
    user_index += 1

unique_business_id = dataFile1.map(lambda row: row[1]).distinct().collect()
unique_business_id.sort()
business_index_map = {}
index_business_map = {}
business_index = 0
for j in unique_business_id:
    business_index_map[j] = business_index
    index_business_map[business_index] = j
    business_index += 1

train_data_file = dataFile1.map(lambda x: (user_index_map[x[0]], business_index_map[x[1]], float(x[2])))
# train_data_file   int int float

testRdd = sc.textFile(test_file_name).map(lambda x: x.split(","))
header1 = testRdd.first()
testRdd =testRdd.filter(lambda x: x != header)
# test_data_file = testRdd.map(lambda x: (x[0], x[1]))
# print(test_data_file.count())


if case_id == 1:

    def cutPred(x):
        # x: pred rate
        if x > 5:
            return 5.0
        elif x < 1:
            return 1.0
        else:
            return x

    # test_data_file = testRdd.map(lambda x: (user_index_map[x[0]], business_index_map[x[1]]))
    rank = 4
    numIterations = 20

    model = ALS.train(train_data_file, rank, numIterations)

    def makeIntTestList(line):
        if line[0] not in user_index_map or line[1] not in business_index_map:
            return (0, 0)
        return (user_index_map[line[0]], business_index_map[line[1]])

    testKey = testRdd.map(lambda x: makeIntTestList(x))
    model_pred = model.predictAll(testKey).map(lambda l: (index_user_map[l[0]], index_business_map[l[1]], cutPred(l[-1]))).collect()
    pred_dict={}
    for line in model_pred:
        if (line[0], line[1]) not in pred_dict:
            pred_dict[(line[0], line[1])] =line[2]


    def matchRating(line):
        if (line[0], line[1]) not in pred_dict:
            return (line[0], line[1], 3.0)
        return (line[0], line[1], pred_dict[(line[0], line[1])])

    ratingPrediction = testRdd.map(lambda x: matchRating(x)).collect()

elif case_id == 2:
    # dictTrain = {}
    # # key user bus v rating
    # for e in train_data_file.collect():
    #     dictTrain[(e[0], e[1])] = e[2]

    # dictAvg[usr_index] =avg
    userAvg = train_data_file.map(lambda x: (x[0], [x[2]])).reduceByKey(lambda x, y: x + y).map(lambda x: (x[0], sum(x[1]) / len(x[1])))
    dictAvg = {}
    for e in userAvg.collect():
        dictAvg[e[0]] = e[1]

    bToUser = train_data_file.map(lambda x: (x[0], [(x[1], x[2])])).reduceByKey(lambda x, y: x + y)
    udict ={}
    for line in bToUser.collect():
        udict[line[0]] = {}
        for irPair in line[1]:
            udict[line[0]][irPair[0]] = irPair[1]

    bdict = {}
    uToB = train_data_file.map(lambda x: (x[1], [(x[0], x[2])])).reduceByKey(lambda x, y: x + y)
    for line in uToB.collect():
        bdict[line[0]] = {}
        for urPair in line[1]:
            bdict[line[0]][urPair[0]] = urPair[1]

    def compute_w(e1, e2):
        corate_set = set(udict[e1].keys()) & set(udict[e2].keys())
        if len(corate_set) == 0:
            return 0.0
        e1r = []
        e2r = []
        for each in corate_set:
            e1r.append(udict[e1][each])
            e2r.append(udict[e2][each])

        e1r_avg = sum(e1r) / len(e1r)
        e2r_avg = sum(e2r) / len(e2r)

        numerator = 0.0
        # same len
        for i in range(len(e1r)):
            e1r[i] = e1r[i] - e1r_avg
            e2r[i] = e2r[i] - e2r_avg
            numerator += e1r[i]*e2r[i]

        if numerator == 0.0:
            return 0.0

        denominator = pow(sum([each ** 2 for each in e1r]), 0.5) * pow(sum([each ** 2 for each in e2r]), 0.5)
        return numerator/denominator


    def predictFunc(t_u, t_b):
        if t_u not in user_index_map or t_b not in business_index_map:
            return 3.0
        # convert to int
        t_u = user_index_map[t_u]
        t_b = business_index_map[t_b]

        numerator = 0.0
        denominator = 0.0

        for coratedItem in bdict[t_b].keys():
            w = compute_w(t_u, coratedItem)
            if w > 2.5:
                numerator += (udict[coratedItem][t_b] - dictAvg[coratedItem]) * w
                # denominator += abs(w)
                denominator += w
        predictRate = dictAvg[t_u]

        if denominator > 0:
            predictRate += numerator / denominator
        # rate data from yelp only has 1.0 2.0 3.0 4.0 5.0  0.5 is not a proper predictRate
        if predictRate > 5.0:
            return 5.0
        elif predictRate < 1:
            return 1.0
        else:
            return predictRate

    ratingPrediction = testRdd.map(lambda x: (x[0], x[1], predictFunc(x[0], x[1]))).collect()
# end of case 2
else:
    userAvg = train_data_file.map(lambda x: (x[0], [x[2]])).reduceByKey(lambda x, y: x + y).map(
        lambda x: (x[0], sum(x[1]) / len(x[1])))
    dictAvg = {}
    for e in userAvg.collect():
        dictAvg[e[0]] = e[1]

    bToUser = train_data_file.map(lambda x: (x[0], [(x[1], x[2])])).reduceByKey(lambda x, y: x + y)
    udict = {}
    for line in bToUser.collect():
        udict[line[0]] = {}
        for irPair in line[1]:
            udict[line[0]][irPair[0]] = irPair[1]

    bdict = {}
    uToB = train_data_file.map(lambda x: (x[1], [(x[0], x[2])])).reduceByKey(lambda x, y: x + y)
    for line in uToB.collect():
        bdict[line[0]] = {}
        for urPair in line[1]:
            bdict[line[0]][urPair[0]] = urPair[1]


    def compute_w(e1, e2):
        corate_set = set(udict[e1].keys()) & set(udict[e2].keys())
        if len(corate_set) == 0:
            return 0.0
        e1r = []
        e2r = []
        for each in corate_set:
            e1r.append(udict[e1][each])
            e2r.append(udict[e2][each])

        e1r_avg = sum(e1r) / len(e1r)
        e2r_avg = sum(e2r) / len(e2r)

        numerator = 0.0
        # same len
        for i in range(len(e1r)):
            e1r[i] = e1r[i] - e1r_avg
            e2r[i] = e2r[i] - e2r_avg
            numerator += e1r[i] * e2r[i]

        if numerator == 0.0:
            return 0.0

        denominator = pow(sum([each ** 2 for each in e1r]), 0.5) * pow(sum([each ** 2 for each in e2r]), 0.5)
        return numerator / denominator


    def predictFunc(t_u, t_b):
        if t_u not in user_index_map or t_b not in business_index_map:
            return 3.6
        # convert to int
        t_u = user_index_map[t_u]
        t_b = business_index_map[t_b]

        numerator = 0.0
        denominator = 0.0

        for coratedItem in bdict[t_b].keys():
            w = compute_w(t_u, coratedItem)
            if w > 3.0:
                numerator += (udict[coratedItem][t_b] - dictAvg[coratedItem]) * w
                # denominator += abs(w)
                denominator += w
        predictRate = dictAvg[t_u]

        if denominator > 0:
            predictRate += numerator / denominator
        # rate data from yelp only has 1.0 2.0 3.0 4.0 5.0  0.5 is not a proper predictRate
        if predictRate > 5.0:
            return 5.0
        elif predictRate < 1:
            return 1.0
        else:
            return predictRate


    ratingPrediction = testRdd.map(lambda x: (x[0], x[1], predictFunc(x[0], x[1]))).collect()
# file write
op = open(output_file_name, 'w')
op.write("user_id, business_id, stars\n")
for line in ratingPrediction:
    op.write(str(line[0]))
    op.write(",")
    op.write(str(line[1]))
    op.write(",")
    op.write(str(line[2]))
    op.write("\n")

time_end1 = time.time()
m1 = time_end1 - time_start
print("Duration:"+str(m1))


