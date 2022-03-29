from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
import pyspark

conf = pyspark.SparkConf().setAppName('RDDBasics').setMaster('local') 
sc = pyspark.SparkContext(conf=conf)
spark = SparkSession(sc)

"""Create RDD"""
a = ["spark", "rdd", "python",
     "context", "create", "class"]
b = ["operation", "apache", "scala", "lambda",
     "parallel", "partition"]
 ##creating a functions that add value to each content in an rdd 
 def add_key(value):
    return (value,1)
rdd1 = sc.parallelize(a) 
rdd1 = rdd1.map(add_key) 
rdd1.collect()

rdd2 = sc.parallelize(b)
rdd2 = rdd2.map(add_key) 
rdd2.collect()
## Performing rightOuterJoin
roj = rdd1.rightOuterJoin(rdd2)
roj.collect()
## performing fullOuterJoin
foj=rdd1.fullOuterJoin(rdd2)
foj.collect()

### 2. Using map and reduce functions to count how many times the character "s" appears in all a and b
from operator import add
a = ["spark", "rdd", "python", "context", "create", "class"]
b = ["operation", "apache", "scala", "lambda","parallel","partition"] 
#function to count and return the number of occurences of 's' in its parameter 
def find_s(x):
    x = list(x) 
    c =0
    for i in x:
        if i == 's': c+=1
    return c

rdd3 = sc.parallelize(a)
rdd4 = sc.parallelize(b)

#Combining RDD's for input's a and b
rdd1_2 = sc.union([rdd3,rdd4])

#Mapping Input words containing 's' to the number of 's's in that word and adding them up
rdd_s = rdd1_2.map(find_s).filter(lambda x: x is not None).reduce(add) 
print(rdd_s)

### 3.Using aggregate function to count how many times the character "s" appears in all a and b
from operator import add
a = ["spark", "rdd", "python", "context", "create", "class"]
b = ["operation", "apache", "scala", "lambda","parallel","partition"]
def find_s(x):
    x = list(x) 
    c =0
    for i in x:
        if i == 's': c+=1
    return c

rdd5 = sc.parallelize(a)
rdd6 = sc.parallelize(b)

rdd5_6= sc.union([rdd5,rdd6])

rdd_agg = rdd5_6.filter(lambda x: 's' in x).map(find_s).sum() 
print(rdd_agg)
