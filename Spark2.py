import sys
import pyspark
from pyspark import sql
from pyspark.sql.window import Window
import pyspark.sql.functions as F
import sys
from pyspark.sql import Row
from pyspark.sql.dataframe import DataFrame
from operator import add
from datetime import datetime


def daytime(x):
    if x <= 6:
        return 'Night'
    elif x <= 12:
        return 'Morning'
    elif x <= 18:
        return 'Day'
    elif x <= 24:
        return 'Evening'


def timestamp_to_daytime(x):
    x = datetime.fromtimestamp(int(x)).hour
    x = daytime(x)
    return x

conf = pyspark.SparkConf().setAppName('exercise_2').setMaster('yarn')
sc = pyspark.SparkContext(conf=conf)
sc.setLogLevel("ERROR")
sqlContext = sql.SQLContext(sc)

##Reading the input file from the hdfs 

ids_list = sc.read.csv("hdfs://localhost:9000/ratings/ratings.csv", header =True,)

ids_list = list(map(lambda x: x.strip().split('::'), ids_list))
ids_list = list(map(lambda x: [int(x[0]),
                               int(x[1]),
                               float(x[2]),
                               timestamp_to_daytime(x[3])], ids_list))

cols = ['UserID', 'MovieID', 'Rating', 'DayTime']

df = sqlContext.createDataFrame(data=ids_list,
                                schema=cols).sort(['UserID'], ascending=True)

my_window = Window.partitionBy("UserID").orderBy("UserID")

#########################################################################################
df1 = df.withColumn('counter', (df.UserID >= 0).cast("int"))
df1 = df1.withColumn('Count', F.sum(df1.counter).over(my_window))
df1 = df1.filter(df1.Count < 60)
df1 = df1.drop("counter")
df1 = df1.drop('Count')
df1 = df1.withColumn('MeanRating', F.mean(df1.Rating).over(my_window))
df1.groupBy('UserID').max("MeanRating")

res1 = df1.select("UserID").distinct().collect()[0]
print("User with max mean rating: {}".format(res1))
#########################################################################################
my_window = Window.partitionBy("UserID", "DayTime").orderBy("UserID")
df1 = df.withColumn('counter', (df.UserID >= 0).cast("int"))
df1 = df1.withColumn('Count', F.sum(df1.counter).over(my_window))
df1.groupBy('UserID', 'DayTime').max('Count').show()
