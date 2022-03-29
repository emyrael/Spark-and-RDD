import pyspark
from pyspark import sql
from pyspark.sql.window import Window
import pyspark.sql.functions as F
import sys
from pyspark.sql import Row
from pyspark.sql.dataframe import DataFrame
from operator import add

################################################################################################
#1. A tagging session for a user can be dened as the duration in which he/she generated tagging
#activity. Typically, an inactive duration of 30 mins is
# considered termination of the tagging session.
#Your task is to first separate out tagging sessions for each user.

path_to_dat = 'tags.dat'
with open(path_to_dat , 'r', encoding='utf-8') as f:
    ids_list = f.readlines()

ids_list = list(map(lambda x: x.strip().split('::'), ids_list))
ids_list = list(map(lambda x: [int(x[0]), int(x[1]), x[2], int(x[3])], ids_list))

cols = ['UserID', 'MovieID', 'category', 'Timestamp']
df = sqlContext.createDataFrame(data = ids_list, schema = cols).sort(['UserID', 'Timestamp'],
                                                                     ascending = True)
my_window = Window.partitionBy("UserID").orderBy("Timestamp")
df = df.withColumn('prevTime', F.lag(df.Timestamp).over(my_window))
df = df.withColumn("diff", F.when(F.isnull(df.Timestamp - df.prevTime), 0)
                              .otherwise(df.Timestamp - df.prevTime))
df = df.drop("prevTime")

df = df.withColumn("IsLongTime", (df.diff > 30*60).cast('int'))
df = df.drop("diff")

my_window = Window.partitionBy("UserID").orderBy("Timestamp")
df = df.withColumn('SessionNum',
    F.sum(df.IsLongTime).over(Window.partitionBy('UserID').orderBy('Timestamp').rowsBetween(-sys.maxsize, 0)))

df = df.drop("IsLongTime")

df.show()

###################################################################################################
#2. In each tagging session, calculate the frequency of tagging for each user and report those users who
#have the distance of two standard deviation between their tagging frequency per session and mean.

df = df.withColumn("Ones", (df.Timestamp > -1000).cast('int'))

my_window = Window.partitionBy(["UserID", "SessionNum"]).orderBy("SessionNum")

df = df.withColumn("MaxTime", F.max(df.Timestamp).over(my_window))
df = df.withColumn('MinTime', F.min(df.Timestamp).over(my_window))
df = df.withColumn("RangeTime", df.MaxTime - df.MinTime)
df = df.drop("MaxTime")
df = df.drop('MinTime')

df = df.withColumn("Num", F.sum(df.Ones).over(my_window))
df = df.withColumn("Frequency", F.when(F.isnull(df.Num/df.RangeTime), -1)
                              .otherwise(df.Num/df.RangeTime))
df = df.drop("Num")
df = df.drop("Ones")

my_window = Window.partitionBy(["UserID"]).orderBy("SessionNum")

df = df.withColumn("MeanFreq", F.mean(df.Frequency).over(my_window))
df = df.withColumn("DispFreq", F.mean((df.Frequency - df.MeanFreq)**2).over(my_window))
df = df.withColumn("StdFreq", df.DispFreq**0.5)

df = df.drop("DispFreq")

df = df.where((df.Frequency - df.MeanFreq > 3*df.StdFreq) | (df.Frequency - df.MeanFreq > -3*df.StdFreq))

res = df.select("UserID").distinct().collect()
res = list(set(map(lambda x: x[0], res)))


print("##############################################################")
print("Results (IDs) B2: {}".format(res))
