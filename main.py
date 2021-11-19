import json
from pyspark import SparkContext
from pyspark.sql import SQLContext,SparkSession
from pyspark.streaming import StreamingContext
import sys

def helper(x):
    myList = x.collect()
    if(myList):
        d = json.loads(myList[0])
        df = sc.parallelize(d.values()).toDF()
        df.show(5)

sc = SparkContext.getOrCreate()
sqlContext = SQLContext(sc)
spark = SparkSession(sc)
ssc = StreamingContext(sc,5)

streamDS = ssc.socketTextStream('localhost',6100)

streamDS.foreachRDD(helper)

ssc.start()
ssc.awaitTermination()