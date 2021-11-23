from pyspark import SparkContext
from pyspark.sql import SQLContext,SparkSession,Row
from pyspark.streaming import StreamingContext
from pyspark.ml.feature import Tokenizer,StopWordsRemover, HashingTF,IDF,StringIndexer
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.linalg import Vector
from pyspark.ml import Pipeline
from pyspark.ml.functions import vector_to_array
from sklearn.naive_bayes import MultinomialNB
import json
import pickle

def getSqlContextInstance(sparkContext):
    if ('sqlContextSingletonInstance' not in globals()):
        globals()['sqlContextSingletonInstance'] = SQLContext(sparkContext)
    return globals()['sqlContextSingletonInstance']

def process(time, rdd):
    print("========= %s =========" % str(time))
    if(not rdd.isEmpty()):
        flag[0] = 0
        sqlContext = getSqlContextInstance(rdd.context)
        df = sqlContext.createDataFrame(rdd)
        preprocess = data_prep_pipe.fit(df)
        clean_data = preprocess.transform(df)
        clean_data = clean_data.select(['label','features'])
        clean_data.show(5)
        clean_data = clean_data.withColumn('features', vector_to_array('features'))
        X = clean_data.select('features').rdd.map(lambda x:x['features']).collect()
        Y = clean_data.select('label').rdd.map(lambda x:x['label']).collect()
        multinomial_nb.partial_fit(X,Y,classes=[0,1])
        print('working')
    else:
        flag[0] += 1
        if(flag[0]>2):
            pickle.dump(multinomial_nb, open('MultinomialNB.sav', 'wb'))
            ssc.stop()

sc = SparkContext.getOrCreate()
#sqlContext = SQLContext(sc)
spark = SparkSession(sc)
ssc = StreamingContext(sc,5)

tokenizer = Tokenizer(inputCol="text", outputCol="token_text")
stopremove = StopWordsRemover(inputCol='token_text',outputCol='stop_tokens')
count_vec = HashingTF(inputCol='stop_tokens',outputCol='c_vec',numFeatures=4096)
idf = IDF(inputCol="c_vec", outputCol="tf_idf")
ham_spam_to_num = StringIndexer(inputCol='classLabel',outputCol='label')
clean_up = VectorAssembler(inputCols=['tf_idf'],outputCol='features')

flag = [0]

data_prep_pipe = Pipeline(stages=[ham_spam_to_num,tokenizer,stopremove,count_vec,idf,clean_up])

multinomial_nb = MultinomialNB()

streamDS = ssc.socketTextStream('localhost',6100)

parsed_JSON_DS = streamDS.map(lambda x : json.loads(x))

list_JSON_DS = parsed_JSON_DS.flatMap(lambda x:x.values())

rows_DS = list_JSON_DS.map(lambda x: Row(text1=x['feature0'],text=x['feature1'],classLabel=x['feature2']))

rows_DS.foreachRDD(process)
        
ssc.start()
ssc.awaitTermination()