from pyspark.sql import SQLContext
from pyspark.ml.functions import vector_to_array
from pyspark.sql.functions import concat_ws
from evaluate import *
import pickle
import numpy as np

def getSqlContextInstance(sparkContext):
    if ('sqlContextSingletonInstance' not in globals()):
        globals()['sqlContextSingletonInstance'] = SQLContext(sparkContext)
    return globals()['sqlContextSingletonInstance']

def preprocessAndTrainModel(time,rdd,ssc,stop_flag,data_prep_pipe,model):
    print()
    print("========= %s =========" % str(time))
    
    if(not rdd.isEmpty()):
        stop_flag[0] = 0
        sqlContext = getSqlContextInstance(rdd.context)
        df1 = sqlContext.createDataFrame(rdd)
        df = df1.select(concat_ws(' ',df1.subject,df1.body).alias("text"),"classLabel")
        preprocess = data_prep_pipe.fit(df)
        clean_data = preprocess.transform(df)
        clean_data = clean_data.select(['label','features'])
        clean_data.show(5)
        clean_data = clean_data.withColumn('features', vector_to_array('features'))
        X = clean_data.select('features').rdd.map(lambda x:x['features']).collect()
        Y = clean_data.select('label').rdd.map(lambda x:x['label']).collect()
        np.random.seed(3)
        model.weights.partial_fit(X,Y,classes=[0,1])
        print('working')
    else:
        stop_flag[0] += 1
        if(stop_flag[0]>2):
            #pickle.dump(model.weights, open(model.name+'6.sav', 'wb'))
            pickle.dump(model.weights, open('MLP.sav', 'wb'))
            ssc.stop()

def preprocessAndTestModel(time,rdd,ssc,stop_flag,data_prep_pipe,model):
    print()
    print("========= %s =========" % str(time))
    if(not rdd.isEmpty()):
        stop_flag[0] = 0
        sqlContext = getSqlContextInstance(rdd.context)
        df1 = sqlContext.createDataFrame(rdd)
        df = df1.select(concat_ws(' ',df1.subject,df1.body).alias("text"),"classLabel")
        preprocess = data_prep_pipe.fit(df)
        clean_data = preprocess.transform(df)
        clean_data = clean_data.select(['label','features'])
        #clean_data.show(10)
        clean_data = clean_data.withColumn('features', vector_to_array('features'))
        X = clean_data.select('features').rdd.map(lambda x:x['features']).collect()
        Y = clean_data.select('label').rdd.map(lambda x:x['label']).collect()
        np.random.seed(3)
        Y_pred = model.weights.predict(X)
        
        evaluate(Y,Y_pred)
        
    else:
        stop_flag[0] += 1
        if(stop_flag[0]>2):
            ssc.stop()