from pyspark import SparkContext
sc = SparkContext()

rdd = sc.textFile('file:/export/server/data/words.txt')
rdd_flotMap = rdd.flatMap(lambda x:x.split())
rdd_map = rdd_flotMap.map(lambda x:(x,1))
rdd_reduceBykey=  rdd_map.reduceByKey(lambda x,y:x+y)
print(rdd_reduceBykey.collect())
rdd_reduceBykey.saveAsTextFile('hdfs://192.168.88.100:8020/datas/words.txt')
