from pyspark import SparkContext
sc = SparkContext(master='yarn', appName='rdd2')
rdd = sc.textFile('/datas/input/words.txt')
print(rdd.collect())
print(rdd.collect())
