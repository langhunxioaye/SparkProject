# 模块导入
from pyspark import SparkContext
# sc对象
sc = SparkContext()

# 读取文件数据转化为rdd
rdd = sc.textFile('/datas/input')
# 打印读取到的rdd数据
print(rdd.collect())
# 将rdd中每行的元数据数据记性切割，获取到每个单词数据
# x 获取每行的数据 'spark python spark hive spark hive'，数据类型是字符串,可以通过split方法对字符串数据切割，获取每个单词数据
# split 返回是列表  默认的切割字符是空格  ，flatMap接收的返回值就是一个列表，会遍历列中每个数据存存到rdd中
rdd_flotMap = rdd.flatMap(lambda x:x.split())
print(rdd_flotMap.collect())

#countByValue可以统rdd中元素出现的次数
# rdd_count =  rdd_flotMap.countByValue()
# print(rdd_count

# map方法 转化为key-v的初始值
rdd_map = rdd_flotMap.map(lambda x:(x,1))
print(rdd_map.collect())

# 对转化后的k-v数据进行聚合计算
rdd_reduceBykey=  rdd_map.reduceByKey(lambda x,y:x+y)
print(rdd_reduceBykey.collect())
