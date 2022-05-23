from pyspark import SparkContext

# 1、生成sc对象
# 提升计算速度可以使用本地模式
sc = SparkContext()

# 2、将文件数据转为rdd数据
#hdfs的文件数据 （实际开发都是读取hdfs文件数据）
# 需要填写hdfs文件的路径位置，按行读取数据加载到rdd中
rdd = sc.textFile('/datas/input')

# 本地服务器的文件数据（支持本地模式，在集群模式无法读取本地服务器文件）
rdd2 = sc.textFile('file:///root/words.txt')


# 3、rdd计算

# 4 获取计算结果
print(rdd.collect())
print(rdd2.collect())