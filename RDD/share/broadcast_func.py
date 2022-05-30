from pyspark import SparkContext

sc =SparkContext(master='yarn',appName='share_data')

# 定义一个变量
a=10
# 将变量值转化为广播变量
broadcast_data = sc.broadcast(a)


# 进入到executor中执行的
# 生成rdd
rdd = sc.parallelize([1,2,3,4])

# 计算
# broadcast_data.value获取广播变量值
# lambda x:x+broadcast_data.value   获取rdd中的元素数据，然后将每个数据加上广播变量值
rdd_map = rdd.map(lambda x:x+broadcast_data.value)
# 广播变量值不能被修改 ，如下的写法是错误
# broadcast_data.value=20
# broadcast_data.value=broadcast_data.value+1


# 执行输出
print(rdd_map.collect())