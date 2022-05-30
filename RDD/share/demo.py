from pyspark import SparkContext

sc =SparkContext()
# 需求： 统计rdd中特殊字符出现的次数
# 次数的统计可以使用累加进行累加
num = 0
num_data = sc.accumulator(num)
# 特殊字符可以通过广播变量指定特殊字符
data_list = ['!','@','#','$','%']
data_broadcast = sc.broadcast(data_list)


# 进入到executor中执行
# rdd生成
rdd = sc.parallelize(['hadoop spark # hadoop saprk @','mapreduce ! hive flink hive #'])
# 现将每个元素数据单独切割数据一个个单词和字符
rdd_flatMap=rdd.flatMap(lambda x:x.split())

# 使用map读取每个元素数据进行判断
def func(data):
    # data 接收每个元素数据
    # data_broadcast.value获取广播变量值，保存了哪些属于特殊字符
    if data in data_broadcast.value:
        num_data.add(1)   # 在特殊子字符中则进行加1

rdd_map = rdd_flatMap.map(func)

# 执行方法进行计算
rdd_map.collect()

# 查看累加的次数得到特殊字符的个数
print(num_data.value)