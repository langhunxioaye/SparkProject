from pyspark import SparkContext

sc =SparkContext(master='yarn',appName='share_data')

# 定义一个变量
a=10
# 将变量值转化为累加器值
accumulator_data = sc.accumulator(a)


# 进入到executor中执行的
# 生成rdd
rdd = sc.parallelize([1,2,3,4])

# 计算  rdd的第一次计算
# accumulator_data.add()  累加器提供了一个add方法，会将接受到的值和累加器值进行累加操作
rdd_map = rdd.map(lambda x:accumulator_data.add(x))
# rdd的第二次计算
rdd_map2 = rdd.map(lambda x:accumulator_data.add(x))
# 执行输出  查看rdd的数据 rdd的数据被传递给累加器使用了，本身rdd中没有数据

# 使用缓存避免重复累加
rdd_map.persist()
# 方法需要被触发执行
rdd_map.collect()
# rdd_map2.collect()

# 对rdd_map 多次执行  因为第一次已将进行过缓存，所有后续就执行读缓存数据，会在重复累加计算
rdd_map.collect()

# 查看累加器累加后的数据  一般累加器应用在计数上，对task的计算次数进行统
# 对rdd进行多次执行，会重复累加
print(accumulator_data.value)  # 10 +1+2+3+4