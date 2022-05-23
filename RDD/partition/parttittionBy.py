# 导入数据模块
from pyspark import SparkContext

# 配置

# 生成sc
sc = SparkContext()

# 生成rdd数据
rdd = sc.parallelize([('hadoop',1),('hive',1),('spark',1),('hadoop',2)])

# 查看默认的分区形式
# [[('hadoop', 1), ('hive', 1)], [('spark', 1), ('hadoop', 2)]]
print(rdd.glom().collect())

# 自定义分区方法
def func(key):
    # 接收rdd每个元素中的key
    # 对key值进行判断划不同的分区
    if key == 'hadoop':
        return 0  # 但符合条件时，返回一个分区值，分区数是从0开始。 当key是hadoop是将数据放入第0个分区
    elif key == 'hive':
        return 1
    else:
        return 2

# 将方法放入rdd中执行
rdd_partition_func=rdd.partitionBy(3,func) # 3 是指定分成3个分区，func是分区方法

print(rdd_partition_func.glom().collect())