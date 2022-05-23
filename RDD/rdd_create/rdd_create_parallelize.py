# 需要导入saprkcontext
from pyspark import SparkContext

# 1、先创建SparkContext对象
# master 参数 可以指定采用那种方式运行代码 如果不指定默认是采用local本地模式执行
# 指定应用计算程序的名称，方便查看日志
sc = SparkContext(master='yarn',appName='create_rdd')

# 2、将python的列表数据转化为RDD数据
rdd = sc.parallelize([1,2,3,4])

# 3、计算rdd数据

# 4、获取rdd的计算结果数据
print(type(rdd))
print(rdd.collect())