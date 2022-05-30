# 编写spark代码，需要导入pysaprk模块进行开发
from pyspark import SparkContext,SparkConf
# 运行参数配置需要用到SparkConf的类
conf = SparkConf()
# 接受key和value两个数据
# key值是配置的属性   value是属性值  类型都是字符串形式
#设置运行方式
conf.set('deploy-mode','cluster')

# 设置driver
# 内存配置  默认不配值driver会使用1G内存
conf.set('driver-memory','2')
# 配置cpu核心数  默认是一个
conf.set('driver-cores','2')

# 设置executor
# 一个executor运行的内存
conf.set('executor-memory','2')
# 一个excutor的cpu核心数据    cpu的核心数据和task线程执行并行度有关， 有两个核心，可以同时执行两个task
conf.set('executor-cores','2')

## 链式配置
# conf.set('executor-memory','2').set('executor-cores','2')

# 1、使用SparkContext生成类对象，定义变量接受类对象
# 通过master指定使用yarn进行资源调度
# 需要统conf参数将配置添加到SparkContext
sc = SparkContext(master='yarn',appName='spark_test_yarn',conf=conf)

# 2、生成rdd数据
data = [1,2,3,4]
# 将python的list数据转化为spark的rdd结构的数据
rdd = sc.parallelize(data)

# 3、转化后就可以使用rdd的方法进行数据计算，计算的返回结果还是以rdd的形式保存在内存上
# lambda data:data*data 将rdd中的每个元素数据相乘   data就是接受rdd中的每个元素数据
rdd2 = rdd.map(lambda data:data*data)

# 查看返回的结果信息
res_data = rdd2.collect()
print(res_data)

data = [1,2,3,4,]