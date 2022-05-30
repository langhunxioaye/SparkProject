# 编写spark代码，需要导入pysaprk模块进行开发
from pyspark import SparkContext

# standalone进行了高可用的配置，就有两个master服务，需要指定两个master服务的ip地址信息
# 程序会自己寻找活动的master申请资源

# appName可以指定计算任务的名称，方便在进行日志查看知道是哪个计算任务产生的日志
sc = SparkContext(master='spark://192.168.88.100:7077,192.168.88.101:7077',appName='standalone_test_ha')

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

