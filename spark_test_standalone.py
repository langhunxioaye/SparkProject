# 编写spark代码，需要导入pysaprk模块进行开发
from pyspark import SparkContext
# 1、使用SparkContext生成类对象，定义变量接受类对象
# master参数可以在不同的运行模式下指定主服务
# 如果是本地模式 local[*]  默认情况下，不进行mater参数指定话是本地模式执行  local[task个数]  可以指定同时执行多少个task任务，不确定指定多个可以使用*号表示，使用*会自动根据运行情自动生成对应个数的task，但是不会超过服务器的cpu核心数
# 如果是satandalone 指定standalone的主服务的ip地址信息

# 通过master参数将standalone的主服务对应的ip地址告知给master

# appName可以指定计算任务的名称，方便在进行日志查看知道是哪个计算任务产生的日志
sc = SparkContext(master='spark://192.168.88.100:7077',appName='standalone_test')

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

