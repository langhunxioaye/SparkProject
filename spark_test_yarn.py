# 编写spark代码，需要导入pysaprk模块进行开发
from pyspark import SparkContext

# python的os模块查看当前代码运行时依赖的环境，
# import os
# 查看依赖的环境
# print(os.environ)
# 添加java依赖环境  JAVA_HOME作为key  java的安装地址作为value
# os.environ['JAVA_HOME']='/export/server/jdk' # 把代码运行的服务器上安装的java地址添加到环境中
# 依赖环境在新文件中开发代码是都需要添加一下，使用很麻烦，直接将java环境添加到程序运行的系统环境里

# 1、使用SparkContext生成类对象，定义变量接受类对象
# 通过master指定使用yarn进行资源调度
sc = SparkContext(master='yarn',appName='spark_test_yarn')

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

