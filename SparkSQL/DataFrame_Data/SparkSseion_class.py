# 导入saprkSession
from pyspark.sql import  SparkSession
# 初始化SparkSession类生成SparkSession对象的链式调用流程。
# 最基本的初始化SparkSession.builder.getOrCreate() 生成sparkSession对象
# 本地模式运行
spark_base = SparkSession.builder.getOrCreate()
# 可以在链式调用是添加其他方法  链式调用顺序是固定的
# 指定master，就调用master方法   指定yarn模式
spark_master = SparkSession.builder.master('yarn').getOrCreate()
# 指定appName  指定任务名称
spark_appName = SparkSession.builder.master('yarn').appName('sparkSQL').getOrCreate()
# 进行conf配置  指定配置
spark_conf = SparkSession.builder.master('yarn').appName('sparkSQL').config('deploy_mode','cluster').getOrCreate()
# 多个配置
spark_conf1 = SparkSession.builder.master('yarn').appName('sparkSQL').config('deploy_mode','cluster').config('executor-cores','2').getOrCreate()

# 使用sparkSession对象获取sparkcontext
sc = spark_base.sparkContext
# sc对象生成rdd数据
rdd = sc.parallelize([1,2,3,4])
print(rdd.collect())

# 使用yarn模式
sc1 = spark_master.sparkContext
rdd1 = sc1.parallelize([2,2,3,3])
print(rdd1.collect())

# 指定了appname
sc2 = spark_master.sparkContext
rdd2 = sc2.parallelize([2,2,88,66])
print(rdd2.collect())
