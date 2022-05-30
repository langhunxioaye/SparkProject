# 导入使用的模块
from pyspark.sql.types import *
from pyspark.sql import SparkSession,Row

# 生成SparkSession对象
spark = SparkSession.builder.getOrCreate()
# 生成sparkcontext对象 sparkContext是一个SparkSession的属性
sc = spark.sparkContext
# 生成sparkcontext对象生成rdd
# rdd的数据，应该是二维列表数据才能转化为dataframe
# [[第一个行数据]，[第二行数据]。。。。]
rdd = sc.parallelize([[1,'张三',20,'男'],[2,'李四',19,'男']])

# 指定字段信息
schema_type = StructType().\
    add('id',IntegerType(),False).\
    add('username',StringType()).\
    add('age',IntegerType()).\
    add('gender',StringType())

# toDF方法  将rdd数据转化为dataframe数据
# 参数，可以接收指定schema的字段信息数据，如果没有指定，会自动判断
df = rdd.toDF(schema_type)
# 查看dataframe数据
df.show()
# 查看schema信息
# df.printSchema()

# 使用原生ql方式查询
# spark是sparksession的对象，下面有一个sql方法可以执行sql语句
# 写sql语句是，需要有一个表的名字方便进行查询，此时可以使用 createTempView给df指定一个表的明智
df.createTempView('stu')
# 写sql查询 ,sql是字符串 ,show展示数据
spark.sql('select id,username from stu').show()