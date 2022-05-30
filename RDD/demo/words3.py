# 模块导入
from pyspark import SparkContext, SparkConf
from pyspark.sql.types import *
from pyspark.sql import SparkSession,Row

# sc对象
spark = SparkSession.builder.appName("count_json").master('local').getOrCreate()
# 读取文件数据转化为rdd
rdd = spark.read.json('file:/export/server/data/employee.json')

schema_type = StructType().\
    add('id',IntegerType(),False).\
    add('rname',StringType()).\
    add('age',IntegerType())


df = rdd.toDF()
df.createTempView("words_json")

spark.sql("SELECT * FROM words_json WHERE age > 30").show()
