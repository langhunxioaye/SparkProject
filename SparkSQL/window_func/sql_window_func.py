# 将hdfs数据转为dataframe
# 通过rdd读取hdfs数据，然后在将rdd数据转化为datafrma数据
from pyspark.sql.types import *
from pyspark.sql import SparkSession

# 生成SparkSession 对象
spark = SparkSession.builder.getOrCreate()
# 创建sc对象
sc = spark.sparkContext
# 读取hdfs数据转化为rdd数据
rdd = sc.textFile('/student/students.txt')
# print(rdd.collect())


# 将rdd数据转化dataframe数据
# RDD的格式是二维列表嵌套
# rdd_map  =rdd.map(lambda x:x.split(','))
# print(rdd_map.collect())
# 将字符串的数据转化为整型数据
rdd_map2  =rdd.map(lambda x:[int(x.split(',')[0]),x.split(',')[1],x.split(',')[2],int(x.split(',')[3]),x.split(',')[4]])
# print(rdd_map2.collect())
# 将转化后的rdd数据转化为dataframe数据
# ['95001', '李勇', '男', '20', 'CS']
# 可以定表字段信息
schema_type = StructType().\
    add('id',IntegerType(),False).\
    add('username',StringType()).\
    add('gender',StringType()).\
    add('age',IntegerType()).\
    add('cls',StringType())

df = rdd_map2.toDF(schema_type)

# 查看
df.show()


# 使用sql中的窗口函数完成计算
#创建表名
df.createTempView('stu')
# 聚合方法配合窗口函数使用
# 不同年级的年龄平均值，年龄最大值，年龄最小值，年龄总和
# spark.sql('select id,username,age,cls,avg(age) over(partition by cls) as avg_data,max(age) over(partition by cls) as max_data,min(age) over(partition by cls) as min_data,sum(age) over(partition by cls) as sum_data from stu').show()

# 配合排序方法生成序号
# spark.sql('select id,username,age,cls,rank() over(order by age desc) as rank_num,dense_rank() over(order by age desc) as dense_num ,row_number() over(order by age desc) as row_num from stu').show()


# 取值
# spark.sql('select id,username,age,cls,lag(username) over(order by age) as lag_data,lead(username) over(order by age) as lead_data from stu').show()
# first_value 取第一个值
spark.sql('select id,username,age,cls,first_value(username) over(order by age) as first_data from stu').show()

