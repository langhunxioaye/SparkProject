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
# df.show()


# DSL方法查询学生数据
# df.select(df['id'],df['username']).show()
# df.select('id','username').show()


# 编写的链式调用和sql的执行顺序有关
# from   where  groupby 聚合 having orderby select  limit
# where条件过滤查询
df.where(df['age'] > 20).select(df['username'],df['age']).show()
#多个条件 and 是与  or 或
# df.where("age > 20 and cls =='CS' ").select(df['username'],df['age'],df['cls']).show()




# 数据分组配合聚合计算
# 两个方法的作用一样
# 求不同年级的年龄平均数  avg  sum   max min
# df.groupBy(df['cls']).avg('age').show()
# df.groupby()
# 求不同年级的年龄总和
df.groupBy(df['cls']).sum('age').show()

# 排序操作
# 将学生按照年龄进行排序
# df.orderBy(df['age']).show() # 默认是升序
# df.orderBy(df['age'],ascending=False).show()  #  ascending=False 降序

# 分组后排序 排序的字段名用聚合函数名进行指定
df.groupBy(df['cls']).sum('age').orderBy('sum(age)').show()
df.groupBy(df['cls']).avg('age').orderBy('avg(age)').show()

# 指定点展示数量
df.limit(3).show()
