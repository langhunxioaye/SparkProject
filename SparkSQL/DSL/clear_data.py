# 数据清洗
from pyspark.sql.types  import *
from pyspark.sql import SparkSession
import pandas as pd

spark = SparkSession.builder.getOrCreate()
# 重复数据处理
pd_df = pd.DataFrame({
    "id":[1,2,3,1],
    "name":['张三','李四',None,'张三'],
    "age":[18,None,20,18],
    "gender":[None,None,'男',None],
    "datetime":['4984654',None,'4984654',None]
})

# 转化
spark_df = spark.createDataFrame(pd_df)

spark_df.show()

# 去重方法
# 会对整行数据进行判断是否重复
# spark_df.dropDuplicates().show()
# # 指定字段判断是否重复，如果指定的字段有重复值就会去重整行数据
# spark_df.dropDuplicates(['gender']).show()
# # 多个字段
# spark_df.dropDuplicates(['id','gender']).show()

# 空值
# 去除空值
# 一行数据中只要有空值就会去除
spark_df.dropna().show()  # 尽量少用该方式，可能在计算时，一些无效数据是空值,其他数据是有效，使用这个方法也会把有效数据删除掉
# 指定有效值（非空值）个数  指定一行中有多少非空的数据会进行保留
spark_df.dropna(thresh=3).show()
# # 指定字段是否是有效字段
# # 指定age为有效字段   实际去重中可采用指定字段方式，对哪个字段计算，就对哪个字段进行去除空值
spark_df.dropna(thresh=1,subset=['age']).show()
# # age和gender两个都是有效字段
spark_df.dropna(thresh=2,subset=['age','gender']).show()
# # 两个字段中有一个是有效字段就可以
spark_df.dropna(thresh=1,subset=['age','gender']).show()

# 空值替换
# 替换的数据是数值，那么只会替换数值类型的
spark_df.fillna(100).show()
spark_df.fillna('itcast').show()
# 明确对那些字段替换
spark_df.fillna({'name':'aaaa','age':0,'gender':'ccc'}).show()
