# 创建dataframe的表的字段信息
# 将dataframe中的所有的结构类型导入
from pyspark.sql.types import *

# 创建schema的类的使用
# StructType() 是创建schema用到类，可以在StructType()匿名对象后使用add方法添加字段信息
# add(参数1，参数2，参数3)
# 参数1 是字段名  是字符串
# 参数2 是字段类型 IntegerType()表示整型（数值） StringType()表示字符串类型  DecimalType():表示小数   BooleanType()布尔类型
# 参数3 是否允许为空值  true为空 可选参数  默认不写是true
# 定义多个字段就在后面一直点add，链式调用add
schema_type = StructType().\
    add('id',IntegerType(),False).\
    add('name',StringType()).\
    add('age',IntegerType()).\
    add('gender',StringType())
