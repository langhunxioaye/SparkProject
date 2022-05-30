# 导入模块
from pyspark.sql.types import *
from pyspark.sql import SparkSession,Row,functions as F
import pandas as pd

# 生成sparkSession对象
# 使用arrow框架进行python和java进程间的数据传递，提升传递效率。需要进行配置指定
spark = SparkSession.builder.config('spark.sql.execution.arrow.pyspark.enabled','true').getOrCreate()

# 生成dataframe数据
pd_df = pd.DataFrame({
    'id':[1,2,3],
    'name':['zhangsan','lisi','wamgwu'],
    'age':[18,20,21]
})
df = spark.createDataFrame(pd_df)

# 计算age平均值
# 使用pandas中series类型，处理一整列的数据
# data:pd.Series  将data参数接受的值指定为pd.Series类型
# 参数的接受形式和返回值类型的指定形式是固定写法
# 使用装饰器将pandas的定义方法进行注册
@F.pandas_udf(returnType=FloatType())
def func(data:pd.Series) -> float:   # ->  float 指定返回值类型是float。根据返回值进行指定
    # data是接受参数，接受一整列的数据，使用Series类型进行保存
    # data 接受到的值自动转化为pandas的Series类型
    # 转化后就可以使用Series类型的方法对数据进行操作
    print(type(data))
    return data.mean() # 使用mean计算平局值，将结果返回

# pandas的自定义方法只能在DSL中使用
# 在DSL方法中使用
df.select(func(df['age'])).show()
