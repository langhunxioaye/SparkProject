# 多个dataframe的操作
# 并集，交集 ，关联

from pyspark.sql import SparkSession
import pandas as pd

# 创建sparksession对象
spark = SparkSession.builder.getOrCreate()

# 创建dataframe数据
pd_data1 = pd.DataFrame({
    'id': [1, 2, 3],
    'name': ['张三', '李四', '王五'],
    'age': [20, 22, 33],
    'gender': ['男', '男', '女'],
})
pd_data2 = pd.DataFrame({
    'id': [3, 4, 5],
    'name': ['小明', '小红', '韩梅梅'],
    'age': [20, 18, 19],
    'gender': ['男', '女', '女'],
})

# 生成两个df
df1 = spark.createDataFrame(pd_data1)
df2 = spark.createDataFrame(pd_data2)
# 查看
df1.show()
df2.show()


# 合并dataframe数据
# union方法合并  求并集
# df1.union(df2).show()
# 求交集 找相同数据展示
# df1.intersect(df2).show()

# 关联join
# join(参数1，参数2，参数3)
# 参数1 关联df
# 参数2 关联字段
# 参数3 关联方式  不写是内关联
# 内关联  找相同的
df1.join(df2,df1['id'] == df2['id']).show()
# 左关联
df1.join(df2,df1['id'] == df2['id'],'left').show()
# 右关联
df1.join(df2,df1['id'] == df2['id'],'right').show()
