from pyspark.sql import SparkSession
from pyspark.sql.types import *


# 生成sparksession对象
spark= SparkSession.builder.getOrCreate()
# 定义对应的表字段信息
data_type = StructType().\
    add('InvoiceNo',StringType()).\
    add('StockCode',StringType()).\
    add('Description',StringType()).\
    add('Quantity',IntegerType()).\
    add('InvoiceDate',StringType()).\
    add('UnitPrice',DoubleType()).\
    add('CustomerID',IntegerType()).\
    add('Country',StringType())
# 读取文件数据转化dataframe数据  csv 如果有表头信息 需要header=True  sep指定分割字符
df = spark.read.csv('hdfs://node1:8020/spark_ods/ECommerce.csv',sep=',',schema=data_type)
# df.show()

# 数据的清洗转化
# 第一清洗CustomerID 用户id不为0 ， Description商品描述信息不为空字符
# print(f'清洗前的数据量{df.count()}' )
clear_df = df.where(df['CustomerID'] != 0).where(df['Description'] !='')
# print(f'清洗前的数据量{clear_df.count()}' )
#清洗过的数据放入下一层
clear_df.write.orc('hdfs://node1:8020/spark_dw/order',mode='overwrite')