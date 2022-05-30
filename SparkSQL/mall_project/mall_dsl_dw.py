from pyspark.sql import SparkSession,functions as F
from pyspark.sql.types import *


# 生成sparksession对象
spark= SparkSession.builder.getOrCreate()
# 定义对应的表字段信息
# 读取文件数据转化dataframe数据  csv 如果有表头信息 需要header=True  sep指定分割字符
df = spark.read.orc('hdfs://node1:8020/spark_dw/order')
# df.show()
# 1、销量最高的10个国家
# df_res = df.groupby(df['Country']).agg(F.sum(df['Quantity']).alias('sum_data')).orderBy('sum_data',ascending=False).limit(10)
# df_res.write.jdbc('jdbc:mysql://192.168.88.100:3306/mall_app?useSSL=false',mode='overwrite',table='max_country_quantity',properties={'user':'root','password':'123456','driver':'com.mysql.jdbc.Driver'})
# 2、各个国家的总销售额分布情况   总销售额 sum（商品的购买数量* 商品的单价）
# df_res=df.groupby(df['Country']).agg( F.round(F.sum(df['Quantity']*df['UnitPrice']),2).alias('sum_data')).orderBy('sum_data',ascending=False)
# df_res.write.jdbc('jdbc:mysql://192.168.88.100:3306/mall_app?useSSL=false',mode='overwrite',table='country_total_price',properties={'user':'root','password':'123456','driver':'com.mysql.jdbc.Driver'})
# 3、销售最高的10个商品 StockCode 商品编号，一个编号对应一个商品
df_res1= df.groupby(df['StockCode']).agg(F.sum(df['Quantity']).alias('sum_data')).orderBy('sum_data',ascending=False).limit(10)
df_res1.write.jdbc('jdbc:mysql://192.168.88.100:3306/mall_app?useSSL=false',mode='overwrite',table='stockcode_top_ten',properties={'user':'root','password':'123456','driver':'com.mysql.jdbc.Driver'})
# 4、商品描述的热门关键词TOP300  对商品的描述信息进行词频统计    truncate=False不省略信息
# split 切割单词，进行爆炸函数的使用，将每个单词作为一列字段数据
df_res2= df.select( F.explode(F.split(df['Description'],' ')).alias('words')).groupby('words').agg(F.count('words').alias('word_count')).orderBy('word_count',ascending=False).limit(300)
df_res2.write.jdbc('jdbc:mysql://192.168.88.100:3306/mall_app?useSSL=false',mode='overwrite',table='words_hot',properties={'user':'root','password':'123456','driver':'com.mysql.jdbc.Driver'})
# 5、退货订单数最多的10个国家 like 模糊查询
df_res3= df.where(df['InvoiceNo'].like('C%')).groupby(df['Country']).agg(F.count(df['InvoiceNo']).alias('order_count')).orderBy('order_count',ascending=False).limit(10)
df_res3.write.jdbc('jdbc:mysql://192.168.88.100:3306/mall_app?useSSL=false',mode='overwrite',table='order_country',properties={'user':'root','password':'123456','driver':'com.mysql.jdbc.Driver'})
# 6、商品的平均单价与销量的关系
df_res4=df.groupby(df['StockCode']).agg( F.round(F.avg(df['UnitPrice']),2).alias('avg_price'),F.sum(df['Quantity']).alias('sum_quantity'))
df_res4.write.jdbc('jdbc:mysql://192.168.88.100:3306/mall_app?useSSL=false',mode='overwrite',table='stockcode_price_quantity',properties={'user':'root','password':'123456','driver':'com.mysql.jdbc.Driver'})