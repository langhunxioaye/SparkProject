# 导入模块
# 导入schema相关类型
from pyspark.sql.types import *
# 导入sql的方法
from pyspark.sql import SparkSession,Row,functions as F

# 创建sparksession对象
# 在发生shuffle时 spark.sql.shuffle.partitions调整分区数
spark = SparkSession.builder.master('yarn').appName('movie_project_shuffle11').config('spark.sql.shuffle.partitions',4).getOrCreate()

# 需要读取hdfs的文件数据，可以使用sparkcontext的方法读取文件数据
sc = spark.sparkContext
# 读取文件转化为rdd
rdd = sc.textFile('/movies/movie.txt')

# 将rdd数据转化为dataframe数据
# rdd数据是一个二维列表,才能转化为datafrmae
rdd_map = rdd.map(lambda x:[int(x.split()[0]),int(x.split()[1]),int(x.split()[2]),x.split()[3]])

# 定义表字段信息
schema_type = StructType().\
    add('user_id',IntegerType()).\
    add('movie_id',IntegerType()).\
    add('score',IntegerType()).\
    add('create_time',StringType())
# 将rdd数据转化为dataframe
df = rdd_map.toDF(schema_type)

# 查看
# df.show()

# 需求实现
# 查询每个用户平均分
# DSL
# 整体平均分 和 每个用户的平均分
# df.select(F.avg('score')).show() # 整体

# df.groupby(df['user_id']).agg(F.avg('score')).show()   # 分组配合内置函数的实现需要用到agg

# df.groupby(df['user_id']).avg('score').show()   # 不能同时聚合多个
# 查询每个电影平均分
# df.groupby(df['movie_id']).agg(F.round(F.avg('score'),2)).show()

# 查询大于平均分的电影的数量
# sql方式中可以使用子查询将计算分成多步， dsl方法没有子查询，可以将数据一次计算返回得到一个新的dataframe数据
# 1-计算出平均分 整体打分的平均分
# df2 = df.select(F.avg('score'))  # 将计算的dataframe结果重新一个新的变量接受，计算返回的还是一个dataframe数据
# 取出平均值信息
# r = df2.first() # 取出第一行的row对象数据
#  row对象中取出平均分  可以通过下标取值
# avg_data = r[0]
# 2-基于平均分过滤查询出大于平均分的电影的数量
# count_data =df.where(df['score'] > avg_data).count()
# print(count_data) # 55375

# 查询高分电影中(>3)打分次数最多的用户, 并求出此人打的平均分
# 1-找出打高分最多的用户
# r = df.where(df['score'] > 3).groupby(df['user_id']).agg(F.count(df['score']).alias('count_data')).orderBy('count_data',ascending=False).first()
# user=r[0]
# 2-对该用户计算他所有打分的平均分
# df.where(df['user_id'] == user).agg(F.avg(df['score'])).show()

# 查询每个用户的平均打分, 最低打分, 最高打分
# df.groupby(df['user_id']).agg(F.avg(df['score']),F.min(df['score']),F.max(df['score'])).show()
# 查询被评分超过100次的电影, 的平均分 排名 TOP10
# 1 统计每个电影的打分次数和平均分
# df2 = df.groupby(df['movie_id']).agg(F.count(df['score']).alias('count_data'),F.avg(df['score']).alias('avg_data'))
# df2.show()
# 2 获取平均分前十的电影信息
# df2.where(df2['count_data'] > 100 ).orderBy(df2["avg_data"],ascending=False).limit(10).show()


# SQL语句实现需求  将df数指定一个表名
# 临时表  在当前的客户端连接中可以使用  一般使用临时表 类似 hive中的set
df.createTempView('tb_movie')
# 全局表   在所有的客户端连接中都可以使用
# df.createGlobalTempView('tb_movie')

# 查询用户平均分
# spark.sql("select user_id,round(avg(score),2) from tb_movie group by user_id ").show()
# 查询电影平均分
# spark.sql("select movie_id,avg(score) from tb_movie group by movie_id ").show()
# 查询大于平均分的电影的数量
# spark.sql("select count(movie_id) from tb_movie where score > (select avg(score) from tb_movie)").show()
# 查询高分电影中(>3)打分次数最多的用户, 并求出此人打的平均分
# spark.sql('select user_id from tb_movie where score >3 group by user_id order by count(score) desc limit 1').show()
# spark.sql("select user_id,avg(score) from tb_movie where user_id = (select user_id from tb_movie where score >3 group by user_id order by count(score) desc limit 1 ) group by user_id ").show()
# 查询每个用户的平均打分, 最低打分, 最高打分
# spark.sql('select user_id,round(avg(score),2),max(score),min(score) from tb_movie group by user_id').show()
# 查询被评分超过100次的电影, 的平均分 排名 TOP10

# spark.sql('select movie_id,count(score) as count_data,avg(score) as avg_data from tb_movie group by movie_id').show()
spark.sql('with tb_new as (select movie_id,count(score) as count_data,avg(score) as avg_data from tb_movie group by movie_id)select * from tb_new where count_data> 100 order by avg_data desc limit 10').show()





