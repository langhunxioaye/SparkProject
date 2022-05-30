from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("count").getOrCreate()
sc = spark.sparkContext

# TODO 1: SQL 风格进行处理
rdd = sc.textFile("/datas/words.txt"). \
    flatMap(lambda x: x.split(" ")). \
    map(lambda x: [x])

df = rdd.toDF(["word"])

# 注册DF为表格
df.createTempView("words")

spark.sql("SELECT word, COUNT(*) AS cnt FROM words GROUP BY word ORDER BY cnt DESC").show()