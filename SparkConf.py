from pyspark import SparkContext, SparkConf

# 运行参数配置需要用到SparkConf的类
# 接受key和value两个数据
# key值是配置的属性   value是属性值  类型都是字符串形式
conf = SparkConf().set('deploy-mode', 'cluster')

# 1、使用SparkContext生成类对象，定义变量接受类对象
# 通过master指定使用yarn进行资源调度
# 需要统conf参数将配置添加到SparkContext
sc = SparkContext(master='yarn', appName='spark_test_yarn', conf=conf)
