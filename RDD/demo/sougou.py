import jieba
from pyspark import SparkContext

sc = SparkContext(master='yarn')

# 读取hdf上文件数据
rdd = sc.textFile('/sougou')
# 文件中的每行数据是rdd中有一个元素数据
# [第一行数据，第二行数据，第三行数据]
# ['23:58:47\t6812058619484457\t[学业水平测试历史知识点]\t4 4\twhlsjstm.blog.sohu.com/86842042.html']
# \t  制表符 代表四个空格
# print(rdd.take(10))

# 将数据进行过滤，x.strip() 每行数据进行空格去除  len(x.strip()) 计算每行字符串的长度，长度大于0，说明有数据
# len(x.split())将每行的字符串进行切割，判断切割能否获取到6个元素的数据，要求字段是6个，不等于6说明数据不符合条件会被过滤掉
rdd_filter = rdd.filter(lambda x:len(x.strip()) > 0 and len(x.split()) == 6)

# 对过滤后的数据，通过map转化为一行一行的元素数据
rdd_map = rdd_filter.map(lambda x:x.split())
# [
# ['10:38:27', '137880161979531', '[tailuoaoteman]', '4', '2', 'home.hexun.com/tailuoaoteman/default.html'],
# ['10:38:27', '003212746552655188', '[袁咏仪博客]', '86', '17', 'www.cnxbr.com/n_read.asp?n=16628'],
# ['10:38:27', '5881397847340641', '[谷歌高清晰卫星地图]', '3', '12', 'down.zdnet.com.cn/detail/9/81112.shtml']
# ]
# [ [第一行字段1,第一行的字段2，第一行字段3]，[第2行字段1,第2行的字段2，第2行字段3] ]
# print(rdd_map.take(10))

# 需求1： 用户查询字符串的词频
# x获取一行数据['10:38:27', '003212746552655188', '[袁咏仪博客]', '86', '17', 'www.cnxbr.com/n_read.asp?n=16628']
# x的数据类型是一个list，通过取下标得到用户的查询字符串数据，然后进行结巴分词，分词后的数据是列表可以交给flatMap处理
rdd_flatMap = rdd_map.flatMap(lambda x:jieba.cut(x[2][1:-1]))
# 取出10个数据展示  ['360', '安全卫士', '哄抢', '救灾物资', '75810', '部队', '绳艺', '汶川', '地震', '原因']
# print(rdd_flatMap.take(10))

# 简便方法
rdd_count = rdd_flatMap.countByValue()
print(rdd_count)

# 使用map方法 转化k-v形式，给定初始值1
rdd_map1 = rdd_flatMap.map(lambda x:(x,1))
print(rdd_map1.take(10))
rdd_res = rdd_map1.reduceByKey(lambda x,y:x+y)
print(rdd_res.take(10))
