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
# rdd_count = rdd_flatMap.countByValue()
# print(rdd_count)

# 使用map方法 转化k-v形式，给定初始值1
rdd_map1 = rdd_flatMap.map(lambda x:(x,1))
# print(rdd_map1.take(10))
rdd_res = rdd_map1.reduceByKey(lambda x,y:x+y)
# print(rdd_res.take(10))


# 用户搜索点击统计
# print(rdd_map.take(3))
# 构造计算的数据结构  (用户id+搜索内容，点击次数)
# x接受rdd中的元素数据
rdd_map3 = rdd_map.map(lambda x:(x[1]+x[2],1))
# print(rdd_map3.take(3))
rdd_res = rdd_map3.reduceByKey(lambda x,y:x+y)
# print(rdd_res.take(3))
# 可以对统计的记过进行排序，按照点击次数进行排序
# x接受 rdd中的每个元素，如果元素是一个集合数据，可通过下标的方式指定按照哪个数据进行排序
# ascending=True 默认是True 表示升序，从小到大排
rdd_sort = rdd_res.sortBy(lambda x:x[1],ascending=False)
# print(rdd_sort.take(1))


# 不同时间段的用户搜索次数(按照小时统计)  (时间，1)
# print(rdd_map.take(10))
# 从rdd取出每个元素数据，然后再对每个元素中时间数据进行处理
# 将时间转化为key-value形式方便记性reduceBykey
# x[0][0:2]
rdd_map4 = rdd_map.map(lambda x:(x[0].split(':')[0],1))
# print(rdd_map4.take(10))
# 根据key进行累加计算
rdd_res2 = rdd_map4.reduceByKey(lambda x,y:x+y)
# print(rdd_res.take(10))
rdd_sort1 = rdd_res2.sortBy(lambda x:x[1],ascending=False)
print(rdd_sort1.take(10))






