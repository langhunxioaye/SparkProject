# 导入模块
import jieba

# 主要是对中文字符串进行单词切割
data_str='欢迎来到王者荣耀'

data_cut = jieba.cut(data_str)
# 使用list强制类型转化
print(list(data_cut))


# 搜狗的每行数据都是字符串,可以进行相应的处理
sougou_data = '00:00:00	2982199073774412	[360安全卫士]	8 3	download.it.com.cn/softweb/software/firewall/antivirus/20067/17938.html'

# 对一整行数据记性切割
sougou_data_list  = sougou_data.split()
print(sougou_data_list)

# 在项目中会对用户搜索的字符串数据进行分词后统计单词出现的次数
# [360安全卫士] 切片 [1,-1]  360安全卫士
print(sougou_data_list[2][1:-1])
jie_data = jieba.cut(sougou_data_list[2][1:-1])
print(list(jie_data))