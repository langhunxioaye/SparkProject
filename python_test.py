# # 匿名函数  加法
# # 冒号前是函数的参数， 冒号后是对参数进行计算然后返回结果
# # 需要有一个变量接收匿名函数，接受后func变量就是一个函数
# func = lambda a,b:a+b
# print(type(func))
# # 等价于下面的函数定义
# def func1(a,b):
#     return a+b
# print(type(func1))
#
# def func2(f):
#     # 函数中的参数可以接受任意数据
#     return f(10,20)
#
#
# # 调用
# data = func(1,2)
# print(data)
# # 直接将lambda的函数当成参数传参使用
# data2 = func2(lambda a,b:a+b)
# print(data2)
# # 把函数当成参数使用时不能加括号
# data3 = func2(func1)
# print(data3)
#
# # lambda比较适合简单计算，业务逻辑只有一步的方式的计算
#
# # python中的类型
# # 每种类型都有自己的数据格式
# # 字典类型  格式 key和value形式
# dict
# data_dict = {'name':'张三'}
# print(type(data_dict))
# # 字典类型里的处理数据方法
# data_dict.items() # 返回key和vale数据
# data_dict.get('name') # 获取指定key对应value值
#
# # 列表类型
# list
# data_list = [1,2,3,4]
# print(type(data_list))
# # 列表类型的处理方法
# data_list.sort() # 排序
# data_list.append([6,4,5])# 添加新的数据
# # 类型本质是通过类的方式定义出来的不同存储方式，每种类型有不同的方法来操作数据
#
# # 可以通过自定义类来定义自己程序数据类型，每种类型可以规定自己的数据格式，和数据处理的方法
#
# class RDD(object):
#
#     # 定义格式
#     data = []
#
#     # 定义方法
#     def add_value(self,value):
#         self.data.extend(value)
#
#         return self
#
#     # 读取数据方法
#     def read_func(self,f):
#         # 每次遍历一个数据
#
#         # 对遍历的数据进行计算，进行计算方式需要有使用人员提供
#         # f接受用户传递计算数据逻辑
#
#         self.data1=[]  # 定义一个新的rdd
#         for i in self.data:
#             # 将计算后的结果重新写入rdd
#             self.data1.append(f(i))
#
#         return self
#
#     def read_func2(self,f):
#         # 使用遍历的i值做下标在进行取值
#         self.data1=[]  # 定义一个新的rdd
#         for i in self.data:
#             # i不能超过下标
#             if i >= len(self.data):
#                 break
#             # 将计算后的结果重新写入rdd
#             self.data1.append(f(self.data[i]))
#
#         return self
#
#     def res_data(self):
#         print(self.data1)
#
#         return self
#
#
# # 使用自定义的类型
# rdd = RDD()
# rdd.add_value([1,2,3,4])
# # 计算逻辑有使用人员自己编写
# rdd.read_func(lambda a:a*3-a)
# # rdd.read_func2(lambda a:a)
# rdd.res_data()
#
# # rdd是一种数据类型，规定了数据的存储格式和数据的处理方式
#
# # 链式调用  rdd支持链式调用
# rdd2 = RDD()
# rdd2.add_value([1,2,3,5]).read_func(lambda a:a*3-a).res_data()
#
#
# # 面向对象中 方法
# # 对象方法  需要在定义函数。执行一个参数self ，self代表对象本身
#
# # 类方法  cls
# # 静态方法
# class Student(object):
#
#     # 对象方法  必须有self 被对象调用，但是不能被类调用
#     def func(self):
#         print('func')
#     # 类方法  必选有装饰器和cls参数   可以被对象和类调用
#     @classmethod
#     def func1(cls):
#         print('func1')
#         pass
#
#     # 静态方法  必须有装饰器，参数可以根据需求定义 可以被对象和类调用
#     @staticmethod
#     def func2():
#         pass
#
#     @property  # 把函数当成属性使用
#     def func3(self):
#         print('func3')
#
# # 类型可以直接调用类方法
# Student.func1()
# # 类加括号就是初始化生成对象
# Student().func1()
#
# # 类不能直接调用对象方法
# # Student.func()
# # 对象是可以直接调用对象方法
# Student().func()
#
#
# # 在调用函数时不需要加括号可以当成属性调用
# Student().func3

# data使用列表 ，列表是一个迭代对象  元祖 集合，
data = [1,2,3,4]


# for i in data: # 执行遍历是 会将data的数据全部加载内存中，如果数据量特别大内存中需要在加载很多数据
#     print(i)

# 使用迭代器遍历迭代对象，每次取出一个值加载到内存进行计算
# 将迭代对象加载到迭代器中
data_iter = iter(data)
# 从迭代器中每次取一个值

# print(next(data_iter))  # 取一个值加载到内存，节省空间
# print(next(data_iter))
# print(next(data_iter))


# 自定义迭代器
# def func(data):
#     for i in data:
#         yield i  # 自定义迭代器中使用yield关键词，每次返回一个数据
#
# f1 = func(data) # 迭代器
# # res_data1 = next(func(data))  # func(data) 是一个迭代器
# # res_data2 = next(func(data))   # func(data) 是一个新的迭代器
# # print(res_data1) # 上面的方式相当于是对两个不同的迭代器进行操作
# # print(res_data2)
#
# # 取出迭代器中的数据
# res_data1 = next(f1)
# print(res_data1)
# res_data2 = next(f1)
# print(res_data2)
#
#
# a = 10
#
# def func():
#     b=2
#     global a
#
#
# def func2():
#     c=100
#     global a
#

# import threading
# # transformation方法
# def map():
#     print(111111)
#
# def flatMap():
#     print(22222)
#
#
# # action方法
# def collect(t):
#     t.start()
#
# # 创建线程
# t1 = threading.Thread(target=map)
#
# collect(t1)

class SparkContext(object):

    def __init__(self,master=None,appName=None,conf=None):
        self.master =master
        self.appName = appName
        self.conf = conf

    def textFile(self,path):
        print('SparlContext')
        print(path)
        return 'rdd'

# sc = SparkContext(master='yarn')
# rdd = sc.textFile('/datas/input')

# 类继承
class SparkSession(SparkContext):
    # sparkSession兼容Sparktext，可以使用sparkcontext的方法

    def createDataFrame(self):
        print('createDataFrame')
#
# spark = SparkSession()
# spark.textFile('/datas/input')

# 把另外一个类的当成前类的属性值
class SparkSession1(object):
    # sparkSession兼容Sparktext，可以使用sparkcontext的方法
    # sparkcontext 是SparkSession1类的属性
    # 属性的值是SparkContex对象
    sparkcontext = SparkContext()

    def createDataFrame(self):
        print('createDataFrame')

# SparkSession1.sparkcontext.textFile('data/aaaaaa')


# 把另外一个类的当成前类的属性值
class SparkSession2(object):
    # sparkSession兼容Sparktext，可以使用sparkcontext的方法

    def createDataFrame(self):
        print('createDataFrame')
    @property
    def sparkContext(self):
        # sc 就是对象属性
        # 对象属性值是SparkContext对象
        self.sc = SparkContext()
        return self.sc


spark = SparkSession2()
# 调用方法会执行返回sc
# spark.sparkContext().textFile('/data/111111')
spark.sparkContext.textFile('/data/22222')



