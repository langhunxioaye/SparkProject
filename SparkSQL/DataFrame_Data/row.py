# DataFrame中的行对象定义
from pyspark.sql import Row

# 使用Row类定义行对象
# 第一行数据
r1 = Row(1,'张三',18,'男')
# 第二行数据
r2 = Row(2,'李四',19,'男')

# 直接对行对象操作
# 通过取下标的方式取出一行中的某列数据,下标从0开始
print(r1[1])
print(r2[1])