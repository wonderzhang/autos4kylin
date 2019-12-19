
# 循环方法计算阶乘：5!
# def fact1(n):
#     i = 1
#     result = 1
#     while i <= n:
#         result = result*i
#         i = i+1
#     return result
# print(fact1(5))


# 递归方法计算阶乘：5!
# def fact2(n):
#     if n == 1:
#         return 1
#     return fact2(n-1)*n
# print(fact2(5))
#
#
#


#
# raw = ['PCXXX', ['0078', 8831], ['0000', '7777']]
#
#
# for item in raw:
#     if isinstance(item, (str, int)):
#                           print(item)
#     else:
#         for item2 in item:
#             print(item2)
#
#
#
#
#  for i in i['children']:
#                 lev=0
#                 lev=lev+1
#
#                 print('lev~~~~~~~~~~~~~~~'+str(lev))
#                 print(i['cuboid_id'])
#                 print(i['name'])
#
#                 for i in i['children']:
#                     lev=1
#                     lev = lev + 1
#                     print('lev~~~~~~~~~~~~~~~' + str(lev))
#
#                     print(i['cuboid_id'])
#                     print(i['name'])
#
#                     for i in i['children']:
#                         lev = 2
#                         lev = lev + 1
#                         print('lev~~~~~~~~~~~~~~~' + str(lev))
#
#                         print(i['cuboid_id'])
#                         print(i['name'])
#
#
# def unlimit_loop(x):
#     for i in x:
#          unlimit_loop(i)['cuboid_id']
#
#
# unlimit_loop(i['children'])




raw = ['PCXXX', ['0078', 8831], ['0000', '7777']]

def get_data(datas):
    for item in datas:
        if isinstance(item, (str, int)):
            print(item, type(item))
        else:
            get_data(item)


get_data(raw)

#
#https://bbs.csdn.net/topics/392010908
#
# raw = ['PCXXX', ['0078', 8831], ['0000', '7777']]
#
# def get_data(datas):
#     for item in datas:
#         if isinstance(item, (str, int)):
#             if item == '0000':
#                 return item
#         else:
#             return get_data(item) # 这里加不加前面的return都一样
#
#
# x = get_data(raw)
# print(x)
#


#
#
# raw = ['PCXXX', ['0078', 8831], ['0000', '7777']]
#
# def get_data(datas):
#     for item in datas:
#         if isinstance(item, (str, int)):
#             if item == '0000':
#                 return item
#         else:
#             return get_data(item) # 问题出在这里，函数中如果没有遇到return的话，python默认会返回None，因此运行到raw当中第一个list，如果没有找到match的元素，就不会去寻找下一个list而是返回None
#
# x = get_data(raw)
# print(x)


raw = ['PCXXX', ['0078', 8831], ['0000', '7777']]


def get_data(datas):
    for item in datas:
        if isinstance(item, (str, int)):
            if item == '0000':
                return item
        else:
            if (get_data(item) != None):  # 这里其实加一个中间变量temp=get_data(item)，再直接判断temp运行效率更高
                return (get_data(item))


x = get_data(raw)
print(x)
