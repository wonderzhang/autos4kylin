import xml.etree.cElementTree as ET
import os
import sys

tree = ET.ElementTree(file='./data/raw_data.xml')

# 根元素（root）是一个Element对象。我们看看根元素都有哪些属性
root = tree.getroot()

# 没错，根元素并没有属性。与其他Element对象一样，根元素也具备遍历其直接子元素的接口
for child_of_root in root:
    print(child_of_root,child_of_root.attrib)
    for x in child_of_root:
        print(child_of_root, x, x.tag,':',x.text)