import json
import os

with open('data/acl.json', 'r') as load_f:
    data_dict = json.load(load_f)

# print(data_dict.keys())
# print(data_dict.values())
for a in data_dict.keys():
    print(a.split('/')[-1], data_dict[a]['owner'])
    if (a.split('/')[-1] not in ('homes', '共享', '测试') and (a.split('/')[-1] != data_dict[a]['owner'])):
        data_dict[a]['owner'] = a.split('/')[-1]

test_dict2 = (dict(zip(data_dict.keys(), data_dict.values())))
print(test_dict2)


jsObj = json.dumps(test_dict2, indent=4, ensure_ascii=False)  ##Unicode中文
print(jsObj)

with open('acl_target.json', "w", encoding='utf-8') as f:
    f.write(jsObj)
