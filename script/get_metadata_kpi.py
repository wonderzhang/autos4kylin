import xml.etree.ElementTree as tree
import json
import  pandas as pd
from   sqlalchemy import create_engine
import saiku_api

parse = tree.parse('./data/iqj_olap_v62.xml')
parse_root = parse.getroot()
print(parse)

sub_cube_measure_list=[]
cube_measure_list=[]
cube_measure_dict={}
for child in parse_root: #遍历
    if (child.tag == 'Cube'):
        print(child.tag) # 节点名称
        print(child.attrib.get('name'))  # 节点名称
        cube_name=child.attrib.get('name')

        for sub in child:

            if (sub.tag == 'Table'):
                table_name=(sub.attrib.get('name'))
                schema_name=(sub.attrib.get('schema'))
            if (sub.tag=='Dimension'):

                #print(sub.tag)
                Dimension_name=str(sub.attrib.get('name'))
                for hirachey in sub:
                    #print('hirachey')
                    print(hirachey.attrib)
                    hirachey_name=(hirachey.attrib.get('name'))
                    for inx, level in enumerate(hirachey):
                        if inx == 0:
                            # #print(eval(level.tag))
                            print('level')
                            table_name=(level.attrib.get('name'))
                        else:
                            print(inx)
                            print(level.attrib.get('primaryKey'))
                            print(level.attrib.get('name'))
                            if level.attrib.get('table') is None:
                                print(table_name)
                                print(level.attrib.get('column'))

                            else:
                                print(level.attrib)
                                print(level.attrib.get('table'))
                                print(level.attrib.get('column'))







#     print(child.attrib)
#     print(child.attrib.get('name'))#节点属性
#     for sub in child:
#
#         if (sub.tag=='Table'):
#             table_name=(sub.attrib.get('name'))
#             schema_name=(sub.attrib.get('schema'))
#
#         if (sub.tag=='Measure'):
#             print(sub.attrib)
#             cube_measure_dict['table_name'] = table_name
#             cube_measure_dict['schema_name'] = schema_name
#             cube_measure_dict['cube_name']=child.attrib.get('name')
#             cube_measure_dict['measure_name']=(sub.attrib.get('name'))
#             cube_measure_dict['column']=(sub.attrib.get('column'))
#             cube_measure_dict['aggregator']=(sub.attrib.get('aggregator'))
#             cube_measure_dict['visible']=(sub.attrib.get('visible'))
#             sub_cube_measure_list.append(child.attrib.get('name'))
#             sub_cube_measure_list.append(schema_name)
#             sub_cube_measure_list.append(table_name)
#             sub_cube_measure_list.append(sub.attrib.get('name'))
#             sub_cube_measure_list.append(sub.attrib.get('column'))
#
#
#             if sub.attrib.get('description') is None:
#                 sub_cube_measure_list.append(sub.attrib.get('name'))
#             else:
#                 sub_cube_measure_list.append(sub.attrib.get('description'))
#             sub_cube_measure_list.append(sub.attrib.get('aggregator'))
#             sub_cube_measure_list.append(sub.attrib.get('visible'))
#             cube_measure_list.append(sub_cube_measure_list)
#             sub_cube_measure_list=[]
#
#
# print(cube_measure_list)
#
# cube_measure_df = pd.DataFrame(cube_measure_list,
#                           columns=['cube', 'main_table_schema', 'main_table', 'measure', 'measure_col',
#                                    'measure_desc', 'agg_type', 'visible'])
# print(cube_measure_df)
#
# DB_CONNECT = 'mysql+pymysql://data_window:zPcQl%R2xWJ!ByXe@192.168.155.69:3306/data_window?charset=utf8'
# conn = create_engine(DB_CONNECT, echo=True, pool_size=10, max_overflow=20)
# cube_measure_df.to_sql(name='cube_measure_mapping', con=conn, if_exists='replace', index=False)










#         if sub.attrib.get('name') == '注册渠道':
#             print(sub.tag)
#             print(sub.attrib)
#         #
#             for Hierarchy in sub:
#                 print(Hierarchy.tag)
#                 print(Hierarchy.attrib)
#                 for inx ,level in enumerate(Hierarchy):
#
#                     print(inx)
#                     if inx==0:
#                         #print(eval(level.tag))
#
#
#                         dict_string = json.dumps(level.tag)
#                         print('~~~~~~~~')
#                         print(level.tag)
#
#                         print(type(eval(dict_string)))
#                         print(level.tag)
#                         print('~~~~~~~~')
#                         print(level.attrib)
#                         schema=level.attrib['schema']
#                         table_name =level.attrib['name']
#                         print(schema+':'+table_name)
#                     else:
#                         levelid=inx
#                         print(level.attrib)
#
#
#                         for key, values in level.attrib.items():
#                              if key in ['name','visible','table','column','nameColumn','captionColumn']:
#                                 print(key,values)
#
#
#
# result = parse.findall('Dimension') # 只能找到子节点 第一层
# print(result)
