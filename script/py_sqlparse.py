from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
import json
import  configparser
from sqlparse.tokens import Keyword, Name
import kylin_api
import sqlparse
from sqlparse.sql import IdentifierList, Identifier, Where
from sqlparse.tokens import Keyword, DML
import pandas as pd
from   sqlalchemy import create_engine

RESULT_OPERATIONS = {'UNION', 'INTERSECT', 'EXCEPT', 'SELECT'}
ON_KEYWORD = 'ON'
PRECEDES_TABLE_NAME = {'FROM', 'JOIN', 'DESC', 'DESCRIBE', 'WITH'}


cf = configparser.ConfigParser()

cf.read('/Users/wonder/Documents/data_dict/kylin/script/conf/kylin_conf.ini')
secs = cf.sections()
print(secs)
option = cf.options("kylin-pwd")
v_pwd = cf.get("kylin-pwd", "pwd")  # 获取[Mysql-Database]中host对应的值
v_url = cf.get("kylin-pwd", "url")  # 获取[Mysql-Database]中host对应的值
v_project = 'iqj_olap'
option_db = cf.options("mysql")
v_model_cube_mapping = cf.get("mysql", "model_cube_mapping")  # 获取[Mysql-Database]中host对应的值
v_hbase_cube_mapping = cf.get("mysql", "hbase_cube_mapping")  # 获取[Mysql-Database]中host对应的值
v_cube_table_mapping=cf.get("mysql", "cube_table_mapping")

class BaseExtractor(object):
    def __init__(self, sql_statement):
        self.sql = sqlparse.format(sql_statement, reindent=True, keyword_case='upper')
        self._table_names = set()
        self._alias_names = set()
        self._limit = None
        self._parsed = sqlparse.parse(self.stripped())
        for statement in self._parsed:
            self.__extract_from_token(statement)
            self._limit = self._extract_limit_from_query(statement)
        self._table_names = self._table_names - self._alias_names

    @property
    def tables(self):
        return self._table_names

    @property
    def limit(self):
        return self._limit

    def is_select(self):
        return self._parsed[0].get_type() == 'SELECT'

    def is_explain(self):
        return self.stripped().upper().startswith('EXPLAIN')

    def is_readonly(self):
        return self.is_select() or self.is_explain()

    def stripped(self):
        return self.sql.strip(' \t\n;')

    def get_statements(self):
        statements = []
        for statement in self._parsed:
            if statement:
                sql = str(statement).strip(' \n;\t')
                if sql:
                    statements.append(sql)
        return statements

    @staticmethod
    def __precedes_table_name(token_value):
        for keyword in PRECEDES_TABLE_NAME:
            if keyword in token_value:
                return True
        return False

    @staticmethod
    def get_full_name(identifier):
        if len(identifier.tokens) > 1 and identifier.tokens[1].value == '.':
            return '{}.{}'.format(identifier.tokens[0].value,
                                  identifier.tokens[2].value)
        return identifier.get_real_name()

    @staticmethod
    def __is_result_operation(keyword):
        for operation in RESULT_OPERATIONS:
            if operation in keyword.upper():
                return True
        return False

    @staticmethod
    def __is_identifier(token):
        return isinstance(token, (IdentifierList, Identifier))

    def __process_identifier(self, identifier):
        if '(' not in '{}'.format(identifier):
            self._table_names.add(self.get_full_name(identifier))
            return

        # store aliases
        if hasattr(identifier, 'get_alias'):
            self._alias_names.add(identifier.get_alias())
        if hasattr(identifier, 'tokens'):
            # some aliases are not parsed properly
            if identifier.tokens[0].ttype == Name:
                self._alias_names.add(identifier.tokens[0].value)
        self.__extract_from_token(identifier)

    def as_create_table(self, table_name, overwrite=False):
        exec_sql = ''
        sql = self.stripped()
        if overwrite:
            exec_sql = 'DROP TABLE IF EXISTS {};\n'.format(table_name)
        exec_sql += 'CREATE TABLE {} AS \n{}'.format(table_name, sql)
        return exec_sql

    def __extract_from_token(self, token):
        if not hasattr(token, 'tokens'):
            return

        table_name_preceding_token = False

        for item in token.tokens:
            if item.is_group and not self.__is_identifier(item):
                self.__extract_from_token(item)

            if item.ttype in Keyword:
                if self.__precedes_table_name(item.value.upper()):
                    table_name_preceding_token = True
                    continue

            if not table_name_preceding_token:
                continue

            if item.ttype in Keyword or item.value == ',':
                if (self.__is_result_operation(item.value) or
                        item.value.upper() == ON_KEYWORD):
                    table_name_preceding_token = False
                    continue
                # FROM clause is over
                break

            if isinstance(item, Identifier):
                self.__process_identifier(item)

            if isinstance(item, IdentifierList):
                for token in item.tokens:
                    if self.__is_identifier(token):
                        self.__process_identifier(token)

    def _get_limit_from_token(self, token):
        if token.ttype == sqlparse.tokens.Literal.Number.Integer:
            return int(token.value)
        elif token.is_group:
            return int(token.get_token_at_offset(1).value)

    def _extract_limit_from_query(self, statement):
        limit_token = None
        for pos, item in enumerate(statement.tokens):
            if item.ttype in Keyword and item.value.lower() == 'limit':
                limit_token = statement.tokens[pos + 2]
                return self._get_limit_from_token(limit_token)

    def get_query_with_new_limit(self, new_limit):
        if not self._limit:
            return self.sql + ' LIMIT ' + str(new_limit)
        limit_pos = None
        tokens = self._parsed[0].tokens
        # Add all items to before_str until there is a limit
        for pos, item in enumerate(tokens):
            if item.ttype in Keyword and item.value.lower() == 'limit':
                limit_pos = pos
                break
        limit = tokens[limit_pos + 2]
        if limit.ttype == sqlparse.tokens.Literal.Number.Integer:
            tokens[limit_pos + 2].value = new_limit
        elif limit.is_group:
            tokens[limit_pos + 2].value = (
                '{}, {}'.format(next(limit.get_identifiers()), new_limit)
            )

        str_res = ''
        for i in tokens:
            str_res += str(i.value)
        return str_res


class SqlExtractor(BaseExtractor):
    """提取sql语句"""

    @staticmethod
    def get_full_name(identifier, including_dbs=False):
        if len(identifier.tokens) > 1 and identifier.tokens[1].value == '.':
            a = identifier.tokens[0].value
            b = identifier.tokens[2].value
            db_table = (a, b)
            full_tree = '{}.{}'.format(a, b)
            if len(identifier.tokens) == 3:
                return full_tree
            else:
                i = identifier.tokens[3].value
                c = identifier.tokens[4].value
                if i == ' ':
                    return full_tree
                full_tree = '{}.{}.{}'.format(a, b, c)
                return full_tree
        return None, None


if __name__ == '__main__':

    cf = configparser.ConfigParser()

    cf.read('/Users/wonder/Documents/data_dict/kylin/script/conf/kylin_conf.ini')
    secs = cf.sections()
    print(secs)
    option = cf.options("kylin-pwd")
    v_pwd = cf.get("kylin-pwd", "pwd")  # 获取[Mysql-Database]中host对应的值
    v_url = cf.get("kylin-pwd", "url")  # 获取[Mysql-Database]中host对应的值
    v_project = 'iqj_olap'
    resp, resp_code=kylin_api.http_get_cube(v_url,'all')
    load_dict = json.loads(resp)
    all_cube_table_list=[]
    for cube in load_dict:
        if (cube['project'])=='iqj_olap' and (cube['status'])=='READY':
            print(cube['name'])
            v_cube_name =cube['name']

            resp, resp_code, cook = kylin_api.http_get_cube_sql(v_url, v_cube_name)
            load_dict = json.loads(resp)
            sql = (load_dict['sql'])
            sql_extractor = SqlExtractor(sql)


            for table in sql_extractor.tables:
                cube_list = []
                print(v_cube_name + ':'+table)
                cube_list.append(v_cube_name)
                cube_list.append(table.split('.')[0])
                cube_list.append(table.split('.')[1])

                all_cube_table_list.append(cube_list)
        print(all_cube_table_list)

    all_cube_table_df = pd.DataFrame(all_cube_table_list,columns=['cube_name', 'hive_db', 'hive_table'])

    DB_CONNECT = 'mysql+pymysql://root:20131130@localhost:3306/finup_db4test?charset=utf8'

            # DB_CONNECT = 'mysql+pymysql://data_window:zPcQl%R2xWJ!ByXe@192.168.198.30:3306/data_window?charset=utf8'
    conn = create_engine(DB_CONNECT, echo=True, pool_size=10, max_overflow=20)
    all_cube_table_df.to_sql(name=v_cube_table_mapping, con=conn, if_exists='replace', index=False)

    # if (load_dict['project'])=='iqj_olap':
    #     print(load_dict['name'])
    #     print(load_dict['snapshots'])

    #
    # print(resp)
    exit()

#
#
# sql="""
# SELECT
# `F_IQJ_SCA_INVEST`.`INVEST_DATE` as `F_IQJ_SCA_INVEST_INVEST_DATE`
# ,`F_IQJ_SCA_INVEST`.`INVEST_HOUR` as `F_IQJ_SCA_INVEST_INVEST_HOUR`
# ,`F_IQJ_SCA_INVEST`.`PRODUCT_ID` as `F_IQJ_SCA_INVEST_PRODUCT_ID`
# ,`F_IQJ_SCA_INVEST`.`PRODUCT_PERIOD` as `F_IQJ_SCA_INVEST_PRODUCT_PERIOD`
# ,`F_IQJ_SCA_INVEST`.`PERIOD_CD` as `F_IQJ_SCA_INVEST_PERIOD_CD`
# ,`F_IQJ_SCA_INVEST`.`INVEST_TYPE` as `F_IQJ_SCA_INVEST_INVEST_TYPE`
# ,`F_IQJ_SCA_INVEST`.`STATD_TYPE` as `F_IQJ_SCA_INVEST_STATD_TYPE`
# ,`F_IQJ_SCA_INVEST`.`NEW_CUST_FIRST_INVEST_DATE_FLAG` as `F_IQJ_SCA_INVEST_NEW_CUST_FIRST_INVEST_DATE_FLAG`
# ,`F_IQJ_SCA_INVEST`.`FIRST_INVEST_REGIST_DAY_FLAG_NEW` as `F_IQJ_SCA_INVEST_FIRST_INVEST_REGIST_DAY_FLAG_NEW`
# ,`F_IQJ_SCA_INVEST`.`FIRST_INVEST_REGIST_MONTHDIFF_FLAG_NEW` as `F_IQJ_SCA_INVEST_FIRST_INVEST_REGIST_MONTHDIFF_FLAG_NEW`
# ,`DIM_TIME_NEW`.`DATE_ID` as `DIM_TIME_NEW_DATE_ID`
# ,`DIM_TIME_NEW`.`M` as `DIM_TIME_NEW_M`
# ,`DIM_TIME_NEW`.`YW` as `DIM_TIME_NEW_YW`
# ,`DIM_TIME_NEW`.`W` as `DIM_TIME_NEW_W`
# ,`DIM_TIME_NEW`.`Q` as `DIM_TIME_NEW_Q`
# ,`DIM_TIME_NEW`.`W_NAME` as `DIM_TIME_NEW_W_NAME`
# ,`DIM_TIME_NEW`.`W_INT` as `DIM_TIME_NEW_W_INT`
# ,`DIM_IQJ_DW_INVEST_TYPE`.`PARENT_INVEST_TYPE` as `DIM_IQJ_DW_INVEST_TYPE_PARENT_INVEST_TYPE`
# ,`DIM_IQJ_DW_INVEST_TYPE`.`PARENT_INVEST_TYPE_NAME` as `DIM_IQJ_DW_INVEST_TYPE_PARENT_INVEST_TYPE_NAME`
# ,`DIM_IQJ_DW_INVEST_TYPE`.`INVEST_TYPE` as `DIM_IQJ_DW_INVEST_TYPE_INVEST_TYPE`
# ,`DIM_IQJ_DW_INVEST_TYPE`.`INVEST_TYPE_NAME` as `DIM_IQJ_DW_INVEST_TYPE_INVEST_TYPE_NAME`
# ,`FDA_PRO_PRODUCT_CODE_MAPPING`.`PRODUCT_ID` as `FDA_PRO_PRODUCT_CODE_MAPPING_PRODUCT_ID`
# ,`FDA_PRO_PRODUCT_CODE_MAPPING`.`PRODUCT_NAME` as `FDA_PRO_PRODUCT_CODE_MAPPING_PRODUCT_NAME`
# ,`DIM_IQJ_FDA_USER_INFO`.`CUST_ASSETS_TAG` as `DIM_IQJ_FDA_USER_INFO_CUST_ASSETS_TAG`
# ,`DIM_IQJ_FDA_USER_INFO`.`PARENT_ASSETS_TAG` as `DIM_IQJ_FDA_USER_INFO_PARENT_ASSETS_TAG`
# ,`DIM_IQJ_FDA_USER_INFO`.`PARENT_ASSETS_TAG_NAME` as `DIM_IQJ_FDA_USER_INFO_PARENT_ASSETS_TAG_NAME`
# ,`DIM_IQJ_FDA_USER_INFO`.`ASSETS_TAG` as `DIM_IQJ_FDA_USER_INFO_ASSETS_TAG`
# ,`DIM_IQJ_FDA_USER_INFO`.`ORDER_TAG` as `DIM_IQJ_FDA_USER_INFO_ORDER_TAG`
# ,`DIM_IQJ_FDA_USER_INFO`.`REGIST_M` as `DIM_IQJ_FDA_USER_INFO_REGIST_M`
# ,`DIM_IQJ_FDA_USER_INFO`.`REGIST_YW` as `DIM_IQJ_FDA_USER_INFO_REGIST_YW`
# ,`DIM_IQJ_FDA_USER_INFO`.`F_IQJ_FDA_USER_INFO_BIRTH_YEAR` as `DIM_IQJ_FDA_USER_INFO_F_IQJ_FDA_USER_INFO_BIRTH_YEAR`
# ,`DIM_IQJ_FDA_USER_INFO`.`DW_DIM_USER_AGE_AGE_NAME` as `DIM_IQJ_FDA_USER_INFO_DW_DIM_USER_AGE_AGE_NAME`
# ,`DIM_IQJ_FDA_USER_INFO`.`BIRTH_YEAR` as `DIM_IQJ_FDA_USER_INFO_BIRTH_YEAR`
# ,`DIM_IQJ_FDA_USER_INFO`.`AGE_NAME` as `DIM_IQJ_FDA_USER_INFO_AGE_NAME`
# ,`DIM_IQJ_FDA_USER_INFO`.`FIRST_INVEST_M` as `DIM_IQJ_FDA_USER_INFO_FIRST_INVEST_M`
# ,`DIM_IQJ_FDA_USER_INFO`.`FIRST_INVEST_YW` as `DIM_IQJ_FDA_USER_INFO_FIRST_INVEST_YW`
# ,`DIM_IQJ_FDA_USER_INFO`.`CITY` as `DIM_IQJ_FDA_USER_INFO_CITY`
# ,`DIM_IQJ_FDA_USER_INFO`.`PROVE` as `DIM_IQJ_FDA_USER_INFO_PROVE`
# ,`DIM_IQJ_FDA_USER_INFO`.`CUR_APPLY_EXIT_FLAG_NAME` as `DIM_IQJ_FDA_USER_INFO_CUR_APPLY_EXIT_FLAG_NAME`
# ,`DIM_IQJ_FDA_USER_INFO`.`SEX` as `DIM_IQJ_FDA_USER_INFO_SEX`
# ,`DIM_IQJ_FDA_USER_INFO`.`SEX_NAME` as `DIM_IQJ_FDA_USER_INFO_SEX_NAME`
# ,`DIM_IQJ_FDA_USER_INFO`.`LAST_LOGIN_MONTH_RANGE` as `DIM_IQJ_FDA_USER_INFO_LAST_LOGIN_MONTH_RANGE`
# ,`DIM_IQJ_FDA_USER_INFO`.`LAST_MONTH_LATEST_ZHI_INVEST_MONTH_RANGE` as `DIM_IQJ_FDA_USER_INFO_LAST_MONTH_LATEST_ZHI_INVEST_MONTH_RANGE`
# ,`DIM_IQJ_FDA_USER_INFO`.`LAST_MONTH_LATEST_LONG_PERIODS_INVEST_MONTH_RANGE` as `DIM_IQJ_FDA_USER_INFO_LAST_MONTH_LATEST_LONG_PERIODS_INVEST_MONTH_RANGE`
# ,`DIM_IQJ_FDA_USER_INFO`.`CURR_MONTH_ZCB_MATURED_FLAG` as `DIM_IQJ_FDA_USER_INFO_CURR_MONTH_ZCB_MATURED_FLAG`
# ,`DIM_IQJ_FDA_USER_INFO`.`DUANCHI_MONTH_RANGE` as `DIM_IQJ_FDA_USER_INFO_DUANCHI_MONTH_RANGE`
# ,`DIM_IQJ_FDA_USER_INFO`.`AGE_RANGE` as `DIM_IQJ_FDA_USER_INFO_AGE_RANGE`
# ,`DIM_IQJ_FDA_USER_INFO`.`LAST_LOGIN_DATE_RANGE` as `DIM_IQJ_FDA_USER_INFO_LAST_LOGIN_DATE_RANGE`
# ,`DIM_IQJ_FDA_USER_INFO`.`LAST_DAY_LATEST_ZHI_INVEST_DATE_RANGE` as `DIM_IQJ_FDA_USER_INFO_LAST_DAY_LATEST_ZHI_INVEST_DATE_RANGE`
# ,`DIM_IQJ_FDA_USER_INFO`.`CITY_LEVEL` as `DIM_IQJ_FDA_USER_INFO_CITY_LEVEL`
# ,`DIM_IQJ_FDA_USER_INFO`.`CITY_LEVEL_ORDER` as `DIM_IQJ_FDA_USER_INFO_CITY_LEVEL_ORDER`
# ,`DIM_IQJ_FDA_USER_INFO`.`CURR_MONTH_AYB_MATURED_FLAG_NAME` as `DIM_IQJ_FDA_USER_INFO_CURR_MONTH_AYB_MATURED_FLAG_NAME`
# ,`DIM_IQJ_FDA_USER_INFO`.`CHANNEL_LEV1` as `DIM_IQJ_FDA_USER_INFO_CHANNEL_LEV1`
# ,`DIM_IQJ_FDA_USER_INFO`.`CHANNEL_LEV2` as `DIM_IQJ_FDA_USER_INFO_CHANNEL_LEV2`
# ,`DIM_IQJ_FDA_USER_INFO`.`CHANNEL_LEV3` as `DIM_IQJ_FDA_USER_INFO_CHANNEL_LEV3`
# ,`DIM_IQJ_FDA_USER_INFO`.`CHANNEL_LEV4` as `DIM_IQJ_FDA_USER_INFO_CHANNEL_LEV4`
# ,`DIM_DATEDIFF`.`DIFF_FLAG` as `DIM_DATEDIFF_DIFF_FLAG`
# ,`DIM_DATEDIFF`.`FLAG` as `DIM_DATEDIFF_FLAG`
# ,`DIM_DATEDIFF_MONTH`.`DIFF_FLAG` as `DIM_DATEDIFF_MONTH_DIFF_FLAG`
# ,`DIM_DATEDIFF_MONTH`.`FLAG` as `DIM_DATEDIFF_MONTH_FLAG`
# ,`DIM_COMMON_DICT`.`DICT_KEY` as `DIM_COMMON_DICT_DICT_KEY`
# ,`DIM_COMMON_DICT`.`DICT_VAULE` as `DIM_COMMON_DICT_DICT_VAULE`
# ,`F_IQJ_SCA_INVEST`.`REGIST_DATE` as `F_IQJ_SCA_INVEST_REGIST_DATE`
# ,`DIM_TIME_NEW_REG`.`DATE_ID` as `DIM_TIME_NEW_REG_DATE_ID`
# ,`DIM_TIME_NEW_REG`.`M` as `DIM_TIME_NEW_REG_M`
# ,`DIM_TIME_NEW_REG`.`YW` as `DIM_TIME_NEW_REG_YW`
# ,`DIM_TIME_NEW_REG`.`Q` as `DIM_TIME_NEW_REG_Q`
# ,`DIM_TIME_NEW_REG`.`W_NAME` as `DIM_TIME_NEW_REG_W_NAME`
# ,`F_IQJ_SCA_INVEST`.`INVEST_AMT` as `F_IQJ_SCA_INVEST_INVEST_AMT`
# ,`F_IQJ_SCA_INVEST`.`REDBAG_AMT` as `F_IQJ_SCA_INVEST_REDBAG_AMT`
# ,`F_IQJ_SCA_INVEST`.`ORDER_REWARD_AMT` as `F_IQJ_SCA_INVEST_ORDER_REWARD_AMT`
# ,`F_IQJ_SCA_INVEST`.`TRANS_REWARD_AMT` as `F_IQJ_SCA_INVEST_TRANS_REWARD_AMT`
# ,`F_IQJ_SCA_INVEST`.`CUST_ID` as `F_IQJ_SCA_INVEST_CUST_ID`
#  FROM `KYLIN`.`F_IQJ_SCA_INVEST` as `F_IQJ_SCA_INVEST`
# LEFT JOIN `DATAWINDOW_DW`.`DIM_TIME_NEW` as `DIM_TIME_NEW`
# ON `F_IQJ_SCA_INVEST`.`INVEST_DATE` = `DIM_TIME_NEW`.`DATE_ID`
# LEFT JOIN `KYLIN`.`DIM_IQJ_DW_INVEST_TYPE` as `DIM_IQJ_DW_INVEST_TYPE`
# ON `F_IQJ_SCA_INVEST`.`INVEST_TYPE` = `DIM_IQJ_DW_INVEST_TYPE`.`INVEST_TYPE`
# LEFT JOIN `IQJ_FDA`.`FDA_PRO_PRODUCT_CODE_MAPPING` as `FDA_PRO_PRODUCT_CODE_MAPPING`
# ON `F_IQJ_SCA_INVEST`.`PRODUCT_ID` = `FDA_PRO_PRODUCT_CODE_MAPPING`.`PRODUCT_ID`
# LEFT JOIN `KYLIN`.`DIM_IQJ_FDA_USER_INFO` as `DIM_IQJ_FDA_USER_INFO`
# ON `F_IQJ_SCA_INVEST`.`CUST_ID` = `DIM_IQJ_FDA_USER_INFO`.`CUST_ID`
# LEFT JOIN `KYLIN`.`DIM_DATEDIFF` as `DIM_DATEDIFF`
# ON `F_IQJ_SCA_INVEST`.`FIRST_INVEST_REGIST_DAY_FLAG_NEW` = `DIM_DATEDIFF`.`DIFF_FLAG`
# LEFT JOIN `KYLIN`.`DIM_DATEDIFF` as `DIM_DATEDIFF_MONTH`
# ON `F_IQJ_SCA_INVEST`.`FIRST_INVEST_REGIST_MONTHDIFF_FLAG_NEW` = `DIM_DATEDIFF_MONTH`.`DIFF_FLAG`
# LEFT JOIN `KYLIN`.`DIM_COMMON_DICT` as `DIM_COMMON_DICT`
# ON `F_IQJ_SCA_INVEST`.`NEW_CUST_FIRST_INVEST_DATE_FLAG` = `DIM_COMMON_DICT`.`DICT_KEY`
# LEFT JOIN `DATAWINDOW_DW`.`DIM_TIME_NEW` as `DIM_TIME_NEW_REG`
# ON `F_IQJ_SCA_INVEST`.`REGIST_DATE` = `DIM_TIME_NEW_REG`.`DATE_ID`
# WHERE 1=1;
#
# """
#
#
# def is_subselect(parsed):
#     if not parsed.is_group:
#         return False
#     for item in parsed.tokens:
#         if item.ttype is DML and item.value.upper() == 'SELECT':
#             return True
#     return False
#
#
# def extract_from_part(parsed):
#     from_seen = False
#     for item in parsed.tokens:
#         if from_seen:
#             if is_subselect(item):
#                 for x in extract_from_part(item):
#                     yield x
#             elif item.ttype is Keyword:
#                 raise StopIteration
#             else:
#                 yield item
#         elif item.ttype is Keyword and item.value.upper() in ['FROM','JOIN']:
#             from_seen = True
#
#
# def extract_table_identifiers(token_stream):
#     for item in token_stream:
#         if isinstance(item, IdentifierList):
#             for identifier in item.get_identifiers():
#                 yield identifier.get_name()
#         elif isinstance(item, Identifier):
#             yield item.get_name()
#         # It's a bug to check for Keyword here, but in the example
#         # above some tables names are identified as keywords...
#         elif item.ttype is Keyword:
#             yield item.value
#
#
# def extract_tables(sql):
#     stream = extract_from_part(sqlparse.parse(sql)[0])
#     return list(extract_table_identifiers(stream))
#
# if __name__ == '__main__':
#     print ('Tables: %s' % ', '.join(extract_tables(sql)))
#
#
# # meas = ["USER_NUM",
# #         "SUM_ASSET",
# #         "MAX_ASSET",
# #         "USER_ASSET_NUM",
# #         "ID_VERIFIED_NUM",
# #         "CURR_DAY_AYB_MATURED_CNT",
# #         "CURR_DAY_EXIT_AMT",
# #         "CURR_DAY_TRANS_INVEST_USER_NUM",
# #         "CURR_DAY_TRANS_INVEST_CNT",
# #         "CURR_DAY_TRANS_INVEST_AMOUNT",
# #         "CURR_DAY_ZHI_INVEST_USER_NUM",
# #         "CURR_DAY_ZHI_INVEST_CNT",
# #         "CURR_DAY_ZHI_INVEST_AMOUNT",
# #         "CURR_DAY_CONTINUE_USER_NUM",
# #         "CURR_DAY_CONTINUE_CNT",
# #         "CURR_DAY_CONTINUE_AMOUNT",
# #         "WAITING_EXIT_ASSETS",
# #         "CAN_EXIT_ASSETS",
# #         "CURR_DAY_APPLY_EXIT_ASSETS",
# #         "ACCUM_WITHDRAW_CNT",
# #         "ACCUM_WITHDRAW_ASSETS",
# #         "ACCUM_RECHARGE_CNT",
# #         "ACCUM_RECHARGE_ASSETS",
# #         "CURR_DAY_AYB_MATURED_ASSETS",
# #         "CUST_NAME"]
# #
# # for i in meas:
# #
# #
# #     print('<Measure name="'+i+'" column="'  + i+  '" datatype="Numeric" formatString="#,###.00" aggregator="sum" visible="true">'+ '\n'+ '</Measure>')
# #
# #
# # list1 = ['Google', 'Runoob', 'Taobao']
# # list1.append('Baidu')
# # print ("更新后的列表 : ", list1)
# # a_list=[]
# # # b='2'
# # #
# # # a_list.append(b)
# # # print(a_list)
#
#
# # import base64
# #
# # str = 'cnblogs'
# # str =bytes(str)
# # str64 = base64.b64encode(str)
# # # print(str64)  # Y25ibG9ncw==
# # # print(base64.b64decode(str64))  # cnblog
# #
#
#
# a='a'
# b='b'
# print(a)
# print (type(a))
# print (type(b))
#
# print(a+b)