#!/usr/bin/env python
# -*- coding:utf-8 -*-
#coding: UTF-8


import json
import xmltodict
import  sys
#定义xml转json的函数

import sys
print(sys.getdefaultencoding())
def xmltojson(xmlstr):#parse是的xml解析器
    xmlparse = xmltodict.parse(xmlstr)
    #json库dumps()是将dict转化成json格式，loads()是将json转化成dict格式。
    #dumps()方法的ident=1，格式化json
    jsonstr = json.dumps(xmlparse,indent=1,ensure_ascii=False)
    print(jsonstr)
#
# xml ="""
# <student  name="中文-测试">
#   <stid>10213</stid>
#   <info>
#     <name>name</name>
#     <sex>male</sex>
#   </info>
#   <course>
#     <name>math</name>
#     <score>90</score>
#   </course>
# </student>
#
# """
#
xml="""

<Cube name="投资主题" caption="投资主题" visible="true" cache="true" enabled="true">
    <Table name="F_IQJ_SCA_INVEST" schema="KYLIN">
    </Table>
    <Dimension type="StandardDimension" visible="true" highCardinality="false" name="(必要)统计类型">
        <Hierarchy name="统计类型" visible="true" hasAll="true">
            <Table name="F_IQJ_SCA_INVEST" schema="KYLIN">
            </Table>
            <Level name="投资类型" visible="true" column="STATD_TYPE" nameColumn="STATD_TYPE" type="String" uniqueMembers="false" levelType="Regular" hideMemberIf="Never">
            </Level>
        </Hierarchy>
    </Dimension>
    <Dimension type="StandardDimension" visible="true" foreignKey="INVEST_DATE" highCardinality="false" name="1投资(预约)时间">
        <Hierarchy name="投资时间层级" visible="true" hasAll="true" primaryKey="DATE_ID">
            <Table name="DIM_TIME_NEW" schema="DATAWINDOW_DW">
            </Table>
            <Level name="年" visible="true" table="DIM_TIME_NEW" column="YW" nameColumn="YW" type="String" uniqueMembers="false" levelType="Regular" hideMemberIf="Never">
            </Level>
            <Level name="季度" visible="true" table="DIM_TIME_NEW" column="Q" nameColumn="Q" type="String" uniqueMembers="false" levelType="Regular" hideMemberIf="Never">
            </Level>
            <Level name="月份" visible="true" table="DIM_TIME_NEW" column="M" nameColumn="M" type="String" uniqueMembers="false" levelType="Regular" hideMemberIf="Never">
            </Level>
            <Level name="投资日" visible="true" table="DIM_TIME_NEW" column="DATE_ID" nameColumn="DATE_ID" type="String" uniqueMembers="false" levelType="Regular" hideMemberIf="Never">
            </Level>
        </Hierarchy>
        <Hierarchy name="投资时间层级(周)" visible="true" hasAll="true" primaryKey="DATE_ID">
            <Table name="DIM_TIME_NEW" schema="DATAWINDOW_DW">
            </Table>
            <Level name="年" visible="true" table="DIM_TIME_NEW" column="YW" nameColumn="YW" type="String" uniqueMembers="false" levelType="Regular" hideMemberIf="Never">
            </Level>
            <Level name="周" visible="true" table="DIM_TIME_NEW" column="W" nameColumn="W" ordinalColumn="W_INT" type="Integer" internalType="int" uniqueMembers="false" levelType="Regular" hideMemberIf="Never" captionColumn="W_NAME">
            </Level>
            <Level name="投资日" visible="true" table="DIM_TIME_NEW" column="DATE_ID" nameColumn="DATE_ID" type="String" uniqueMembers="false" levelType="Regular" hideMemberIf="Never">
            </Level>
        </Hierarchy>
    </Dimension>
    <Dimension type="StandardDimension" visible="true" foreignKey="PRODUCT_ID" highCardinality="false" name="2理财产品">
        <Hierarchy name="理财产品" visible="true" hasAll="true" primaryKey="PRODUCT_ID">
            <Table name="FDA_PRO_PRODUCT_CODE_MAPPING" schema="IQJ_FDA">
            </Table>
            <Level name="理财产品" visible="true" table="FDA_PRO_PRODUCT_CODE_MAPPING" column="PRODUCT_ID" nameColumn="PRODUCT_NAME" ordinalColumn="PRODUCT_ID" type="String" uniqueMembers="true" levelType="Regular" hideMemberIf="Never">
            </Level>
        </Hierarchy>
    </Dimension>
    <Dimension type="StandardDimension" visible="true" foreignKey="INVEST_TYPE" highCardinality="false" name="3投资类型">
        <Hierarchy name="投资类型" visible="true" hasAll="true" primaryKey="INVEST_TYPE">
            <Table name="DIM_IQJ_DW_INVEST_TYPE" schema="KYLIN">
            </Table>
            <Level name="投资大类" visible="true" table="DIM_IQJ_DW_INVEST_TYPE" column="PARENT_INVEST_TYPE" nameColumn="PARENT_INVEST_TYPE_NAME" type="String" uniqueMembers="false" levelType="Regular" hideMemberIf="Never">
            </Level>
            <Level name="类型" visible="true" table="DIM_IQJ_DW_INVEST_TYPE" column="INVEST_TYPE" nameColumn="INVEST_TYPE_NAME" type="String" uniqueMembers="true" levelType="Regular" hideMemberIf="Never">
            </Level>
        </Hierarchy>
    </Dimension>
    <Dimension type="StandardDimension" visible="true" highCardinality="false" name="4投资期限">
        <Hierarchy name="投资期限" visible="true" hasAll="true">
            <Table name="F_IQJ_SCA_INVEST" schema="KYLIN">
            </Table>
            <Level name="单位" visible="true" column="PERIOD_CD" nameColumn="PERIOD_CD" type="String" uniqueMembers="false" levelType="Regular" hideMemberIf="Never">
            </Level>
            <Level name="期限" visible="true" column="PRODUCT_PERIOD" nameColumn="PRODUCT_PERIOD" type="String" uniqueMembers="false" levelType="Regular" hideMemberIf="Never">
            </Level>
        </Hierarchy>
    </Dimension>
    <Dimension type="StandardDimension" visible="true" foreignKey="CUST_ID" highCardinality="false" name="5用户属性">
        <Hierarchy name="在持标签" visible="true" hasAll="true" allLevelName="All层级-上月底在持标签" primaryKey="CUST_ID">
            <Table name="DIM_IQJ_FDA_USER_INFO" schema="KYLIN">
            </Table>
            <Level name="一级在持标签" visible="true" table="DIM_IQJ_FDA_USER_INFO" column="PARENT_ASSETS_TAG" nameColumn="PARENT_ASSETS_TAG_NAME" type="String" uniqueMembers="false" levelType="Regular" hideMemberIf="Never" captionColumn="PARENT_ASSETS_TAG_NAME">
            </Level>
            <Level name="在持标签" visible="true" table="DIM_IQJ_FDA_USER_INFO" column="CUST_ASSETS_TAG" nameColumn="CUST_ASSETS_TAG" ordinalColumn="ORDER_TAG" type="String" uniqueMembers="false" levelType="Regular" hideMemberIf="Never">
            </Level>
        </Hierarchy>
        <Hierarchy name="年龄" visible="false" hasAll="true" allLevelName="All层级-年龄" primaryKey="CUST_ID" caption="年龄所有">
            <Table name="DIM_IQJ_FDA_USER_INFO" schema="KYLIN">
            </Table>
            <Level name="年龄" visible="true" table="DIM_IQJ_FDA_USER_INFO" column="BIRTH_YEAR" nameColumn="AGE_NAME" type="String" uniqueMembers="false" levelType="Regular" hideMemberIf="Never">
            </Level>
        </Hierarchy>
        <Hierarchy name="注册日期" visible="true" hasAll="true" allLevelName="All层级-注册日期" primaryKey="CUST_ID">
            <Table name="DIM_IQJ_FDA_USER_INFO" schema="KYLIN">
            </Table>
            <Level name="注册年份" visible="true" table="DIM_IQJ_FDA_USER_INFO" column="REGIST_YW" nameColumn="REGIST_YW" type="String" uniqueMembers="false" levelType="Regular" hideMemberIf="Never">
            </Level>
            <Level name="注册月份" visible="true" table="DIM_IQJ_FDA_USER_INFO" column="REGIST_M" nameColumn="REGIST_M" type="String" uniqueMembers="false" levelType="Regular" hideMemberIf="Never">
            </Level>
        </Hierarchy>
    </Dimension>
    <Measure name="投资金额" column="INVEST_AMT" formatString="#,###.00" aggregator="sum" caption="投资金额" visible="true">
    </Measure>
    <Measure name="红包金额" column="REDBAG_AMT" formatString="#,###.00" aggregator="sum" caption="红包金额" visible="true">
    </Measure>
    <Measure name="预约奖励金额" column="ORDER_REWARD_AMT" formatString="#,###.00" aggregator="sum" caption="预约奖励金额" visible="true">
    </Measure>
    <Measure name="转投奖励金额" column="TRANS_REWARD_AMT" formatString="#,###.00" aggregator="sum" caption="转投奖励金额" visible="true">
    </Measure>
    <Measure name="投资笔数" column="F_CUST_ID" datatype="Integer" aggregator="count" caption="投资笔数" visible="true">
    </Measure>
    <Measure name="投资人数" column="F_CUST_ID" datatype="Integer" aggregator="distinct count" caption="投资人数" visible="true">
    </Measure>
</Cube>


"""
# xmltojson(xml.decode('gb2312'))
# xmltojson(xml) #调用函数
xmltojson(xml)
#b=json.dumps(xmltojson(xml),  ensure_ascii=False)


#
# import json
# a={}
# a[1] = u'中'
#
# #b=json.dumps(a,  ensure_ascii=False)
# b=json.dumps(xml,  ensure_ascii=False)
#
# print b
#
# 结果 ===> {"1": "中"}
