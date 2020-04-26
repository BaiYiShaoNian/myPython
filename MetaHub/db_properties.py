# coding:utf8
"""
file name: db_properties.py
date: 2020/3/18
author: huangxuanfeng
"""
import sys, os
import time, datetime

reload(sys)
sys.setdefaultencoding('utf-8')

MYSQL_CONFIG_DEV = {
    'metahub': {
        'host': '172.16.1.31',
        'port': 3306,
        'name': 'keep_melon_server',
        'username': 'root',
        'passwd': 'keep_1234'
    }
}

MYSQL_CONFIG_PRE = {
    'metahub': {
        'host': 'premysql01.rds.svc.ali.keep',
        'port': 3306,
        'name': 'metahub',
        'username': 'k_metahub',
        'passwd': 'r3fyfi4OL6TGUnVM'
    },
    'metahub2': {
        'host': 'premysql01.rds.svc.ali.keep',
        'port': 3306,
        'name': 'tagm',
        'username': 'k_tagm',
        'passwd': '3MqNobFeSO8Rsauu'
    }
}

MYSQL_CONFIG_ONLINE = {
    'metahub': {
        'host': 'metahub.rds.svc.ali.keep',
        'port': 3306,
        'name': 'metahub',
        'username': 'k_metahub',
        'passwd': 'r3fyfi4OL6TGUnVD'
    }
}

HIVE_CONFIG_ONLINE = {
    'host': 'bj-hd-di-14.ali.keep',
    'port': 10000,
    'username': 'hdfs'
}

HIVE_META_CONFIG_DI = {
    'hive_new': {
        'host': 'cdh.rds.svc.ali.keep',
        'port': 3306,
        'name': 'hive_new',
        'username': 'datakeep',
        'passwd': 'R6dHdnZ9n4qHclLy'
    }
}

# mysql -hcdh.rds.svc.ali.keep -udatakeep -pR6dHdnZ9n4qHclLy hive_new
# tbls，表名，TBL_ID，CREATE_TIME，TBL_NAME，TBL_TYPE，SD_ID
# sds，文件存储基本信息，SD_ID，CD_ID，
# partition_keys, 分区名
# partition，分区信息，最早分区，最晚分区
# columns_v2, 列信息，CD_ID


