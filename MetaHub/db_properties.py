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
        'host': '******',
        'port': 3306,
        'name': '******',
        'username': '******',
        'passwd': '******'
    }
}

MYSQL_CONFIG_PRE = {
    'metahub': {
        'host': '******',
        'port': 3306,
        'name': '******',
        'username': '******',
        'passwd': '******'
    },
    'metahub2': {
        'host': '******',
        'port': 3306,
        'name': '******',
        'username': '******',
        'passwd': '******'
    }
}

MYSQL_CONFIG_ONLINE = {
    'metahub': {
        'host': '******',
        'port': 3306,
        'name': '******',
        'username': '******',
        'passwd': '******'
    }
}

HIVE_CONFIG_ONLINE = {
    'host': '******',
    'port': 10000,
    'username': 'hdfs'
}

HIVE_META_CONFIG_DI = {
    'hive_new': {
        'host': '******',
        'port': 3306,
        'name': '******',
        'username': '******',
        'passwd': '******'
    }
}

# tbls，表名，TBL_ID，CREATE_TIME，TBL_NAME，TBL_TYPE，SD_ID
# sds，文件存储基本信息，SD_ID，CD_ID，
# partition_keys, 分区名
# partition，分区信息，最早分区，最晚分区
# columns_v2, 列信息，CD_ID


