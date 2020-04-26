# coding:utf8
"""
file name: meta_sql.py
date: 2020/3/19
author: huangxuanfeng
"""
import sys, os
import time, datetime

reload(sys)
sys.setdefaultencoding('utf-8')

DB_ID_SQL = """select DB_ID from dbs where NAME = '%s'"""

TBL_INFO_SQL = """select TBL_ID,CREATE_TIME,DB_ID,SD_ID,TBL_NAME from tbls where DB_ID = %d"""

TBL_PKEY_SQL = """
    select b.TBL_NAME, group_concat(f.PKEY_NAME separator ',')

    from (
      select DB_ID from dbs where NAME = '%s'
    )a

    left join (
      select TBL_ID,CREATE_TIME,DB_ID,SD_ID,TBL_NAME from tbls
    )b on a.DB_ID = b.DB_ID

    left join (
      select PKEY_NAME, TBL_ID from partition_keys
    )f on b.TBL_ID = f.TBL_ID
    
    group by b.TBL_NAME
"""

TBL_PART_NAME_SQL = """
    select b.TBL_NAME, min(d.PART_NAME), max(d.PART_NAME)
    
    from (
      select DB_ID from dbs where NAME = '%s'
    )a
    
    left join (
      select TBL_ID,CREATE_TIME,DB_ID,SD_ID,TBL_NAME from tbls
    )b on a.DB_ID = b.DB_ID
    
    left join (
      select PART_NAME,TBL_ID from partitions
    )d on b.TBL_ID = d.TBL_ID
    
    group by b.TBL_NAME
"""

TBL_COLUMN_SQL = """
    select b.TBL_NAME, e.COLUMN_NAME, e.TYPE_NAME, e.COMMENT

    from (
      select DB_ID from dbs where NAME = '%s'
    )a

    left join (
      select TBL_ID,CREATE_TIME,DB_ID,SD_ID,TBL_NAME from tbls
    )b on a.DB_ID = b.DB_ID

    left join (
      select CD_ID, SD_ID from sds 
    )c on b.SD_ID = c.SD_ID

    left join (
      select CD_ID, COMMENT, COLUMN_NAME, TYPE_NAME, INTEGER_IDX from columns_v2
    )e on c.CD_ID = e.CD_ID
"""

TBL_ROLE_PRIVS_SQL = """
    select b.TBL_NAME, group_concat(c.PRINCIPAL_NAME separator ',')
    
    from (
      select DB_ID from dbs where NAME = '%s'
    )a

    inner join (
      select TBL_ID,CREATE_TIME,DB_ID,SD_ID,TBL_NAME from tbls
    )b on a.DB_ID = b.DB_ID
    
    left join (
      select TBL_ID,PRINCIPAL_NAME from tbl_privs
    )c on b.TBL_ID = c.TBL_ID
    
    group by b.TBL_NAME
"""




