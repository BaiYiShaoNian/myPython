# coding:utf8
"""
file name: meta_dw.py
date: 2020/3/18
author: huangxuanfeng
"""
import sys, os
import time, datetime

reload(sys)
sys.setdefaultencoding('utf-8')

import re
import MySQLdb
from pyhive import hive

from db_properties import *
from meta_schema import *
from meta_sql import *
from log_config import *

SEPARATE_SPECIAL = '#%&'

class MetaHub:
    # metahub的MySQL存储
    host = None; port = None; name = None; username = None; passwd = None;
    mysql_conn = None; mysql_cursor = None

    # hive-server2连接，弃用
    hive_host = None; hive_port = None; hive_username = None; hive_conn = None; hive_cursor = None

    # 存储hive集群元数据的MySQL
    hive_meta_host = None; hive_meta_port = None; hive_meta_name = None; hive_meta_username = None; hive_meta_passwd = None
    hive_meta_conn = None; hive_meta_cursor = None

    meta_db = None
    meta_table = None

    logger = None

    def __init__(self, param_env='pre', param_db=None, param_table=None, param_log_path=None):
        self.meta_db = param_db
        self.meta_table = param_table

        self.logger = get_log_session(param_log_path)

        # self.hive_host = HIVE_CONFIG_ONLINE['host']
        # self.hive_port = HIVE_CONFIG_ONLINE['port']
        # self.hive_username = HIVE_CONFIG_ONLINE['username']

        self.hive_meta_host = HIVE_META_CONFIG_DI['hive_new']['host']
        self.hive_meta_port = HIVE_META_CONFIG_DI['hive_new']['port']
        self.hive_meta_name = HIVE_META_CONFIG_DI['hive_new']['name']
        self.hive_meta_username = HIVE_META_CONFIG_DI['hive_new']['username']
        self.hive_meta_passwd = HIVE_META_CONFIG_DI['hive_new']['passwd']

        self.host = MYSQL_CONFIG_PRE['metahub']['host']
        self.port = MYSQL_CONFIG_PRE['metahub']['port']
        self.name = MYSQL_CONFIG_PRE['metahub']['name']
        self.username = MYSQL_CONFIG_PRE['metahub']['username']
        self.passwd = MYSQL_CONFIG_PRE['metahub']['passwd']

        if str(param_env) == 'online':
            self.host = MYSQL_CONFIG_ONLINE['metahub']['host']
            self.port = MYSQL_CONFIG_ONLINE['metahub']['port']
            self.name = MYSQL_CONFIG_ONLINE['metahub']['name']
            self.username = MYSQL_CONFIG_ONLINE['metahub']['username']
            self.passwd = MYSQL_CONFIG_ONLINE['metahub']['passwd']

        self.mysql_conn = MySQLdb.connect(
            host=self.host,
            port=self.port,
            user=self.username,
            passwd=self.passwd,
            db=self.name,
            charset='utf8'
        )
        self.mysql_cursor = self.mysql_conn.cursor()

        self.hive_meta_conn = MySQLdb.connect(
            host=self.hive_meta_host,
            port=self.hive_meta_port,
            user=self.hive_meta_username,
            passwd=self.hive_meta_passwd,
            db=self.hive_meta_name,
            charset='utf8'
        )
        self.hive_meta_cursor = self.hive_meta_conn.cursor()

        # self.hive_conn = hive.connect(
        #     host=self.hive_host,
        #     port=self.hive_port,
        #     username=self.hive_username
        # )
        # self.hive_cursor = self.hive_conn.cursor()

    def close(self):
        self.mysql_cursor.close()
        self.mysql_conn.close()

        self.hive_meta_cursor.close()
        self.hive_meta_conn.close()

        # self.hive_cursor.close()
        # self.hive_conn.close()
        pass

    """ 获取hive表基本信息的元数据，对应metahub后端MySQL存储的 meta_dw, meta_change_info
    主要功能和流程：
    1、从hive集群元数据存储的MySQL拉取最新版数据，解析
    2、和metahub现有数据进行比对，得到 hive表新增、删除记录
    """
    def get_base_info(self):
        if self.meta_db is None:
            return
        print 'start exec get_base_info...'

        base_info_list = []
        dbs_map = {}

        # 获取库信息，ID和名称对应
        sql = DB_ID_SQL % self.meta_db
        self.hive_meta_cursor.execute(sql)
        db_id = self.hive_meta_cursor.fetchall()[0][0]
        dbs_map[self.meta_db] = db_id

        # 获取表信息，
        sql = TBL_INFO_SQL % int(dbs_map.get(self.meta_db))
        self.hive_meta_cursor.execute(sql)
        tbls_list = self.hive_meta_cursor.fetchall()

        # 获取表的分区名称
        sql = TBL_PKEY_SQL % self.meta_db
        self.hive_meta_cursor.execute(sql)
        tbl_pkey_list = self.hive_meta_cursor.fetchall()
        # print tbl_pkey_list

        # 获取表最小最大分区
        sql = TBL_PART_NAME_SQL % self.meta_db
        self.hive_meta_cursor.execute(sql)
        tbl_part_name_list = self.hive_meta_cursor.fetchall()
        # print tbl_part_name_list

        # 获取表查询权限信息
        sql = TBL_ROLE_PRIVS_SQL % self.meta_db
        self.hive_meta_cursor.execute(sql)
        tbl_role_privs_list = self.hive_meta_cursor.fetchall()

        tbl_pkey_map = {}
        for tbl_pkey in tbl_pkey_list:
            k = self.meta_db + '.' + str(tbl_pkey[0]).strip()
            v = str(tbl_pkey[1]).strip()
            tbl_pkey_map[k] = v

        tbl_part_name_map = {}
        for tbl_part_name in tbl_part_name_list:
            k = self.meta_db + '.' + str(tbl_part_name[0]).strip()
            min_part_name = str(tbl_part_name[1]).strip()
            max_part_name = str(tbl_part_name[2]).strip()
            min_object = re.findall(r'.*?p_date=([0-9-]+).*?', min_part_name)
            max_object = re.findall(r'.*?p_date=([0-9-]+).*?', max_part_name)
            min_part = min_object[0] if len(min_object) == 1 else ''
            max_part = max_object[0] if len(max_object) == 1 else ''
            tbl_part_name_map[k] = min_part + SEPARATE_SPECIAL + max_part

        tbl_role_privs_map = {}
        for tbl_privs in tbl_role_privs_list:
            k = str(tbl_privs[0]).strip()
            v = str(tbl_privs[1]).strip()
            tbl_role_privs_map[k] = v

        # 查找 metahub 现有表
        metahub_map = {}
        metahub_list = []
        dw = Meta_Dw()
        dw.status = 1
        dw.db_name = self.meta_db
        dw_ddl = Meta_Dw_DDL(dw)
        self.mysql_cursor.execute(dw_ddl.get_select())
        metahub_list = self.mysql_cursor.fetchall()
        for line in metahub_list:
            metahub_map[line[1]] = line[1]

        # name, db_name, status, pt_name, earliest_pt, latest_pt, product_style, creation, modification
        for line in tbls_list:
            tbl_id = int(line[0])
            create_time = int(line[1])
            db_id = int(line[2])
            sd_id = int(line[3])
            tbl_name = str(line[4])
            db_table = self.meta_db + '.' + tbl_name

            if metahub_map.get(tbl_name) == None:
                metahub_map[tbl_name] = tbl_name + SEPARATE_SPECIAL + 'add'
            else:
                metahub_map[tbl_name] = metahub_map.get(tbl_name) + SEPARATE_SPECIAL + 'has'

            base_info = Meta_Dw()
            base_info.name = tbl_name
            base_info.db_name = self.meta_db
            base_info.status = 1
            base_info.owner = ''
            base_info.description = ''
            base_info.pt_name = tbl_pkey_map[db_table]
            base_info.earliest_pt = tbl_part_name_map[db_table].split(SEPARATE_SPECIAL)[0]
            base_info.latest_pt = tbl_part_name_map[db_table].split(SEPARATE_SPECIAL)[1]
            base_info.product_style = '全量'
            base_info.product_time = ''
            base_info.workflow = ''
            base_info.auth_select = tbl_role_privs_map[tbl_name]
            base_info.creation = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(create_time))
            base_info.modification = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
            base_info_list.append(base_info)

        change_info_list = []
        # 记录哪些表被删除
        for k,v in metahub_map.items():
            name = k
            if len(v.split(SEPARATE_SPECIAL)) == 2 and v.split(SEPARATE_SPECIAL)[1] == 'has':
                continue
            status = ''
            description = ''
            if len(v.split(SEPARATE_SPECIAL)) == 2 and v.split(SEPARATE_SPECIAL)[1] == 'add':
                status = 1
                description = '新表增加'
            if len(v.split(SEPARATE_SPECIAL)) == 1:
                status = -1
                description = '表删除下线'
                base_info = Meta_Dw()
                base_info.name = name
                base_info.db_name = self.meta_db
                base_info.status = status
                base_info.modification = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
                base_info_list.append(base_info)

            if description != '':
                change_info = Meta_Change_Info()
                change_info.category = "数据仓库"
                change_info.name = name
                change_info.description = description
                change_info.creation = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
                change_info.modification = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
                change_info_list.append(change_info)

        self.load_into_mysql(base_info_list, Meta_Dw_DDL)
        self.load_into_mysql(change_info_list, Meta_Change_Info_DDL)

    """ 获取hive表的字段信息，对应metahub后端MySQL存储的 meta_dw_fields, meta_change_info
    主要功能和流程：
    1、从hive集群元数据存储的MySQL拉取最新版数据，解析
    2、和metahub现有数据进行比对，得到 字段增加、删除记录 和 字段类型修改、字段注释修改记录
    """
    def get_fields_info(self):
        if self.meta_db is None:
            return
        print 'start exec get_fields_info...'

        dw = Meta_Dw()
        dw.status = 1
        dw.db_name = self.meta_db
        dw_ddl = Meta_Dw_DDL(dw)
        self.mysql_cursor.execute(dw_ddl.get_select())
        result_list = self.mysql_cursor.fetchall()
        dw_id_name_map = {}
        dw_name_id_map = {}
        for result in result_list:
            id = str(result[0]).strip()
            name = str(result[1]).strip()
            dw_id_name_map[id] = name
            dw_name_id_map[name] = id

        # 字段变更信息：字段增加、删除；字段类型变更、字段注释修改
        metahub_fields_map = {}
        for result in result_list:
            id = str(result[0]).strip()
            dw_fields = Meta_Dw_Fields()
            dw_fields.dw_id = id
            dw_fields.status = 1
            dw_fields_ddl = Meta_Dw_Fields_DDL(dw_fields)
            self.mysql_cursor.execute(dw_fields_ddl.get_select())
            metahub_fields_list = self.mysql_cursor.fetchall()

            for fields in metahub_fields_list:
                dw_id_tmp = str(fields[1]).strip()
                field_name_tmp = str(fields[2]).strip()
                field_type_tmp = str(fields[3]).strip()
                field_comment_tmp = str(fields[4]).strip()
                metahub_fields_map[dw_id_tmp+SEPARATE_SPECIAL+field_name_tmp] = field_type_tmp + SEPARATE_SPECIAL + field_comment_tmp

        # 获取表的列名信息
        sql = TBL_COLUMN_SQL % self.meta_db
        self.hive_meta_cursor.execute(sql)
        tbl_column_list = self.hive_meta_cursor.fetchall()

        base_info_list = []
        change_info_list = []
        for tbl_column in tbl_column_list:
            tbl_name = str(tbl_column[0]).strip()
            field_name = str(tbl_column[1]).strip()
            field_type = str(tbl_column[2]).strip()
            field_comment = str(tbl_column[3]).strip()

            base_info = Meta_Dw_Fields()
            base_info.dw_id = dw_name_id_map[tbl_name]
            base_info.field_name = field_name
            base_info.field_type = field_type
            base_info.field_comment = field_comment
            base_info.status = 1
            base_info.join_date = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
            base_info.creation = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
            base_info.modification = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
            base_info_list.append(base_info)

            k = base_info.dw_id + SEPARATE_SPECIAL + base_info.field_name
            if metahub_fields_map.get(k) != None:
                v = metahub_fields_map.get(k)
                if str(v).split(SEPARATE_SPECIAL)[0] != base_info.field_type:
                    change_info = Meta_Change_Info()
                    change_info.category = '数据仓库'
                    change_info.name = tbl_name
                    change_info.description = '%s 字段类型改变,由 [%s] 改为 [%s]' % (base_info.field_name, str(v).split(SEPARATE_SPECIAL)[0], base_info.field_type)
                    change_info.creation = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
                    change_info.modification = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
                    change_info_list.append(change_info)
                if str(v).split(SEPARATE_SPECIAL)[1].strip() != base_info.field_comment.strip():
                    change_info = Meta_Change_Info()
                    change_info.category = '数据仓库'
                    change_info.name = tbl_name
                    change_info.description = '%s 字段注释改变,由 [%s] 改为 [%s]' % (base_info.field_name, str(v).split(SEPARATE_SPECIAL)[1], base_info.field_comment)
                    change_info.creation = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
                    change_info.modification = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
                    change_info_list.append(change_info)
                metahub_fields_map[k] = field_type + SEPARATE_SPECIAL + field_comment + SEPARATE_SPECIAL + 'has'
            else:
                metahub_fields_map[k] = field_type + SEPARATE_SPECIAL + field_comment + SEPARATE_SPECIAL + 'add'

        # 记录哪些字段有更改
        for k,v in metahub_fields_map.items():
            if len(str(v).split(SEPARATE_SPECIAL)) == 3 and str(v).split(SEPARATE_SPECIAL)[2] == 'has':
                continue

            dw_id = str(k).split(SEPARATE_SPECIAL)[0]
            field_name = str(k).split(SEPARATE_SPECIAL)[1]
            field_type = str(v).split(SEPARATE_SPECIAL)[0]
            field_comment = str(v).split(SEPARATE_SPECIAL)[1]
            change_info = Meta_Change_Info()
            change_info.category = '数据仓库'
            change_info.name = dw_id_name_map[dw_id]
            change_info.creation = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
            change_info.modification = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())

            if len(str(v).split(SEPARATE_SPECIAL)) == 3 and str(v).split(SEPARATE_SPECIAL)[2] == 'add':
                change_info.description = '%s 字段增加' % field_name
            elif len(str(v).split(SEPARATE_SPECIAL)) == 2:
                change_info.description = '%s 字段删除' % field_name
                base_info = Meta_Dw_Fields()
                base_info.dw_id = dw_id
                base_info.field_name = field_name
                base_info.status = -1
                base_info.modification = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
                base_info_list.append(base_info)

            if change_info.description != '':
                change_info_list.append(change_info)

        self.load_into_mysql(base_info_list, Meta_Dw_Fields_DDL)
        self.load_into_mysql(change_info_list, Meta_Change_Info_DDL)

    def get_change_info(self):
        pass

    """ 获取hive表的血缘信息，对应metahub后端MySQL存储的 meta_dw_lineage
    主要功能和流程：
    1、从 meta_origin_lineage 获取到当前天数仓任务的血缘关系
    """
    def get_lineage_info(self):
        dw = Meta_Dw()
        dw.status = 1
        dw_ddl = Meta_Dw_DDL(dw)
        self.mysql_cursor.execute(dw_ddl.get_select())
        result_list = self.mysql_cursor.fetchall()
        dw_id_name_map = {}
        dw_name_id_map = {}
        for result in result_list:
            id = str(result[0]).strip()
            name = str(result[1]).strip()
            db_name = str(result[2]).strip()
            dw_id_name_map[id] = db_name + '.' + name
            dw_name_id_map[db_name + '.' + name] = id

        origin_lineage = Meta_Origin_Lineage()
        origin_lineage.creation = time.strftime('%Y-%m-%d', time.localtime())
        origin_lineage_dll = Meta_Origin_Lineage_DDL(origin_lineage)
        self.mysql_cursor.execute(origin_lineage_dll.get_select())
        origin_lineage_list = self.mysql_cursor.fetchall()

        lineage_info_list = []
        dw_id_exists_map = {}
        for line in origin_lineage_list:
            id = str(line[0]).strip()
            input_tables = str(line[1]).strip().replace('[', '').replace(']', '').split(',')
            output_tables = str(line[2]).strip().replace('[', '').replace(']', '').split(',')
            in_list = []; out_list = []
            for table in input_tables:
                if len(table.split('@')) < 2:
                    continue
                name = str(table.split('@')[0]) + '.' + str(table.split('@')[1])
                if name not in in_list:
                    in_list.append(name)
            for table in output_tables:
                if len(table.split('@')) < 2 or str(table.split('@')[0]).strip() != self.meta_db:
                    continue
                name = str(table.split('@')[0]) + '.' + str(table.split('@')[1])
                if name not in out_list:
                    out_list.append(name)
            if len(in_list) == 0 or len(out_list) == 0:
                continue
            target_table = out_list[0]  # 目标表只看一个，HQL任务里insert的表也只有一个
            target_id = dw_name_id_map.get(target_table)

            for source_table in in_list:
                source_id = dw_name_id_map.get(source_table)
                if source_id is None or target_id is None:
                    continue
                dw_id_exists_map[target_id + '@' + source_id] = 1
                dw_id_exists_map[source_id + '@' + target_id] = 1

                lineage_info = Meta_Dw_Lineage()
                lineage_info.dw_id = target_id
                lineage_info.relate_dw_id = source_id
                lineage_info.relate_status = 0  # relate_dw_id是上游依赖
                lineage_info.creation = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
                lineage_info.modification = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
                lineage_info_list.append(lineage_info)

                lineage_info = Meta_Dw_Lineage()
                lineage_info.dw_id = source_id
                lineage_info.relate_dw_id = target_id
                lineage_info.relate_status = 1  # relate_dw_id是下游依赖
                lineage_info.creation = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
                lineage_info.modification = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
                lineage_info_list.append(lineage_info)

        # 判断现有血缘关系是否有被解除
        # dw_lineage = Meta_Dw_Lineage()
        # dw_lineage_dll = Meta_Dw_Lineage_DLL(dw_lineage)
        # self.mysql_cursor.execute(dw_lineage_dll.get_select())
        # dw_lineage_list = self.mysql_cursor.fetchall()
        # for line in dw_lineage_list:
        #     dw_id = str(line[0]).strip()
        #     relate_dw_id = str(line[1]).strip()
        #     if dw_id_exists_map.get(dw_id + '@' + relate_dw_id) is None:
        #         lineage_info = Meta_Dw_Lineage()
        #         lineage_info.dw_id = dw_id
        #         lineage_info.relate_dw_id = relate_dw_id
        #         lineage_info.relate_status = -1
        #         lineage_info.modification = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
        #         lineage_info_list.append(lineage_info)

        self.load_into_mysql(lineage_info_list, Meta_Dw_Lineage_DDL)

    """ 导入MySQL """
    def load_into_mysql(self, load_info_list, Meta_DDL):
        # 有则更新，无则插入
        for base_info in load_info_list:
            meta_ddl = Meta_DDL(base_info)
            # print 'select: ', meta_ddl.get_select()
            # print 'update: ', meta_ddl.get_update()
            # print 'insert: ', meta_ddl.get_insert()
            if isinstance(base_info, Meta_Change_Info):
                self.logger.info(meta_ddl.get_insert())
                self.mysql_cursor.execute(meta_ddl.get_insert())
                continue

            self.logger.info(meta_ddl.get_select())
            self.mysql_cursor.execute(meta_ddl.get_select())
            result_list = self.mysql_cursor.fetchall()
            if len(result_list) < 1:
                self.logger.info(meta_ddl.get_insert())
                self.mysql_cursor.execute(meta_ddl.get_insert())
            else:
                self.logger.info(meta_ddl.get_update())
                self.mysql_cursor.execute(meta_ddl.get_update())
        self.mysql_conn.commit()

def start(env, hive_db=None, hive_table=None, log_path=None):
    metahub = MetaHub(param_env=env, param_db=hive_db, param_table=hive_table, param_log_path=log_path)

    metahub.get_base_info()
    metahub.get_fields_info()
    metahub.get_lineage_info()

    metahub.close()


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print ''
        exit(-1)
    env = str(sys.argv[1]).strip()

    hive_db = str(sys.argv[2]).strip()
    hive_table = None
    # if len(sys.argv) >= 4:
    #     hive_table = str(sys.argv[3]).strip()
    log_path = None
    if len(sys.argv) >= 4:
        log_path = str(sys.argv[3]).strip()

    start(env, hive_db, hive_table, log_path)
    pass


