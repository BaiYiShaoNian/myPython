# coding:utf8
"""
file name: meta_schema.py
date: 2020/3/18
author: huangxuanfeng
"""
import sys, os
import time, datetime

reload(sys)
sys.setdefaultencoding('utf-8')


class Meta_Dw:
    name = ''
    db_name = ''
    status = ''
    owner = ''
    description = ''
    pt_name = ''
    earliest_pt = ''
    latest_pt = ''
    product_style = ''
    product_time = ''
    workflow = ''
    auth_select = ''
    creation = ''
    modification = ''

class Meta_Dw_DDL:
    meta_dw = Meta_Dw

    def __init__(self, meta_dw):
        self.meta_dw = meta_dw

    def get_select(self):
        sql = "select id,name,db_name,status,owner,description,pt_name,earliest_pt,latest_pt,product_style,product_time," \
               "workflow,auth_select,creation,modification from meta_dw "
        where_sql = " and status > 0"
        if self.meta_dw.name != '':
            where_sql += " and name='%s'" % self.meta_dw.name
        if self.meta_dw.db_name != '':
            where_sql += " and db_name='%s'" % self.meta_dw.db_name
        return sql + where_sql.replace(' and ', ' where ', 1)

    def get_update(self):
        sql = "update meta_dw set modification='%s'" % self.meta_dw.modification
        if self.meta_dw.status != '':
            sql += ",status='%s'" % self.meta_dw.status
        if self.meta_dw.pt_name != '':
            sql += ",pt_name='%s'" % self.meta_dw.pt_name
        if self.meta_dw.earliest_pt != '':
            sql += ",earliest_pt='%s'" % self.meta_dw.earliest_pt
        if self.meta_dw.latest_pt != '':
            sql += ",latest_pt='%s'" % self.meta_dw.latest_pt
        if self.meta_dw.auth_select != '':
            sql += ",auth_select='%s'" % self.meta_dw.auth_select
        sql += " where status > 0 and name = '%s' " % self.meta_dw.name

        return sql

    def get_insert(self):
        sql = "insert into meta_dw (name,db_name,status,owner,description,pt_name,earliest_pt,latest_pt,product_style,product_time,workflow,auth_select,creation,modification) " \
              "values ('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')" \
              % (self.meta_dw.name, self.meta_dw.db_name, self.meta_dw.status, self.meta_dw.owner, self.meta_dw.description,
                 self.meta_dw.pt_name, self.meta_dw.earliest_pt, self.meta_dw.latest_pt, self.meta_dw.product_style, self.meta_dw.product_time,
                 self.meta_dw.workflow, self.meta_dw.auth_select, self.meta_dw.creation, self.meta_dw.modification)
        return sql

class Meta_Dw_Fields:
    dw_id = ''
    field_name = ''
    field_type = ''
    field_comment = ''
    status = ''
    join_date = ''
    creation = ''
    modification = ''

class Meta_Dw_Fields_DDL:
    meta_dw_fields = Meta_Dw_Fields

    def __init__(self, meta_dw_fields):
        self.meta_dw_fields = meta_dw_fields

    def get_select(self):
        sql = "select id,dw_id,field_name,field_type,field_comment,status,join_date,creation,modification " \
              "from meta_dw_fields "
        where_sql = " and status > 0"
        if self.meta_dw_fields.dw_id != '':
            where_sql += " and dw_id='%s'" % self.meta_dw_fields.dw_id
        if self.meta_dw_fields.field_name != '':
            where_sql += " and field_name='%s'" % self.meta_dw_fields.field_name
        return sql + where_sql.replace(' and ', ' where ', 1)

    def get_update(self):
        sql = "update meta_dw_fields set modification='%s'" % self.meta_dw_fields.modification
        if self.meta_dw_fields.field_type != '':
            sql += " ,field_type='%s'" % self.meta_dw_fields.field_type
        #if self.meta_dw_fields.field_comment != '':
        sql += " ,field_comment='%s'" % self.meta_dw_fields.field_comment
        if self.meta_dw_fields.status != '':
            sql += " ,status='%s'" % self.meta_dw_fields.status
        sql += " where status > 0 and dw_id='%s' and field_name='%s'" % (self.meta_dw_fields.dw_id, self.meta_dw_fields.field_name)
        return sql

    def get_insert(self):
        sql = "insert into meta_dw_fields (dw_id,field_name,field_type,field_comment,status,join_date,creation,modification) " \
              "values ('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s')" % \
              (self.meta_dw_fields.dw_id, self.meta_dw_fields.field_name, self.meta_dw_fields.field_type, self.meta_dw_fields.field_comment,
               self.meta_dw_fields.status, self.meta_dw_fields.join_date, self.meta_dw_fields.creation, self.meta_dw_fields.modification)
        return sql

class Meta_Change_Info:
    category = ''
    name = ''
    description = ''
    creation = ''
    modification = ''

class Meta_Change_Info_DDL:
    meta_change_info = Meta_Change_Info

    def __init__(self, meta_change_info):
        self.meta_change_info = meta_change_info

    def get_select(self):
        sql = "select id,category,name,description,creation,modification from meta_change_info"
        where_sql = ""
        if self.meta_change_info.category != '':
            where_sql += " and category='%s' " % self.meta_change_info.category
        if self.meta_change_info.name != '':
            where_sql += " and name='%s' " % self.meta_change_info.name
        if self.meta_change_info.description != '':
            where_sql += " and description='%s' " % self.meta_change_info.description
        return sql + where_sql.replace(' and ', ' where ', 1)

    def get_update(self):
        sql = "update meta_change_info set modification='%s'" % self.meta_change_info.modification
        if self.meta_change_info.category != '':
            sql += " ,category='%s' " % self.meta_change_info.category
        if self.meta_change_info.description != '':
            sql += " ,description='%s' " % self.meta_change_info.description
        sql += " where name='%s' " % self.meta_change_info.name
        return sql

    def get_insert(self):
        sql = "insert into meta_change_info (category,name,description,creation,modification) " \
              "values ('%s', '%s', '%s', '%s', '%s')" % \
                (self.meta_change_info.category, self.meta_change_info.name, self.meta_change_info.description,
                 self.meta_change_info.creation, self.meta_change_info.modification)
        return sql

class Meta_Dw_Lineage:
    dw_id = ''
    relate_dw_id = ''
    relate_status = ''
    creation = ''
    modification = ''

class Meta_Dw_Lineage_DDL:
    meta_dw_lineage = Meta_Dw_Lineage

    def __init__(self, meta_dw_lineage):
        self.meta_dw_lineage = meta_dw_lineage

    def get_select(self):
        sql = "select dw_id,relate_dw_id,relate_status,creation,modification from meta_dw_lineage "
        where_sql = "and relate_status > -1"
        if self.meta_dw_lineage.dw_id != '':
            where_sql += " and dw_id='%s'" % self.meta_dw_lineage.dw_id
        if self.meta_dw_lineage.relate_dw_id != '':
            where_sql += " and relate_dw_id='%s'" % self.meta_dw_lineage.relate_dw_id
        # if self.meta_dw_lineage.relate_status != '':
        #     where_sql += " and relate_status='%s'" % self.meta_dw_lineage.relate_status
        return sql + where_sql.replace('and', ' where ', 1)

    def get_update(self):
        sql = "update meta_dw_lineage set modification='%s'" % self.meta_dw_lineage.modification
        if self.meta_dw_lineage.relate_status != '':
            sql += " ,relate_status='%s'" % self.meta_dw_lineage.relate_status
        sql += " where dw_id='%s' and relate_dw_id='%s'" % (self.meta_dw_lineage.dw_id, self.meta_dw_lineage.relate_dw_id)
        return sql

    def get_insert(self):
        sql = "insert into meta_dw_lineage (dw_id, relate_dw_id, relate_status, creation, modification) " \
              "values ('%s', '%s', '%s', '%s', '%s')" % \
              (self.meta_dw_lineage.dw_id, self.meta_dw_lineage.relate_dw_id, self.meta_dw_lineage.relate_status,
               self.meta_dw_lineage.creation, self.meta_dw_lineage.modification)
        return sql


class Meta_Origin_Lineage:
    input_table = ''
    output_table = ''
    creation = ''

class Meta_Origin_Lineage_DDL:
    meta_origin_lineage = Meta_Origin_Lineage

    def __init__(self, meta_origin_lineage):
        self.meta_origin_lineage = meta_origin_lineage

    def get_select(self):
        sql = "select id,input_table,output_table,creation from meta_origin_lineage"
        where_sql = ''
        if self.meta_origin_lineage.creation != '':
            where_sql += " and date(creation)='%s'" % self.meta_origin_lineage.creation
        return sql + where_sql.replace('and', ' where ', 1)

    def get_update(self):
        pass

    def get_insert(self):
        pass
