#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""
@Time   : 2019/11/26 12:54
@Author : LeeCQ
@File Name: __init__.py

更多操作详见 下方import 内容。
内容：
    * v1:
        1. 构建 class:MyMySQL 框架
            以 MyMySQL.__write_db(), MyMySQL.__write_rows(), MyMySQL.__read_db()为基础访问SQL。
            创建_select, _insert, _update, _drop, _delete为基础的接口访问。
        2. 构建访问控制 & 安全 相关的语句。
            __key_and_table_is_exists
        3. 优化流程控制。
        4. 优化数据库访问。
    * v2: -2019/12/18
        1. 新增 DBUtils.PooledDB 模块：连接池
            1.1. 新增MyMySQL.pooled_sql()模块，以启用连接池
            1.2. 修改MyMySQL.__write_db(), MyMySQL.__write_rows(), MyMySQL.__read_db():
                    当他的子类或者实例调用 -> MyMySQL.pooled_sql() <- 方法时，以开启连接池；
                    if self.pooled_sql is not None:
                        __sql = self.pooled_sql.connection()
                    else:
                        __sql = self._sql
        2. 微调 MyMySQL._create_table()方法：
            源：( with self._sql.cursor() as cur: \\ cur.execute(command, args) \\self._sql.commit() \\ return 0) ==>
            修改为：( return self.__write_db(command, args) )
            有点：便于代码的重用性；

    * v3: -2020/01/11
        1. 新增 def _alter_table():  --> 向已有数据表中添加 键
        2. 更新详细了注释
        3. MyMySQL - 增加了对 pymysql.connect() - 所需参数的详细注解 -- 1/12

    * v4: -2020/02/29
        1. 修改文件结构

"""

from .mysql import MyMySqlAPI, MySqlAPI, LocalhostMySQL


class Test(MyMySqlAPI):
    """测试的mysql对象封装"""

    def __init__(self, host='localhost', port=3306,
                 user='test', passwd='test123456', db='test', charset='gb18030',
                 **kwargs):
        super().__init__(host, port, user, passwd, db, charset, **kwargs)
