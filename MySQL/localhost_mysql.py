#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""
@File Name  : localhost_mysql.py
@Author     : LeeCQ
@Date-Time  : 2019/12/6 18:40

"""

import pymysql
from SQL.MySQL import MyMySqlAPI
from DBUtils.PooledDB import PooledDB


class LocalhostMySQL(MyMySqlAPI):
    """"""

    def __init__(self, user, passwd, db, **kwargs):
        super().__init__('localhost', 3306, user, passwd, db, 'utf8', **kwargs)
        self.table_name = 'insert_multiple_at_once'

    def test_create_table(self):
        _c = (f"CREATE TABLE IF NOT EXISTS `{self.table_name}` ( "
              f"a VARCHAR(10),"
              f"b VARCHAR(10)"
              f" ) ")
        super()._create_table(_c)

    def test_insert(self):
        """"""
        self.test_create_table()
        a = 'a1', 'a2'
        b = 'b1', 'b2'
        a = 'aa'
        return self._insert(self.table_name, a=a, b=b)


if __name__ == '__main__':
    # 忽略警告；
    import warnings
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        LocalhostMySQL('root', '123456', 'test').test_insert()
