#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""
@Time   : 2019/11/26 12:54
@Author : LeeCQ
@File Name: __init__.py


"""

from .mysqlbase import MyMySqlAPI, MySqlAPI


class Test(MyMySqlAPI):
    """测试的mysql对象封装"""

    def __init__(self, host='localhost', port=3306,
                 user='test', passwd='test123456', db='test', charset='gb18030',
                 **kwargs):
        super().__init__(host, port, user, passwd, db, charset, **kwargs)
