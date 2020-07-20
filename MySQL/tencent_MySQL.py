#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""
@File Name  : tencent_MySQL.py
@Author     : LeeCQ
@Date-Time  : 2019/12/5 10:05

"""

from MySQL import MyMySqlAPI


class TencentMySQL(MyMySqlAPI):
    """
    腾讯Mysql的入口；
    """
    def __init__(self, user, passwd, db, **kwargs):

        self.SQL_HOST = 't.sql.leecq.xyz'  # 主机
        self.SQL_PORT = 10080  # 端口
        self.SQL_USER = user  # 用户
        self.SQL_PASSWD = passwd  # 密码
        self.SQL_DB = db  # 数据库
        self.SQL_CHARSET = 'utf8'  # 编码
        super().__init__(host=self.SQL_HOST,
                         port=self.SQL_PORT,
                         user=self.SQL_USER,
                         passwd=self.SQL_PASSWD,
                         db=self.SQL_DB,
                         charset=self.SQL_CHARSET,
                         **kwargs
                         )


if __name__ == '__main__':
    a = TencentMySQL('Mzitu', 'Mzitu123456', 'Mzitu')
    res = a.select('Mzitu_no_json', '*', LIMIT='10,10')
    print(res)

