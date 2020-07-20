#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""
@File Name  : sql_error.py
@Author     : LeeCQ
@Date-Time  : 2019/12/5 11:39

"""


class MyMySqlError(Exception):
    pass


class MyMySqlKeyNameError(MyMySqlError):
    pass


class MyMySqlTableNameError(MyMySqlError):
    pass


class MyMySqlWriteError(MyMySqlError):
    pass


class MyMySqlInsertZipError(MyMySqlError):
    """"""
    pass

