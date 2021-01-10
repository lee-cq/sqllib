#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""
@File Name  : error
@Author     : LeeCQ
@Date-Time  : 2021/1/8 22:45
"""
try:
    from pymysql.err import (Error,
                             DataError,
                             DatabaseError,
                             OperationalError,
                             ProgrammingError,
                             InternalError,
                             InterfaceError,
                             IntegrityError,
                             NotSupportedError,
                             MySQLError,
                             )
    from sqlite3 import (Error,
                         DataError,
                         DatabaseError,
                         OperationalError,
                         ProgrammingError,
                         InternalError,
                         InterfaceError,
                         IntegrityError,
                         NotSupportedError,
                         )
except:
    pass


class SqllibError(Exception):
    """sqllib Error"""


class InsertZipError(SqllibError):
    """批量插入数据时，各数据的长度不一致"""


class SqlKeyNameError(SqllibError):
    pass


class SqlTableNameError(SqllibError):
    pass


class SqlWriteError(SqllibError):
    pass


class SqlModuleError(SqllibError):
    pass
