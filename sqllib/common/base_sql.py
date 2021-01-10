#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""
@File Name  : sql_base
@Author     : LeeCQ
@Date-Time  : 2021/1/8 20:46
"""
from abc import ABC, abstractmethod
from warnings import warn

from .base import DBBase
from .error import *

__all__ = ['BaseSQL', 'BaseSQLAPI']

from .common import sql_join
# from sqllib.SQLite.sqlite import SQLiteBase


class BaseSQL(DBBase, ABC):
    """关系型数据库的基类"""

    SQL_DB = None  # 数据库

    @abstractmethod
    def _tables_name(self):
        pass

    @abstractmethod
    def _columns_name(self, table):
        pass

    @staticmethod  # 插入或更新多条数据时，数据格式转换
    def zip_data_for_insert(values):
        """一次插入数据库多条数据时，打包相应的数据。"""
        for x in values:
            if not isinstance(x, (tuple, list)):
                raise InsertZipError(f"INSERT多条数据时，出现非列表列！确保数据都是list或者tuple。\n错误的值是：{x}")

            if not len(values[0]) == len(x):
                raise InsertZipError(f'INSERT多条数据时，元组长度不整齐！请确保所有列的长度一致！\n'
                                     f'[0号]{len(values[0])}-[{values.index(x)}号]{len(x)}')

        return tuple([v for v in zip(*values)])  # important *

    # 判断表、键值的存在性
    def key_and_table_is_exists(self, table, key, *args, **kwargs):
        """ 判断 key & table 是否存在

        :param table: 前缀 + 表单名
        :param key: 键名
        :param args: 键名, 多个键名
        :param kwargs: 键名=键值；
        :return: 0 存在
        """
        tables_name = [i.upper() for i in self._tables_name()]
        if table.upper() not in tables_name:
            raise SqlTableNameError(f"{table} NOT in This Database: {self.SQL_DB};\n"
                                    f"(ALL Tables {self._tables_name()}")

        cols = [i.decode() if isinstance(i, bytes) else i for i in self._columns_name(table)]
        not_in_table_keys = [k.upper() for k, v in kwargs.items() if k not in cols]
        not_in_table_keys += [k.upper() for k in args if k not in cols]
        if key.upper() not in cols and not_in_table_keys:
            raise SqlKeyNameError(f'The key {key.upper()} NOT in this Table: {table};\n'
                                  f'(ALL Columns {cols})'
                                  )
        return 0

    @abstractmethod
    def _create_table(self, cmd, table_name, exists_ok, table_args, *args):
        pass

    @abstractmethod
    def _insert(self, table, ignore, **kwargs):
        pass

    @abstractmethod
    def _select(self, table, cols, *args, result_type=None, **kwargs):
        pass

    @abstractmethod
    def _update(self, table, where_key, where_value, **kwargs):
        pass

    @abstractmethod
    def _drop(self, option, name):
        pass

    @abstractmethod
    def _delete(self, table, where_key, where_value, **kwargs):
        pass

    @abstractmethod
    def _alter(self, table, command: str):
        pass


class BaseSQLAPI(BaseSQL, ABC):

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def __enter__(self):
        return self

    def create_table(self, cmd: (str, tuple), table_name, exists_ok=False, table_args='', *args):
        """ 创建一个数据表：

        模板：
            _c = (f"CREATE TABLE IF NOT EXISTS `{table_name}` ( "
                    f"a VARCHAR(10),"
                    f"b VARCHAR(10)"
                    f" ) ")
        这样就可以创建一个名为'table_name' 的数据表；
        有2个键：a, b 都是变长字符串(10)

        :param table_name:
        :param exists_ok: 为False，创建的表存在时，抛出异常
        :param cmd 字段字符串
        :param table_args: 创建表时附加的额外的参数 CREATE TABLE `NAME` ( params ) >>table_args<< ;
        :param args: 无，不要填
        :return: 0 成功
        """
        if isinstance(cmd, (tuple, list)):
            cmd = sql_join(cmd)[0]
        return self._create_table(cmd, table_name, exists_ok=exists_ok, table_args=table_args, *args)

    def insert(self, table, ignore=None, **kwargs):
        """ 向数据库插入内容。

        允许一次插入多条数据，以 key=(tuple)的形式；
            但是要注意，所有字段的元组长度需要相等；否组报错。

        :param table: 表名；
        :param ignore: 忽视重复
        :param kwargs: 字段名 = 值；字段名一定要存在与表中， 否则报错；
        :return: 0 成功 否则 报错
        """
        return self._insert(table, ignore=ignore, **kwargs)

    def select(self, table, cols, *args, result_type=None, **kwargs):
        """ 从数据库中查找数据；

            column_name 可以设置别名；

            注意： `就不再支持 * `
            注意：column_name, table, key 可以用 ` ` 包裹， value 一定不能用，
                  如果有需要，value 用 ' ' 。
        :param table:
        :param cols: 传参时自行使用 `` , 尤其是数字开头的参数
        :param result_type: 返回结果集：{dict, None, tuple, 'SSCursor', 'SSDictCursor'}
        :param kwargs: {'WHERE', 'LIMIT', 'OFFSET', 'ORDER'} 全大写
                        WHERE 查询字符串 如 KEY=VALUE
                        LIMIT 2 OFFSET 2  通常连在一起使用
                        ORDER 排序  COL_NAME  [ASC | DESC]
                      特殊键：result_type = {dict, None, tuple, 'SSCursor', 'SSDictCursor'}
        :return 结果集 通过键 - result_type 来确定 -
        """

        _cols = []
        if isinstance(cols, str):
            _cols.append(cols)
        if isinstance(cols, (list, tuple)):
            [_cols.append(_) for _ in cols]
        if args:
            [_cols.append(_) for _ in args]
        return self._select(table, _cols, result_type=result_type, **kwargs)

    def select_new(self, table, columns_name: tuple or list, result_type=None, **kwargs):
        """ SELECT的另一种传参方式：
                要求所有的查询字段放在一个列表中传入。

        :param table:
        :param columns_name:
        :type columns_name: tuple or list
        :param result_type: 返回结果集类型：{dict, None, tuple, 'SSCursor', 'SSDictCursor'}
        :return: 结果集 通过键 - result_type 来确定
        """
        warn('不建议使用此函数！')
        return self.select(table, columns_name, result_type=result_type, **kwargs)

    def update(self, table, where_key, where_value, **kwargs):
        """ 更新数据库数据：

        :rtype: int
        :param table: 表名
        :param where_key: 通过字段查找行的键
        :param where_value: 其值
        :param kwargs: 需要更新的键值对
        :return: 0 or Error
        """
        return self._update(table, where_key, where_value, **kwargs)

    def drop_table(self, name):
        """用来删除一张表

        :param name: table name
        :return: 0 or Error
        """
        return self._drop('TABLE', name)

    def drop_db(self, name):
        """用来删除一个数据库

        :param name:
        :return:
        """
        return self._drop('DB', name)

    def delete(self, table, where_key, where_value, **kwargs):
        """ 用来删除数据表中的一行数据；

        :param table: 表名
        :param where_key: 查找到键
        :param where_value: 查找的值 可以使用表达式
        :param kwargs: 补充查找的键值对；
        :return: 0 or Error
        """
        return self._delete(table, where_key, where_value, **kwargs)

    def alter_table(self, table, command: str):
        """向已有表中插入键

        语法：
            ALTER TABLE `{self.TABLE_PREFIX}{table_name}` ADD COLUMN
                (`device_ip` VARCHAR(32) DEFAULT NULL COMMENT '设备IP',
                 `device_name` VARCHAR(128) DEFAULT NULL COMMENT '设备名称',
                 );
        :return:
        """
        return self._alter(table, command)
