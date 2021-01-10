#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""
@File Name  : base
@Author     : LeeCQ
@Date-Time  : 2021/1/8 20:46
"""
from abc import ABC, abstractmethod

__all__ = ['ABCBase', 'DBBase']


class DBBase(ABC):
    """基类"""

    # TABLE_PREFIX = None
    _sql = '__sql_connect()'

    @staticmethod
    def __sql_connect():
        return ''

    @abstractmethod
    def _write_db(self, command, args=None):
        """单条数据库的写入"""

    @abstractmethod
    def _write_affair(self, command, args):
        """事务"""

    def _read_db(self, command, args=None, result_type=None):
        """读数据库"""

    @abstractmethod
    def close(self):
        """关闭数据库连接句柄"""

    def set_prefix(self, prefix):
        """设置表前缀"""
        self.TABLE_PREFIX = prefix


class ABCBase(ABC):
    """实现的抽象基类"""

    @abstractmethod
    def show_dbs(self):
        """输出数据库"""

    @abstractmethod
    def show_tables(self):
        """展示数据表"""

    @abstractmethod
    def insert(self):
        """插入数据（增）"""

    @abstractmethod
    def delete(self):
        """删除数据行（删）"""

    @abstractmethod
    def update(self):
        """更新数据（改）"""

    @abstractmethod
    def select(self):
        """查询数据（查）"""

    @abstractmethod
    def drop_table(self):
        """删除数据表"""

    @abstractmethod
    def drop_db(self):
        """删除数据库"""

    @abstractmethod
    def create_table(self):
        """创建数据表"""

    @abstractmethod
    def alter_table(self):
        """修改数据表"""
