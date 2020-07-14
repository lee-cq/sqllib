#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""
@Time   : 2019/11/26 12:54
@Author : LeeCQ
@File Name: __init__v2.py

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
                    当他的子类或者实例调用 >>> MyMySQL.pooled_sql() <<< 方法时，以开启连接池；
                    if self.pooled_sql is not None:
                        __sql = self.pooled_sql.connection()
                    else:
                        __sql = self._sql
        2. 微调 MyMySQL._create_table()方法：
            源：( with self._sql.cursor() as cur: \\ cur.execute(command, args) \\self._sql.commit() \\ return 0) ==>
            修改为：( return self.__write_db(command, args) )
            有点：便于代码的重用性；

"""

import pymysql
import sys
from warnings import filterwarnings
from DBUtils.PooledDB import PooledDB
from SQL.MySQL.sql_error import *


__all__ = ['MyMySqlAPI', 'TencentMySQL', 'LocalhostMySQL']


class MyMySQL:
    """MySQL 操作的模板：

    这个类包含了最基本的MySQL数据库操作，SELECT, INSERT, UPDATE, DELETE

    主要的接口方法：
    :method _select(self, table, column_name, *args, **kwargs):
            从数据库查询数据： - 表名，查询列，拓展查询列（可以使用通配符'*'）
                             - kwargs: {WHERE, LIMIT, OFFSET}  LIMIT 有一种新用法 - limit 偏移量，限制值
        :return 查找的数据

    :method _insert(self, table, **kwargs)
            向数据库插入新数据 - kwargs: 需要插入的数据的 键值对；
        :return 0 or -1

    :method _update(self, table, where_key, where_value, **kwargs)
            向数据库更新字段的值，  - where代表着查询的行  -- 如果没有将会更新所有行。
                                 - kwargs 插入的键值对
        :return 0 or -1

    :method _drop(self, option, name)：
            删除数据表或者数据库      - option = table or database
                                   - name 对应的名称
        :return 0 or -1

    :method _delete(self, table, where_key, where_value, **kwargs):

    :method write_db(self, command):
            没有模板时使用     - command: 数据库查询字符串。
        :return 0 or -1

    :method read_db(self, command):
                没有模板时使用     - command: 数据库查询字符串。
        :return 查询结果；


    :param str host:    链接的数据库主机；
    :param int port:    数据库服务器端口
    :param str user:    数据库用户名
    :param str passwd:  数据库密码
    :param str db:      数据库的DataBase
    :param str charset: 数据库的字符集
    :param str prefix:  表前缀
    """

    def __init__(self, host, port, user, passwd, db, charset, use_unicode=None, **kwargs):
        self.SQL_HOST = host  # 主机
        self.SQL_PORT = port  # 端口
        self.SQL_USER = user  # 用户
        self.SQL_PASSWD = passwd  # 密码
        self.SQL_DB = db  # 数据库
        self.SQL_CHARSET = charset  # 编码
        self.use_unicode = use_unicode
        # 表前缀
        self.TABLE_PREFIX = '' if 'prefix' not in kwargs.keys() else kwargs['prefix']
        self._sql = pymysql.connect(host=self.SQL_HOST,
                                    port=self.SQL_PORT,
                                    user=self.SQL_USER,
                                    password=self.SQL_PASSWD,  # 可以用 passwd为别名
                                    database=self.SQL_DB,  # 可以用 db    为别名；
                                    charset=self.SQL_CHARSET,
                                    use_unicode=use_unicode
                                    )
        self.pooled_sql = None

    def set_use_db(self, db_name):
        return self._sql.select_db(db_name)

    def set_charset(self, charset):
        return self._sql.set_charset(charset)

    # 建立连接池
    def pooling_sql(self, creator=pymysql, min_cached=0, max_cached=0,
                    max_connections=0, blocking=True,
                    max_usage=None, set_session=None, reset=True,
                    failures=None, ping=1,
                    **kwargs):
        """ 连接池建立

        :param creator: 数据库连接池返回的模块；
        :param min_cached: 初始化时，池中最小链接数；
        :param max_cached: 链接池中最多闲置的链接，0和None不限制；
        :param max_connections: 池中最大链接数；
        :param blocking: 链接数用尽时，是否阻塞等待链接 True 等待 -- False 不等待 & 报错
        :param max_usage: 一个链接最多被重复使用的次数，None表示无限制
        :param set_session: # 开始会话前执行的命令列表。如：["set datestyle to ...", "set time zone ..."]
        :param reset: 当连接返回到池中时，应该如何重置连接
        :param failures:
        :param ping: ping MySQL服务端，检查是否服务可用，
        :param kwargs: {host=, poet=, user=, passwd=, database=, charset=}
        """
        self.pooled_sql = PooledDB(creator, mincached=min_cached, maxcached=max_cached,
                                   maxconnections=max_connections, blocking=blocking,
                                   maxusage=max_usage, setsession=set_session, reset=reset,
                                   failures=failures, ping=ping,
                                   host=self.SQL_HOST if 'host' not in kwargs.keys() else kwargs['host'],
                                   port=self.SQL_PORT if 'post' not in kwargs.keys() else kwargs['port'],
                                   user=self.SQL_USER if 'user' not in kwargs.keys() else kwargs['user'],
                                   password=self.SQL_PASSWD if 'passwd' not in kwargs.keys() else kwargs['passwd'],
                                   database=self.SQL_DB if 'db' not in kwargs.keys() else kwargs['db'],
                                   charset=self.SQL_CHARSET if 'charset' not in kwargs.keys() else kwargs['charset']
                                   )

    def set_prefix(self, prefix):
        self.TABLE_PREFIX = prefix

    def __write_db(self, command, args=None):
        """执行数据库写入操作

        :type args: str, list or tuple
        """
        if self.pooled_sql is not None:
            __sql = self.pooled_sql.connection()
        else:
            __sql = self._sql

        cur = __sql.cursor()  # 使用cursor()方法获取操作游标
        try:
            cur.execute(command, args)
            __sql.commit()  # 提交数据库
            return 0
        except:
            __sql.rollback()
            sys.exc_info()
            raise MyMySqlWriteError(f'操作数据库时出现问题，数据库已回滚至操作前——\n{sys.exc_info()}')
        finally:
            cur.close()

    def __write_rows(self, command, args):
        """向数据库写入多行"""
        if self.pooled_sql is not None:
            __sql = self.pooled_sql.connection()
        else:
            __sql = self._sql

        try:
            with __sql.cursor() as cur:  # with 语句自动关闭游标
                cur.executemany(command, args)
                __sql.commit()
            return 0
        except:
            __sql.rollback()
            sys.exc_info()
            raise MyMySqlWriteError("__write_rows() 操作数据库出错，已回滚 \n" + sys.exc_info())

    def __read_db(self, command, args=None, result_type=None):
        """执行数据库读取数据， 返回结果

        :param result_type: 返回的结果集类型{dict, None, tuple, 'SSCursor', 'SSDictCursor'}
        """
        if self.pooled_sql is not None:
            __sql = self.pooled_sql.connection()
        else:
            __sql = self._sql

        ret_ = {dict: pymysql.cursors.DictCursor,
                None: pymysql.cursors.Cursor,
                tuple: pymysql.cursors.Cursor,
                'SSCursor': pymysql.cursors.SSCursor,
                'SSDictCursor': pymysql.cursors.SSDictCursor
                }
        cur = __sql.cursor(ret_[result_type])
        cur.execute(command, args)
        results = cur.fetchall()
        cur.close()
        return results

    def __columns(self, table):
        """返回table中列（字段）的所有信息"""
        return self.__read_db(f'show columns from `{table}`')

    def __columns_name(self, table):
        """返回 table 中的 列名在一个列表中"""
        return [_c[0] for _c in self.__columns(table)]

    def __tables_name(self):
        """由于链接时已经指定数据库，无需再次指定。返回数据库中所有表的名字。"""
        return [str(_c[0]) for _c in self.__read_db("show tables")]

    def __key_and_table_is_exists(self, table, key, *args, **kwargs):
        """ 判断 key & table 是否存在

        :param table: 前缀 + 表单名
        :param key, args: 键名
        :param kwargs: 键名=键值；
        :return: 0 存在
        """
        if table not in self.__tables_name():
            raise MyMySqlTableNameError(f"{table} NOT in This Database: {self.SQL_DB};\n"
                                        f"(ALL Tables {self.__tables_name()}")
        cols = self.__columns_name(table)

        not_in_table_keys = [k for k, v in kwargs.items() if k not in cols]
        not_in_table_keys += [k for k in args if k not in cols]
        if key not in cols and not_in_table_keys:
            raise MyMySqlKeyNameError(f'The key {not_in_table_keys} NOT in this Table: {table};'
                                      f'(ALL Columns {cols})'
                                      )
        return 0

    @staticmethod
    def __insert_zip(values):
        """一次向插入数据库多条数据时，打包相应的数据。"""
        for x in values:
            if not isinstance(x, (tuple, list)):
                raise MyMySqlInsertZipError(f"INSERT多条数据时，出现非列表列！确保数据都是list或者tuple。\n错误的值是：{x}")

            if not len(values[0]) == len(x):
                raise MyMySqlInsertZipError(f'INSERT多条数据时，元组长度不整齐！请确保所有列的长度一致！\n'
                                            f'[0号]{len(values[0])}-[{values.index(x)}号]{len(x)}')

        return tuple([v for v in zip(*values)])  # important *

    def _create_table(self, command, table_name=None, table_args='', *args):
        """创建一个数据库，没有构造指令；等同于self.write_db()"""
        if not table_name:
            _c = command
        else:
            _c = (f"CREATE TABLE IF NOT EXISTS `{self.TABLE_PREFIX}{table_name}` ("
                  + command +
                  ") " + table_args)
        return self.__write_db(_c, args)

    def _insert(self, table, ignore=None, **kwargs):
        """ 向数据库插入内容。

        :param table: 表名；
        :param kwargs: 字段名 = 值；
        :return:
        """
        ignore_ = 'IGNORE' if ignore else ''
        _c = (f"INSERT {ignore_} INTO `{self.TABLE_PREFIX}{table}`  "
              "( " +
              ', '.join([" `" + _k + "` " for _k in kwargs.keys()]) +
              " ) "  # 这一行放在后面会发生，乱版；
              " VALUES "
              " ( " + ', '.join([" %s " for _k in kwargs.values()]) + " ) ; "  # 添加值
              )
        # print(self._insert.__name__, _c)
        if not isinstance(list(kwargs.values())[0], str):
            arg = self.__insert_zip(tuple(kwargs.values()))
            return self.__write_rows(_c, arg)
        else:
            for x in kwargs.values():
                if not isinstance(x, (int, str)):
                    raise MyMySqlInsertZipError("INSERT一条数据时，出现列表列或元组！确保数据统一")
            return self.__write_db(_c, list(kwargs.values()))  # 提交

    def _select(self, table, columns_name: tuple and list, result_type=None, **kwargs):
        """ select的应用。

            ·· `就不再支持 * `
            ·· column_name, table, key 可以用 ` ` 包裹， value 一定不能用， value 用 ' ' 。
        :param table:
        :param columns_name: 传参时自行使用 `` , 尤其是数字开头的参数
        :param _args: column_name 的拓展
        :param kwargs: {'WHERE', 'LIMIT', 'OFFSET', ORDER} 全大写
                        特殊键：result_type = {dict, None, tuple, 'SSCursor', 'SSDictCursor'}
        :return 结果集
        """

        command = f"SELECT  "
        command += ' , '.join(columns_name) + " "
        command += f'FROM `{self.TABLE_PREFIX}{table}` '
        for key, value in kwargs.items():
            key = key.upper()
            if key in ['WHERE', 'LIMIT', 'OFFSET']:
                command += f' {key}  {value}'
            if key == 'ORDER':
                command += f' {key} BY {value}'
        return self.__read_db(command, result_type=result_type)

    def _update(self, table, where_key, where_value, **kwargs):
        """ 更新数据库 --2019/12/15

        :param table: 数据表名字
        :param where_key: where的键
        :param where_value: where的值
        :param kwargs: 更新的键 = 更新的值， 注意大小写，数字键要加 - ``
        :return: 0 成功。
        """
        self.__key_and_table_is_exists(self.TABLE_PREFIX + table, where_key, **kwargs)  # 判断 表 & 键 的存在性！

        command = f"UPDATE `{self.TABLE_PREFIX}{table}` SET  "
        command += ' , '.join([f" `{k}`=%({k})s  " for k, v in kwargs.items()])  # 构造更新内容
        command += r" WHERE `{}`='{}' ;".format(where_key, where_value)  # 构造WHERE语句

        return self.__write_db(command, kwargs)  # 执行SQL语句

    def _drop(self, option, name):
        """ 删除数据库内容：

        :param option: (TABLE or DATABASE)
        :param name:
        :return: 0 成功
        """
        if option.upper() == 'TABLE':
            command = f'DROP  {option}  `{self.TABLE_PREFIX}{name}`'
        else:
            command = f'DROP  {option}  `{name}`'
        return self.__write_db(command)

    def _delete(self, table, where_key, where_value, **kwargs):
        """删除数据表中的某一行数据，
        :param table:
        :param where_key: 最好是数据库的主键或唯一的键。如果数据库没有，则最好组合where，以保证删除 - 唯一行。
        :param where_value:
        :param kwargs: 键名=键值；where——key的补充。
        """
        self.__key_and_table_is_exists(self.TABLE_PREFIX + table, where_key, **kwargs)

        command = f"DELETE FROM `{self.TABLE_PREFIX}{table}` WHERE {where_key}='{where_value}'  "
        for k, v in kwargs.items():
            command += f"{k}='{v}'"

        return self.__write_db(command)

    def write_db(self, command, *args):
        return self.__write_db(command, *args)

    def read_db(self, command, args=None, result_type=None):
        return self.__read_db(command, args, result_type)

    def test_show(self):
        return self.__read_db('show tables')


class MyMySqlAPI(MyMySQL):
    def __init__(self, host, port, user, passwd, db, charset, warning=True, **kwargs):
        super().__init__(host, port, user, passwd, db, charset, **kwargs)
        if not warning:
            filterwarnings("ignore", category=pymysql.Warning)

    def create_table(self, command, table_name=None, table_args='', *args):
        """ 创建一个数据表：

        模板：
            _c = (f"CREATE TABLE IF NOT EXISTS `{table_name}` ( "
                    f"a VARCHAR(10),"
                    f"b VARCHAR(10)"
                    f" ) ")
        这样就可以创建一个名为'table__name' 的数据表；
        有2个键：a, b 都是变长字符串(10)

        :param command: 字段字符串
        :param table_name:
        :param table_args:
        :param args: 无
        :return: 0 成功
        """
        return self._create_table(command, table_name, table_args, *args)

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

    def select(self, table, column_name, *args, result_type=None, **kwargs):
        """ 从数据库中查找数据；

            column_name 可以设置别名；

            注意： `就不再支持 * `
            注意：column_name, table, key 可以用 ` ` 包裹， value 一定不能用，
                  如果有需要，value 用 ' ' 。
        :param table:
        :param column_name: 传参时自行使用 `` , 尤其是数字开头的参数
        :param result_type: 返回结果集：{dict, None, tuple, 'SSCursor', 'SSDictCursor'}
        :param kwargs: {'WHERE', 'LIMIT', 'OFFSET', 'ORDER'} 全大写
                      特殊键：result_type = {dict, None, tuple, 'SSCursor', 'SSDictCursor'}
        :return 结果集 通过键 - result_type 来确定 -
        """
        columns_name = [column_name] + list(args)
        return self._select(table, columns_name, result_type=result_type, **kwargs)

    def select_new(self, table, columns_name: tuple or list, result_type=None, **kwargs):
        """ SELECT的另一种传参方式：
                要求所有的查询字段放在一个列表中传入。

        :param table:
        :param columns_name:
        :type columns_name: tuple or list
        :param result_type: 返回结果集类型：{dict, None, tuple, 'SSCursor', 'SSDictCursor'}
        :return: 结果集 通过键 - result_type 来确定 -
        """
        return self._select(table, columns_name, result_type=result_type, **kwargs)

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

    def drop(self, option, name):
        """用来删除一张表或者一个数据库;

        :param option: 选项 - table or database
        :param name: table name or database name
        :return: 0 or Error
        """
        return self._drop(option, name)

    def delete(self, table, where_key, where_value, **kwargs):
        """ 用来删除数据表中的一行数据；

        :param table: 表名
        :param where_key: 查找到键
        :param where_value: 查找的值 可以使用表达式
        :param kwargs: 补充查找的键值对；
        :return: 0 or Error
        """
        return self._delete(table, where_key, where_value, **kwargs)


class TencentMySQL(MyMySqlAPI):
    """
    腾讯Mysql的入口；
    """

    def __init__(self, user, passwd, db,
                 host='t.sql.leecq.xyz', port=10080,
                 charset='utf8', **kwargs):
        self.SQL_HOST = host  # 主机
        self.SQL_PORT = port  # 端口
        self.SQL_USER = user  # 用户
        self.SQL_PASSWD = passwd  # 密码
        self.SQL_DB = db  # 数据库
        self.SQL_CHARSET = charset  # 编码
        super().__init__(host=self.SQL_HOST,
                         port=self.SQL_PORT,
                         user=self.SQL_USER,
                         passwd=self.SQL_PASSWD,
                         db=self.SQL_DB,
                         charset=self.SQL_CHARSET,
                         **kwargs
                         )


class LocalhostMySQL(MyMySqlAPI):
    """"""

    def __init__(self, user, passwd, db, **kwargs):
        super().__init__('localhost', 3306, user, passwd, db, 'utf8', **kwargs)


if __name__ == '__main__':
    # a = MyMySQL('t.sql.leecq.xyz', 10080, 'test', 'test123456', 'test', 'utf8', use_unicode=False)
    a = TencentMySQL('test', 'test123456', 'test', use_unicode=False)
    print(a.use_unicode)
