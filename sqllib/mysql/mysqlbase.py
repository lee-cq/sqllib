#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""
@Time   : 2019/11/26 12:54
@Author : LeeCQ
@File Name: mysql.py

更多操作详见 下方import 内容。
内容：
    * v1:
        1. 构建 class:MySQL 框架
            以 MySQL._write_db(), MySQL._write_rows(), MySQL._read_db()为基础访问SQL。
            创建_select, _insert, _update, _drop, _delete为基础的接口访问。
        2. 构建访问控制 & 安全 相关的语句。
            _key_and_table_is_exists
        3. 优化流程控制。
        4. 优化数据库访问。
    * v2: -2019/12/18
        1. 新增 DBUtils.PooledDB 模块：连接池
            1.1. 新增MyMySQL.pooled_sql()模块，以启用连接池
            1.2. 修改MyMySQL._write_db(), MySQL._write_rows(), MySQL._read_db():
                    当他的子类或者实例调用 -> MySQL.pooled_sql() <- 方法时，以开启连接池；
                    if self.pooled_sql is not None:
                        _sql = self.pooled_sql.connection()
                    else:
                        _sql = self._sql
        2. 微调 MySQL._create_table()方法：
            源：( with self._sql.cursor() as cur: \\ cur.execute(command, args) \\self._sql.commit() \\ return 0) ==>
            修改为：( return self._write_db(command, args) )
            有点：便于代码的重用性；

    * v3: -2020/01/11
        1. 新增 def _alter_table():  --> 向已有数据表中添加 键
        2. 更新详细了注释
        3. MySQL - 增加了对 pymysql.connect() - 所需参数的详细注解 -- 1/12

    *v4: - 2020/02/29
        1. 修改show_columns() - 表名前缀问题 <-  ok
        2. 修改show_table -> show_tables                     --ok  2020/07/20
        3. 修改show_tables逻辑 str(_c[0]) -> _c[0].decode()   -- ok 2020/07/20

"""

import logging
import sys
import pymysql
from sqllib.common.base_sql import BaseSQL, BaseSQLAPI
from sqllib.common.error import *
# from sqllib.common.common import sql_join
from dbutils.pooled_db import PooledDB
from warnings import filterwarnings

logger = logging.getLogger("mysql")  # 创建实例
formatter = logging.Formatter("[%(asctime)s] < %(funcName)s: %(lineno)d > [%(levelname)s] %(message)s")
# 终端日志
terminal_handler = logging.StreamHandler(sys.stdout)
terminal_handler.setFormatter(formatter)  # 日志文件的格式
logger.setLevel(logging.DEBUG)  # 设置日志文件等级

_all_ = ['MyMySqlAPI', 'MySqlAPI']


class MyBaseSQL(BaseSQL):
    """mysql 操作的模板：

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

    def __init__(self, host, port, user, passwd, db, charset,
                 use_unicode=None, pool=False, **kwargs):
        super().__init__()
        self.SQL_HOST = host  # 主机
        self.SQL_PORT = port  # 端口
        self.SQL_USER = user  # 用户
        self.SQL_PASSWD = passwd  # 密码
        self.SQL_DB = db  # 数据库
        self.SQL_CHARSET = charset  # 编码
        self.use_unicode = use_unicode
        # 表前缀
        self.TABLE_PREFIX = kwargs.pop('prefix', '')
        self._sql = pymysql.connect(host=self.SQL_HOST,
                                    port=self.SQL_PORT,
                                    user=self.SQL_USER,
                                    password=self.SQL_PASSWD,  # 可以用 passwd为别名
                                    database=self.SQL_DB,  # 可以用 db    为别名；
                                    charset=self.SQL_CHARSET,
                                    use_unicode=use_unicode,
                                    **kwargs
                                    )
        self.pooled_sql = None
        self.pooling_sql() if pool else None

    def set_use_db(self, db_name):
        """设置当前数据库"""
        return self._sql.select_db(db_name)

    def set_charset(self, charset):
        """设置数据库链接字符集"""
        return self._sql.set_charset(charset)

    # 建立连接池
    def pooling_sql(self, min_cached=0, max_cached=0,
                    max_connections=10, blocking=True,
                    max_usage=None, set_session=None, reset=True,
                    failures=None, ping=1,
                    **kwargs):
        """ 连接池建立

       ::param creator: 数据库连接池返回的模块；默认pymysql
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
        self.pooled_sql = PooledDB(creator=pymysql, mincached=min_cached, maxcached=max_cached,
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
        """设置表前缀"""
        self.TABLE_PREFIX = prefix

    def close(self):
        """关闭数据库连接"""
        self._sql.close()

    def _write_db(self, command, args=None):
        """执行数据库写入操作

        :type args: str, list or tuple
        """
        if self.pooled_sql is not None:
            _sql = self.pooled_sql.connection()
        else:
            _sql = self._sql

        cur = _sql.cursor()  # 使用cursor()方法获取操作游标
        try:
            _c = cur.execute(command, args)
            _sql.commit()  # 提交数据库
            return _c
        except Exception:
            _sql.rollback()
            sys.exc_info()
            raise SqlWriteError(f'操作数据库时出现问题，数据库已回滚至操作前——\n{sys.exc_info()}\n\n{command}')
        finally:
            cur.close()

    # 写入事务
    def _write_affair(self, command, args):
        """向数据库写入多行"""
        if self.pooled_sql is not None:
            _sql = self.pooled_sql.connection()
        else:
            _sql = self._sql

        try:
            with _sql.cursor() as cur:  # with 语句自动关闭游标
                _c = cur.executemany(command, args)
                _sql.commit()
            return _c
        except Exception:
            _sql.rollback()
            sys.exc_info()
            raise SqlWriteError("_write_rows() 操作数据库出错，已回滚 \n" + str(sys.exc_info()))

    def _read_db(self, command, args=None, result_type=None):
        """执行数据库读取数据， 返回结果

        :param result_type: 返回的结果集类型{dict, None, tuple, 'SSCursor', 'SSDictCursor'}
        """
        if self.pooled_sql is not None:
            _sql = self.pooled_sql.connection()
        else:
            _sql = self._sql

        ret_ = {dict: pymysql.cursors.DictCursor,
                None: pymysql.cursors.Cursor,
                tuple: pymysql.cursors.Cursor,
                list: pymysql.cursors.Cursor,
                'SSCursor': pymysql.cursors.SSCursor,
                'SSDictCursor': pymysql.cursors.SSDictCursor
                }
        cur = _sql.cursor(ret_[result_type])
        cur.execute(command, args)
        results = cur.fetchall()
        cur.close()
        return results

    # 查表中键的所有信息 - > list
    def _columns(self, table, result_type=None):
        """返回table中列（字段）的所有信息

         +-------+-------+------+------+-----+---------+-------+
         | index |   0   |  1   |   2  |  3  |    4    |   5   |
         +-------+-------+------+------+-----+---------+-------+
         | dict  | Field | Type | Null | Key | Default | Extra |
         +-------+-------+------+------+-----+---------+-------+
        """
        table = self.get_real_table_name(table)
        return self._read_db(f'show columns from `{table}`', result_type=result_type)

    # 查表中的键
    def columns_name(self, table) -> list:
        """返回 table 中的 列名在一个列表中"""
        table = self.get_real_table_name(table)
        return [_c[0].decode() if isinstance(_c[0], bytes) else _c[0] for _c in self._columns(table)]

    # 获取数据库的表名
    def tables_name(self) -> list:
        """由于链接时已经指定数据库，无需再次指定。返回数据库中所有表的名字。"""
        return [_c[0].decode() if isinstance(_c[0], bytes) else _c[0] for _c in self._read_db("show tables")]

    def _create_table(self, command: str, table_name, exists_ok=False, table_args='', *args):
        """回退强制要求传入 table_name"""

        if command.strip().endswith(','):
            command = command.strip()[:-1] + ' '

        _c = (f"CREATE TABLE {'IF NOT EXISTS' if exists_ok else ' '} "
              f"`{self.get_real_table_name(table_name)}` ( "
              + command +
              ") " + table_args)
        logger.debug(_c)
        # print(_c)
        return self._write_db(_c, args)

    # 插入表
    def _insert(self, table, ignore_repeat=False, **kwargs):
        """ 向数据库插入内容。

        :param table: 表名；
        :param ignore: 重复是否抛出异常
        :param kwargs: 字段名 = 值；
        :return:
        """
        ignore_ = 'IGNORE' if ignore_repeat else ''
        _c = (f"INSERT {ignore_} INTO `{self.get_real_table_name(table)}`  "
              "( " +
              ', '.join([" `" + _k + "` " for _k in kwargs.keys()]) +
              " ) "  # 这一行放在后面会发生，乱版；
              " VALUES "
              " ( " + ', '.join([" %s " for _k in kwargs.values()]) + " ) ; "  # 添加值
              )
        # print( _c)
        if not isinstance(list(kwargs.values())[0], (str, int, type(None), float)):
            arg = self.zip_data_for_insert(tuple(kwargs.values()))
            return self._write_affair(_c, arg)
        else:
            for x in kwargs.values():
                if isinstance(x, (list, tuple)):
                    raise InsertZipError("INSERT一条数据时，出现列表列或元组！确保数据统一")
            return self._write_db(_c, list(kwargs.values()))  # 提交

    def _insert_rows(self, table_name, args, k=None, ignore_repeat=False):
        """插入

        :param table_name:
        :param args:
        :param ignore_repeat:
        :return:
        """
        _a = [_.values() for _ in args]
        if k is None:
            if not isinstance(args[0], dict):
                raise ValueError(f'既没有k, 也不是dict')
            k = args[0].keys()
        _ignore = 'OR IGNORE' if ignore_repeat else ''
        _c = f"INSERT {_ignore} INTO {self.get_real_table_name(table_name)} ( "
        _c += ", ".join([_ for _ in k]) + " ) "
        _c += "VALUES ( "
        _c += ", ".join([f" %s " for _ in args[0].values()]) + ");"
        return self._write_affair(_c, _a)

    # 检索表
    def _select(self, table, columns_name: tuple and list, result_type=None, **kwargs):
        """ select的应用。

            ·· `就不再支持 * `
            ·· column_name, table, key 可以用 ` ` 包裹， value 一定不能用， value 用 ' ' 。
        :param result_type: {dict, None, tuple, 'SSCursor', 'SSDictCursor'}
        :param table:
        :param cols: 传参时自行使用 `` , 尤其是数字开头的参数
        :param kwargs: {'WHERE', 'LIMIT', 'OFFSET', ORDER} 全大写
        :return 结果集
        """

        command = f"SELECT  "
        command += ' , '.join(columns_name) + " "
        command += f'FROM `{self.get_real_table_name(table)}` '
        for key, value in kwargs.items():
            key = key.upper()
            if key in ['WHERE', 'LIMIT', 'OFFSET']:
                command += f' {key}  {value}'
            if key == 'ORDER':
                command += f' {key} BY {value}'
        # print(command, )
        return self._read_db(command, result_type=result_type)

    # 更新表
    def _update(self, table, where_key, where_value, **kwargs):
        """ 更新数据库 --2019/12/15

        :param table: 数据表名字
        :param where_key: where的键
        :param where_value: where的值
        :param kwargs: 更新的键 = 更新的值， 注意大小写，数字键要加 - ``
        :return: 0 成功。
        """
        self.key_and_table_is_exists(f'{self.get_real_table_name(table)}', where_key, **kwargs)  # 判断 表 & 键 的存在性！
        _update_data = ' , '.join([f" `{k}`=%({k})s  " for k, v in kwargs.items()])  # 构造更新内容
        command = (f"UPDATE `{self.get_real_table_name(table)}` SET  "
                   f"{_update_data}"
                   f" WHERE {where_key}='{where_value}' ;"  # 构造WHERE语句
                   )
        return self._write_db(command, kwargs)  # 执行SQL语句

    # 删除表或者数据库
    def _drop(self, option, name):
        """ 删除数据库内容：

        :param option: (TABLE or DATABASE)
        :param name:
        :return: 0 成功
        """
        if option.upper() == 'TABLE':
            command = f'DROP  {option}  `{self.get_real_table_name(name)}`'
        else:
            command = f'DROP  {option}  `{name}`'
        return self._write_db(command)

    def _delete(self, table, where_key, where_value, **kwargs):
        """删除数据表中的某一行数据，
        :param table:
        :param where_key: 最好是数据库的主键或唯一的键。如果数据库没有，则最好组合where，以保证删除 - 唯一行。
        :param where_value:
        :param kwargs: 键名=键值；where——key的补充。
        """
        self.key_and_table_is_exists(self.get_real_table_name(table), where_key, **kwargs)

        command = f"DELETE FROM `{self.get_real_table_name(table)}` WHERE {where_key}='{where_value}'  "
        for k, v in kwargs.items():
            command += f"{k}='{v}'"

        return self._write_db(command)

    def _alter(self, table, command: str):
        """向已有表中插入键

        语法：
            ALTER TABLE `{self.get_real_table_name(table)}` ADD COLUMN
                (`device_ip` VARCHAR(32) DEFAULT NULL COMMENT '设备IP',
                 `device_name` VARCHAR(128) DEFAULT NULL COMMENT '设备名称',
                 );
        :return:
        """
        if command.strip().endswith(','):
            command = command.strip()[:-1] + ' '
        _c = (f"ALTER TABLE `{self.get_real_table_name(table)}` ADD COLUMN ( "
              + command +
              ");")
        return self._write_db(_c)

    def write_db(self, command, *args):
        """write_db的外部访问"""
        return self._write_db(command, *args)

    def write_rows(self, c, *args):
        """write_rows的外部访问"""
        return self._write_affair(c, *args)

    def read_db(self, command, args=None, result_type=None):
        """读取数据库的外部访问"""
        return self._read_db(command, args, result_type)

    def show_tables(self):
        """列出当前数据库的数据表"""
        return self.tables_name()

    def show_columns(self, table_name, result_type=None):
        """列出指定表的字段名

        :param table_name:
        :param result_type: None | [list|tuple] | dict
        :return:
        """
        table_name = self.get_real_table_name(table_name)
        if result_type is None:
            return self.columns_name(table_name)
        elif result_type in [list, tuple]:
            return self._columns(table_name, result_type=list)
        else:
            return self._columns(table_name)

    def test_show(self):
        """数据库链接检测"""
        return self._read_db('show tables')


class MySqlAPI(MyBaseSQL, BaseSQLAPI):
    """

    **kwargs in _init_() : Optionally

        :param bind_address:
                当本地客户端有多个IP地址时，指定一个主机名或者IP。
                    When the client has multiple network interfaces, specify
                    the interface from which to connect to the host. Argument can be
                    a hostname or an IP address.
        :param unix_socket:
                可选的，您可以使用unix套接字而不是TCP/IP。
                    Optionally, you can use a unix socket rather than TCP/IP.
        :param read_timeout:
                读取连接的超时(以秒为单位)(默认:None - no timeout)
                    The timeout for reading from the connection in seconds (default: None - no timeout)
        :param write_timeout:
                写入连接的超时(以秒为单位)(默认:None - no timeout)
                    The timeout for writing to the connection in seconds (default: None - no timeout)
        :param sql_mode:
                我不太明白！
                    Default SQL_MODE to use.
        :param read_default_file:
                指定my.cnf文件从[client]部分读取这些参数。
                    Specifies  my.cnf file to read these parameters from under the [client] section.
        :param conv:
                转换字典以代替默认字典使用。这用于提供类型的自定义编组和解组。
                    Conversion dictionary to use instead of the default one.
                    This is used to provide custom marshalling and unmarshaling of types.
                    See converters.
        :param use_unicode:
                是否默认为unicode字符串。对于Py3k，这个选项默认为true。
                    Whether or not to default to unicode strings.
                    This option defaults to true for Py3k.
        :param client_flag:
                要发送到MySQL的自定义标记。在constants.CLIENT中找到潜在的价值。
                    Custom flags to send to mysql. Find potential values in constants.CLIENT.
        :param cursorclass:
                要使用的自定义游标类。 - 创建的数据库链接句柄默认的游标类。
                    Custom cursor class to use.
        :param init_command:
                建立连接时要运行的初始SQL语句。
                    Initial SQL statement to run when connection is established.
        :param connect_timeout:
                连接时抛出异常之前的超时。
                    Timeout before throwing an exception when connecting.
                        (default: 10, min: 1, max: 31536000)
        :param ssl:
                与mysql_ssl_set()的参数类似的一组参数。
                    A dict of arguments similar to mysql_ssl_set()'s parameters.
        :param read_default_group:
                要从配置文件中读取的组。
                    Group to read from in the configuration file.
        :param autocommit:
                自动提交模式。None表示使用服务器默认值。(默认值:False)
                    Autocommit mode. None means use server_test default. (default: False)
        :param local_infile:
                Bool，允许使用加载数据本地命令。(默认值:False)
                    Boolean to enable the use of LOAD DATA LOCAL command. (default: False)
        :param max_allowed_packet:
                以字节为单位发送到服务器的包的最大大小。(默认值:16 mb)
                    Max size of packet sent to server_test in bytes. (default: 16MB)
                        Only used to limit size of "LOAD LOCAL INFILE" data packet smaller than default (16KB).
        :param defer_connect:
                不要显式连接在建设-等待连接调用。 (默认值:False)
                    Don't explicitly connect on contruction - wait for connect call. (default: False)
        :param auth_plugin_map:
                处理该插件的类的插件名的字典。该类将把Connection对象作为构造函数的参数。
                该类需要一个身份验证方法，该方法将身份验证包作为一个参数。
                对于dialog插件，可以使用提示(echo, prompt)方法，(如果没有验证方法)用于从用户返回字符串。(实验)。
                    A dict of plugin names to a class that processes that plugin.
                    The class will take the Connection object as the argument to the constructor.
                    The class needs an authenticate method taking an authentication packet as
                    an argument.  For the dialog plugin, a prompt(echo, prompt) method can be used
                    (if no authenticate method) for returning a string from the user. (experimental)
        :param server_public_key:
                SHA256 用户鉴权 plugin公钥值。(默认值:无)。
                    SHA256 authenticaiton plugin public key value. (default: None)
        :param db:
                数据库的别名。(为了兼容MySQLdb)。
                    Alias for database. (for compatibility to MySQLdb)
        :param passwd:
                别名密码。(为了兼容MySQLdb)。
                    Alias for password. (for compatibility to MySQLdb)
        :param binary_prefix:
                在字节和字节数组上添加_binary前缀。(默认值:False)。
                    Add _binary prefix on bytes and bytearray. (default: False)

        See `Connection <https://www.python.org/dev/peps/pep-0249/#connection-objects>`_ in the
        specification.
        """

    def __init__(self, host, port, user, passwd, db, charset='utf8', warning=True, **kwargs):
        super().__init__(host, port, user, passwd, db, charset, **kwargs)
        if not warning:
            filterwarnings("ignore", category=pymysql.Warning)

    def create_table_compatible(self, cmd):
        return cmd

    def show_dbs(self):
        pass


class MyMySqlAPI(MySqlAPI):
    """API别名"""


if __name__ == '__main__':
    logger.addHandler(terminal_handler)  # 添加控制台
