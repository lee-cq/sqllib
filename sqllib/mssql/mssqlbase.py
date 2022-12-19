# /bin/env python3
# coding: utf8
"""MSSQL API"""

import sys
import logging

import pymssql

logger = logging.getLogger('sqllib.mssql')


class MsSqlBase:
    """SQLServer API"""

    def __init__(self, host, port, user, password, db):
        self.db = db
        self.password = password
        self.user = user
        self.port = port
        self.host = host
        self._sql = pymssql.connect(
            host=host, 
            port=port, 
            user=user, 
            password=password,
            database=db, 
            autocommit=True)

    # @property
    # def _sql(self):
    #     _s = pymssql.connect(host=self.host, port=self.port, user=self.user, password=self.password, database=self.db,
    #                          )
    #     logger.debug(f'SQL 链接信息： {_s}')
    #     return _s

    def _write_db(self, command, args=None):
        _sql = self._sql
        with _sql.cursor() as cur:
            try:
                _c = cur.execute(command, args)
                _sql.commit()  # 提交数据库
                return _c
            except Exception as _e:
                _sql.rollback()
                sys.exc_info()
                raise pymssql.OperationalError(
                    f'操作数据库时出现问题，数据库已回滚至操作前——\n{sys.exc_info()}\n\n{command}')
            finally:
                _sql.close()

    def _write_affair(self, command, args):
        _sql = self._sql
        try:
            with _sql.cursor() as cur:  # with 语句自动关闭游标
                _c = cur.executemany(command, args)
                _sql.commit()
            return _c
        except Exception:
            _sql.rollback()
            sys.exc_info()
            raise pymssql.OperationalError(
                "_write_rows() 操作数据库出错，已回滚 \n" + str(sys.exc_info()))
        finally:
            _sql.close()
            cur.close()

    def _read_db(self, command, args=None, result_type=None):
        _sql = self._sql
        with _sql.cursor(result_type) as cur:
            cur.execute(command, args)
            results = cur.fetchall()
            _sql.close()
            return results

    def read_db(self, command, args=None, result_type=None):
        return self._read_db(command, args=args, result_type=result_type)

    def write_db(self, command, args=None):
        return self._write_db(command, args=args)

    def show_tables(self) -> tuple:
        return list(zip(*self._read_db('SELECT name FROM [sysobjects] WHERE [xtype]=\'u\'')))[0]

    def tables_name(self):
        return list(zip(*self._read_db('SELECT name FROM [sysobjects] WHERE [xtype]=\'u\'')))[0]

    def create_table(self, name, cols):
        try:
            return self.write_db(f'CREATE TABLE [{name}] ({cols})')
        except pymssql.OperationalError:
            self.drop_table(name)
            return self.create_table(name, cols)

    def drop_table(self, name):
        return self.write_db(f'DROP TABLE [{name}]')

    def insert(self, table, **kwargs):
        _c = (f"INSERT  INTO [{table}]  "
              "( " +
              ', '.join([" [" + _k + "] " for _k in kwargs.keys()]) +
              " ) "  # 这一行放在后面会发生，乱版；
              " VALUES "
              # 添加值
              " ( " + ', '.join([" %s " for _k in kwargs.values()]) + " ) ; "
              )
        return self.write_db(_c, tuple(kwargs.values()))

    def select(self, table, cols, *args, result_type=None, **kwargs):
        """查询"""
        _col = ', '.join(f'[{c}]' for c in [cols] + list(args))
        command = f"SELECT TOP 1000 {_col}  FROM [{table}] "
        for key, value in kwargs.items():
            key = key.upper()
            if key in ['WHERE', 'LIMIT', 'OFFSET']:
                command += f' {key}  {value}'
            if key == 'ORDER':
                command += f' {key} BY {value}'
        logger.debug(f'SQL: {command}')
        return self.read_db(command, result_type=result_type)

    def update(self, table, where_key, where_value, **kwargs):
        _update_data = ' , '.join(
            [f" [{k}]=%({k})s  " for k, v in kwargs.items()])  # 构造更新内容
        command = (f"UPDATE [{table}] SET {_update_data}"
                   f" WHERE [{where_key}]='{where_value}' ;"  # 构造WHERE语句
                   )
        return self._write_db(command, kwargs)  # 执行SQL语句

    def delete(self, table, where_key, where_value, **kwargs):
        command = f"DELETE FROM [{table}] WHERE [{where_key}]='{where_value}'  "
        for k, v in kwargs.items():
            command += f"[{k}]='{v}'"

        return self._write_db(command)

    def close(self):
        self._sql.close()
