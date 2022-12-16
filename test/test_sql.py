#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""
@File Name  : test_sql.py
@Author     : LeeCQ
@Date-Time  : 2020/7/14 19:00

0. 连接检查
1. 创建数据库
2. 插入数据
3. 查询数据
4. 修改数据（update）
5. 删除数据（行）
6. 修改表结构
7. drop操作
9. 删除表操作
"""
import unittest
import json
from time import time
from pathlib import Path

from sqllib.mysql.mysqlbase import MySqlAPI
from sqllib.SQLite.sqlite import SQLiteAPI
from sqllib.common.base_sql import BaseSQL
from sqllib.common.error import *

WORKDIR = Path(__file__).parent
__DEBUG__ = True
# ':memory:' if not False else
__SQLite__ = ":memory:" if not __DEBUG__ else WORKDIR / 'sup/UT.sqlite'  
# __MySQL__ = ('t.sql.leecq.cn', 10080, 'test', 'test123456', 'test')
# __MySQL__ = ('localhost', 3306, 'test', 'test123456', 'test')

table_name = 'PYTHON_UNITTEST'
table_prefix = 'UT_'
table_structure = (  # f' CREATE TABLE `{table_name}` ( '
    f'_ID INT AUTO_INCREMENT PRIMARY KEY , '
    f'TEST_STR VARCHAR(100) NOT NULL COMMENT "随机字符串", '
    f'TEST_DATETIME DATETIME NOT NULL COMMENT "当前时间", '
    f'TEST_INT INT NOT NULL COMMENT "整数时间戳", '
    f'TEST_FLOAT DOUBLE NOT NULL COMMENT "时间戳" , '
    f'TEST_JSON JSON NOT NULL COMMENT "测试JSON格式写入" , '
    f'TEST_BLOB MEDIUMBLOB COMMENT "测试二进制数据的写入"'
)
table_keys = ['TEST_STR', 'TEST_DATETIME', 'TEST_INT', 'TEST_FLOAT', 'TEST_JSON', 'TEST_BLOB']
table_data_one = (r'!@#$%^&*()_+-=/{}[]\' ";,.<>/`·This is a character test string。7894561320',
                  '2021-1-9 17:28:16', int(time()), time(),
                  json.dumps({'a': 1, 'b': 2}),
                  b'\x99'
                  )
table_data_more = (table_data_one,
                   ('A', '2021-1-9 18:21:55', int(time()), time(), json.dumps({"A": 1}), b'This is BLOB for MEDIUM 1 '),
                   ('B', '2021-1-9 18:24:35', int(time()), time(), json.dumps({"B": 2}), b'This is BLOB for MEDIUM 2 ')
                   )


def _dict_for_data(is_more=False):
    """打包"""
    if is_more:
        return {_x: _y for _x, _y in zip(table_keys, zip(*table_data_more))}
    else:
        return {_x: _y for _x, _y in zip(table_keys, table_data_one)}


def _zip_to_list(_zip):
    """行转列"""
    return [_ for _ in zip(*_zip)]


class TESTMySql(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        # sql = MySqlAPI('a.sql.leecq.cn', 3306, 'test_sqllib', 'Test_sqllib', 'test_sqllib', warning=False, charset='utf8')
        # sql = MySqlAPI('localhost', 3306, 'test', 'test123456', 'test', warning=False, charset='utf8')
        sql = SQLiteAPI(__SQLite__, prefix=table_prefix)
        cls.sql = sql
        cls.db_name = 'main' if isinstance(sql, SQLiteAPI) else 'test_sqllib'

    def setUp(self) -> None:
        # self.sql = MySqlAPI(*__MySQL__, warning=False, charset='utf8')
        # self.sql = SQLiteAPI(__SQLite__)
        self.sql.set_prefix(table_prefix)

    # def tearDown(self) -> None:
    #     self.sql.close()

    def test_00_connect(self):
        self.assertIsInstance(self.sql, BaseSQL, "数据库连接失败")

    def test_01_set_pr(self):
        self.assertEqual(table_prefix, self.sql.TABLE_PREFIX, "设置表前缀未生效！")

    def test_02_pool_connect(self):
        """连接池测试"""
        self.sql.pooling_sql()

    @unittest.expectedFailure
    def test_11_create_table_noName(self):
        try:
            self.sql.drop_table(table_name)
        except:
            pass
        _cmd = f'CREATE TABLE `{table_name}` ' + ' ( ' + table_structure + ' ) '
        print(_cmd)
        self.sql.create_table(_cmd, table_name)

    def test_12_create_table_with_name(self):
        self.sql.create_table(cmd=table_structure, table_name=table_name, exists_ok=True)
        self.sql.show_tables()

    def test_13_create_table_existed(self):
        self.sql.create_table(cmd=table_structure, table_name=table_name, exists_ok=True)
        self.assertRaises(SqlWriteError, self.sql.create_table, table_structure, table_name)

    def test_14_create_table_json_sql(self):
        """join_sql()函数的使用情况"""
        _cmd = (('_ID', ' INT', 'auto_increment PRIMARY KEY '),
                ('TEST_STR', ' VARCHAR(100)', ' NOT NULL COMMENT "随机字符串"'),
                ('TEST_DATETIME', ' DATETIME NOT NULL COMMENT "当前时间"'),
                ('TEST_INT', ' INT', ' NOT NULL COMMENT "整数时间戳" '),
                ('TEST_FLOAT', ' DOUBLE', ' NOT NULL COMMENT "时间戳" '),
                ('TEST_JSON', ' JSON', ' NOT NULL COMMENT "测试JSON格式写入"  '),
                ('TEST_BLOB', ' MEDIUMBLOB', ' COMMENT "测试二进制数据的写入"'),
                )
        self.sql.create_table('test_join_sql', _cmd, )
        self.sql.drop_table('test_join_sql')

    def test_21_insert_one(self):
        """插入一条数据"""
        _a = self.sql.insert(table_name, **_dict_for_data())
        self.assertTrue(_a > 0, f'插入的行数为 {_a}')

    def test_22_insert_more(self):
        """插入多条数据"""
        _a = self.sql.insert(table_name, ignore_repeat=False, **_dict_for_data(is_more=True))
        self.assertTrue(_a > 0, f'插入的行数为 {_a}')

    def test_23_insert_zip_error(self):
        """插入插入多条数据时，数据长度不一致"""
        _dict_data = _dict_for_data(is_more=True)
        _dict_data[table_keys[0]] = ('A', 'B')
        self.assertRaises(InsertZipError, self.sql.insert, table_name, ignore=False, **_dict_data)

    def test_24_insert_not_null_error(self):
        """NOT NULL 但是没有给值！"""
        _dict_data = _dict_for_data(is_more=False)
        _dict_data[table_keys[0]] = None
        self.assertRaises(SqlWriteError, self.sql.insert, table_name, **_dict_data)

    def test_31_select(self):
        """基本操作"""
        # print(self.sql.TABLE_PREFIX)
        _a = self.sql.select(table_name, '*')

    def test_32_select_col(self):
        """查询指定列以及传列方式的兼容性"""
        _keys = table_keys[1:3]
        _a = self.sql.select(table_name, _keys, result_type=dict)
        _b = self.sql.select(table_name, *_keys, result_type=dict)
        self.assertEqual(_a, _b, f"列表传参和*args传参返回数据不一致")

    def test_33_select_where(self):
        """查询指定行"""
        _ = [i[0] for i in self.sql.select(table_name, '_ID', where=f'{table_keys[1]}="{table_data_one[1]}"')]
        self.assertEqual([1, 2], _)

    def test_34_select_limit(self):
        """查询2个条目(2,3)"""
        _test_map = [((2, 2), [3, 4], ''),
                     ((3, 1), [2, 3, 4], ''),
                     ((1, 4), [], ''),
                     ]
        for _in, _out, msg in _test_map:
            with self.subTest(_in=_in, _out=_out, msg=msg):
                _ = [i[0] for i in self.sql.select(table_name, '_ID', LIMIT=_in[0], OFFSET=_in[1])]
                self.assertEqual(_out, _, msg)

    def test_35_select_order(self):
        """结果排序"""
        _ = [i[0] for i in self.sql.select(table_name, '_ID', ORDER=f'{table_keys[0]} DESC')]
        self.assertEqual([4, 3, 1, 2], _, )

    def test_36_show_tables(self):
        """返回表名字"""
        _ = [i.upper() for i in self.sql.tables_name()]
        print(_)
        self.assertIn(self.sql.get_real_table_name(table_name).upper(), _, '返回的数据表名称不正确')

    def test_37_show_columns(self):
        _ = self.sql.columns_name(table_name)
        print(_)
        self.assertTrue(set(table_keys).issubset(_), '返回的表字段异常')

    def test_41_update(self):
        """修改数据（更新数据）"""
        self.sql.update(table_name, '_ID', '1', TEST_STR='NEW1')
        _ = self.sql.select(table_name, 'TEST_STR', WHERE='_ID=1')[0][0]
        self.assertEqual('NEW1', _.decode() if isinstance(_, bytes) else _, "数据更改失败")

    def test_51_delete_row(self):
        """删除匹配的行"""
        self.sql.delete(table_name, '_ID', '1', )
        _ = self.sql.select(table_name, '_ID', WHERE='_ID=1')
        self.assertEqual(0, len(_))

    def test_61_alter_add_col(self):
        """修改表结构-添加列"""

    def test_62_alter_edit_col(self):
        """修改表结构-编辑列"""

    def test_63_alter_drop_col(self):
        """修改表结构-删除列"""

    def test_80(self):
        """测试表前缀"""
        tn = 'prefix_test'
        ptn = table_prefix + tn
        self.sql.create_table(tn, 'a varchar(10)', exists_ok=True)
        self.sql.insert(ptn, a='sss')
        self.sql.insert(tn, a='tn')
        assert self.sql.select(ptn, 'a') == self.sql.select(tn, 'a')
        self.sql.update(ptn, 'a', 'sss', a='aaa')
        self.sql.delete(ptn, 'a', 'aaa')
        self.sql.drop_table(ptn)

    # @unittest.skipIf(__DEBUG__, "DEBUG-ING ...")
    def test_91_drop_table(self):
        """删除数据表测试"""
        _ = self.sql.drop_table(table_name)
        self.assertEqual(_, -1, '删除数据表失败')

    @unittest.skipIf(__DEBUG__, "DEBUG-ING ...")
    def test_91_drop_db(self):
        _ = self.sql.drop_db(self.db_name)


class TESTSQLite(TESTMySql):

    def setUp(self) -> None:
        self.sql.set_prefix(table_prefix)

    @unittest.skipIf(__DEBUG__, "DEBUG-ING ...")
    def test_91_drop_db(self):
        _ = self.sql.drop_db(self.db_name)


if __name__ == '__main__':
    unittest.main()
    # mysql = TESTMySql()
    # # sqlite = TESTSQLite()
    # test = unittest.TestSuite()
    # test.addTest(mysql)
    #
    # runner = unittest.TestRunner()
    # runner.run(test)
