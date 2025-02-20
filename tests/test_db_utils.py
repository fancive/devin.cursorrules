#!/usr/bin/env python3

import unittest
import sys
import os
from unittest.mock import patch, MagicMock, call
import json
import io
import pandas as pd

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from tools.db_utils import (
    create_connection_string,
    execute_query,
    list_tables,
    describe_table,
)


class TestDBUtils(unittest.TestCase):
    """数据库工具单元测试"""

    def test_create_connection_string_mysql(self):
        """测试创建MySQL连接字符串"""
        conn_str = create_connection_string(
            username="test-user",
            password="test-pass",
            dbname="test-db",
            driver="mysql",
            host="test-host",
            port=3306,
        )
        expected = "mysql+pymysql://test-user:test-pass@test-host:3306/test-db"
        self.assertEqual(conn_str, expected)

    def test_create_connection_string_postgresql(self):
        """测试创建PostgreSQL连接字符串"""
        conn_str = create_connection_string(
            username="test-user",
            password="test-pass",
            dbname="test-db",
            driver="postgresql",
            host="test-host",
            port=5432,
        )
        expected = "postgresql://test-user:test-pass@test-host:5432/test-db"
        self.assertEqual(conn_str, expected)

    def test_create_connection_string_unsupported(self):
        """测试创建不支持的数据库连接字符串"""
        with self.assertRaises(ValueError):
            create_connection_string(
                username="test-user",
                password="test-pass",
                dbname="test-db",
                driver="unsupported",
                host="test-host",
                port=1234,
            )

    @patch("tools.db_utils.create_engine")
    def test_execute_query_with_connection_string(self, mock_create_engine):
        """测试使用连接字符串执行查询"""
        # 设置模拟对象
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine

        mock_connection = MagicMock()
        mock_engine.connect.return_value.__enter__.return_value = mock_connection

        mock_result = MagicMock()
        mock_connection.execute.return_value = mock_result

        # 模拟查询结果
        mock_result.keys.return_value = ["id", "name"]
        mock_result.fetchall.return_value = [(1, "test1"), (2, "test2")]

        # 创建测试DataFrame
        test_df = pd.DataFrame([(1, "test1"), (2, "test2")], columns=["id", "name"])

        # 模拟DataFrame方法
        with patch("tools.db_utils.pd.DataFrame", return_value=test_df):
            with patch.object(
                test_df, "to_csv", return_value="id,name\n1,test1\n2,test2"
            ):
                # 调用函数
                result = execute_query(
                    query="SELECT * FROM test",
                    connection_string="test-connection-string",
                    output_format="csv",
                )

        # 验证结果
        mock_create_engine.assert_called_once_with("test-connection-string")
        mock_connection.execute.assert_called_once()
        self.assertEqual(result, "id,name\n1,test1\n2,test2")

    @patch("tools.db_utils.create_connection_string")
    @patch("tools.db_utils.create_engine")
    def test_execute_query_without_connection_string(
        self, mock_create_engine, mock_create_connection_string
    ):
        """测试不使用连接字符串执行查询"""
        # 设置模拟对象
        mock_create_connection_string.return_value = "generated-connection-string"

        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine

        mock_connection = MagicMock()
        mock_engine.connect.return_value.__enter__.return_value = mock_connection

        mock_result = MagicMock()
        mock_connection.execute.return_value = mock_result

        # 模拟查询结果
        mock_result.keys.return_value = ["id", "name"]
        mock_result.fetchall.return_value = [(1, "test1"), (2, "test2")]

        # 创建测试DataFrame
        test_df = pd.DataFrame([(1, "test1"), (2, "test2")], columns=["id", "name"])

        # 模拟DataFrame方法
        with patch("tools.db_utils.pd.DataFrame", return_value=test_df):
            with patch.object(
                test_df,
                "to_json",
                return_value='[{"id":1,"name":"test1"},{"id":2,"name":"test2"}]',
            ):
                # 调用函数
                result = execute_query(
                    query="SELECT * FROM test",
                    username="test-user",
                    password="test-pass",
                    dbname="test-db",
                    driver="mysql",
                    host="test-host",
                    port=3306,
                    output_format="json",
                )

        # 验证结果
        mock_create_connection_string.assert_called_once_with(
            "test-user", "test-pass", "test-db", "mysql", "test-host", 3306
        )
        mock_create_engine.assert_called_once_with("generated-connection-string")
        mock_connection.execute.assert_called_once()
        self.assertEqual(result, '[{"id":1,"name":"test1"},{"id":2,"name":"test2"}]')

    @patch("tools.db_utils.create_connection_string")
    @patch("tools.db_utils.create_engine")
    def test_execute_query_with_params(
        self, mock_create_engine, mock_create_connection_string
    ):
        """测试带参数执行查询"""
        # 设置模拟对象
        mock_create_connection_string.return_value = "generated-connection-string"

        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine

        mock_connection = MagicMock()
        mock_engine.connect.return_value.__enter__.return_value = mock_connection

        mock_result = MagicMock()
        mock_connection.execute.return_value = mock_result

        # 模拟查询结果
        mock_result.keys.return_value = ["id", "name"]
        mock_result.fetchall.return_value = [(1, "test1")]

        # 创建测试DataFrame
        test_df = pd.DataFrame([(1, "test1")], columns=["id", "name"])

        # 模拟DataFrame方法
        with patch("tools.db_utils.pd.DataFrame", return_value=test_df):
            # 调用函数
            result = execute_query(
                query="SELECT * FROM test WHERE id = :id",
                params={"id": 1},
                username="test-user",
                password="test-pass",
                dbname="test-db",
                driver="mysql",
                host="test-host",
                port=3306,
                output_format="df",
            )

        # 验证结果
        mock_create_connection_string.assert_called_once_with(
            "test-user", "test-pass", "test-db", "mysql", "test-host", 3306
        )
        mock_create_engine.assert_called_once_with("generated-connection-string")
        self.assertIs(result, test_df)

    @patch("tools.db_utils.create_connection_string")
    @patch("tools.db_utils.create_engine")
    def test_list_tables_mysql(self, mock_create_engine, mock_create_connection_string):
        """测试列出MySQL表"""
        # 设置模拟对象
        mock_create_connection_string.return_value = "generated-connection-string"

        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine

        mock_connection = MagicMock()
        mock_engine.connect.return_value.__enter__.return_value = mock_connection

        mock_result = MagicMock()
        mock_connection.execute.return_value = mock_result

        # 模拟查询结果
        mock_result.fetchall.return_value = [("table1",), ("table2",), ("table3",)]

        # 调用函数
        result = list_tables(
            username="test-user",
            password="test-pass",
            dbname="test-db",
            driver="mysql",
            host="test-host",
            port=3306,
        )

        # 验证结果
        mock_create_connection_string.assert_called_once_with(
            "test-user", "test-pass", "test-db", "mysql", "test-host", 3306
        )
        mock_create_engine.assert_called_once_with("generated-connection-string")
        self.assertEqual(result, ["table1", "table2", "table3"])

    @patch("tools.db_utils.create_connection_string")
    @patch("tools.db_utils.create_engine")
    def test_list_tables_postgresql(
        self, mock_create_engine, mock_create_connection_string
    ):
        """测试列出PostgreSQL表"""
        # 设置模拟对象
        mock_create_connection_string.return_value = "generated-connection-string"

        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine

        mock_connection = MagicMock()
        mock_engine.connect.return_value.__enter__.return_value = mock_connection

        mock_result = MagicMock()
        mock_connection.execute.return_value = mock_result

        # 模拟查询结果
        mock_result.fetchall.return_value = [("table1",), ("table2",), ("table3",)]

        # 调用函数
        result = list_tables(
            username="test-user",
            password="test-pass",
            dbname="test-db",
            driver="postgresql",
            host="test-host",
            port=5432,
        )

        # 验证结果
        mock_create_connection_string.assert_called_once_with(
            "test-user", "test-pass", "test-db", "postgresql", "test-host", 5432
        )
        mock_create_engine.assert_called_once_with("generated-connection-string")
        self.assertEqual(result, ["table1", "table2", "table3"])

    @patch("tools.db_utils.create_connection_string")
    @patch("tools.db_utils.create_engine")
    def test_describe_table_mysql(
        self, mock_create_engine, mock_create_connection_string
    ):
        """测试描述MySQL表结构"""
        # 设置模拟对象
        mock_create_connection_string.return_value = "generated-connection-string"

        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine

        mock_connection = MagicMock()
        mock_engine.connect.return_value.__enter__.return_value = mock_connection

        mock_result = MagicMock()
        mock_connection.execute.return_value = mock_result

        # 模拟查询结果
        mock_result.keys.return_value = [
            "Field",
            "Type",
            "Null",
            "Key",
            "Default",
            "Extra",
        ]
        mock_result.fetchall.return_value = [
            ("id", "int(11)", "NO", "PRI", None, "auto_increment"),
            ("name", "varchar(255)", "YES", "", None, ""),
        ]

        # 创建测试DataFrame
        test_df = pd.DataFrame(
            [
                ("id", "int(11)", "NO", "PRI", None, "auto_increment"),
                ("name", "varchar(255)", "YES", "", None, ""),
            ],
            columns=["Field", "Type", "Null", "Key", "Default", "Extra"],
        )

        # 模拟DataFrame方法
        with patch("tools.db_utils.pd.DataFrame", return_value=test_df):
            with patch.object(
                test_df,
                "to_csv",
                return_value="Field,Type,Null,Key,Default,Extra\nid,int(11),NO,PRI,,auto_increment\nname,varchar(255),YES,,,",
            ):
                # 调用函数
                result = describe_table(
                    table_name="test_table",
                    username="test-user",
                    password="test-pass",
                    dbname="test-db",
                    driver="mysql",
                    host="test-host",
                    port=3306,
                    output_format="csv",
                )

        # 验证结果
        mock_create_connection_string.assert_called_once_with(
            "test-user", "test-pass", "test-db", "mysql", "test-host", 3306
        )
        mock_create_engine.assert_called_once_with("generated-connection-string")
        self.assertEqual(
            result,
            "Field,Type,Null,Key,Default,Extra\nid,int(11),NO,PRI,,auto_increment\nname,varchar(255),YES,,,",
        )


class TestDBCommandLine(unittest.TestCase):
    """数据库命令行工具测试"""

    @patch("tools.db_utils.execute_query")
    @patch("sys.stdout", new_callable=io.StringIO)
    def test_main_query(self, mock_stdout, mock_execute_query):
        """测试命令行查询"""
        # 设置模拟对象
        mock_execute_query.return_value = "id,name\n1,test1\n2,test2"

        # 模拟命令行参数
        test_args = [
            "db_utils.py",
            "query",
            "--query",
            "SELECT * FROM test",
            "--username",
            "test-user",
            "--password",
            "test-pass",
            "--dbname",
            "test-db",
            "--driver",
            "mysql",
            "--host",
            "test-host",
            "--port",
            "3306",
            "--output-format",
            "csv",
        ]

        with patch("sys.argv", test_args):
            with patch("argparse.ArgumentParser.parse_args") as mock_parse_args:
                # 创建模拟参数对象
                args = MagicMock()
                args.command = "query"
                args.query = "SELECT * FROM test"
                args.params = None
                args.username = "test-user"
                args.password = "test-pass"
                args.dbname = "test-db"
                args.driver = "mysql"
                args.host = "test-host"
                args.port = 3306
                args.output_format = "csv"

                mock_parse_args.return_value = args

                # 导入并执行main函数
                from tools.db_utils import main

                with patch("tools.db_utils.parser.parse_args", return_value=args):
                    main()

        # 验证结果
        mock_execute_query.assert_called_once_with(
            query="SELECT * FROM test",
            params=None,
            username="test-user",
            password="test-pass",
            dbname="test-db",
            driver="mysql",
            host="test-host",
            port=3306,
            output_format="csv",
        )

    @patch("tools.db_utils.list_tables")
    @patch("sys.stdout", new_callable=io.StringIO)
    def test_main_list_tables(self, mock_stdout, mock_list_tables):
        """测试命令行列出表"""
        # 设置模拟对象
        mock_list_tables.return_value = ["table1", "table2", "table3"]

        # 模拟命令行参数
        test_args = [
            "db_utils.py",
            "list-tables",
            "--username",
            "test-user",
            "--password",
            "test-pass",
            "--dbname",
            "test-db",
            "--driver",
            "mysql",
            "--host",
            "test-host",
            "--port",
            "3306",
        ]

        with patch("sys.argv", test_args):
            with patch("argparse.ArgumentParser.parse_args") as mock_parse_args:
                # 创建模拟参数对象
                args = MagicMock()
                args.command = "list-tables"
                args.username = "test-user"
                args.password = "test-pass"
                args.dbname = "test-db"
                args.driver = "mysql"
                args.host = "test-host"
                args.port = 3306

                mock_parse_args.return_value = args

                # 导入并执行main函数
                from tools.db_utils import main

                with patch("tools.db_utils.parser.parse_args", return_value=args):
                    main()

        # 验证结果
        mock_list_tables.assert_called_once_with(
            username="test-user",
            password="test-pass",
            dbname="test-db",
            driver="mysql",
            host="test-host",
            port=3306,
        )

    @patch("tools.db_utils.describe_table")
    @patch("sys.stdout", new_callable=io.StringIO)
    def test_main_describe_table(self, mock_stdout, mock_describe_table):
        """测试命令行描述表"""
        # 设置模拟对象
        mock_describe_table.return_value = "Field,Type,Null,Key,Default,Extra\nid,int(11),NO,PRI,,auto_increment\nname,varchar(255),YES,,,"

        # 模拟命令行参数
        test_args = [
            "db_utils.py",
            "describe-table",
            "--table",
            "test_table",
            "--username",
            "test-user",
            "--password",
            "test-pass",
            "--dbname",
            "test-db",
            "--driver",
            "mysql",
            "--host",
            "test-host",
            "--port",
            "3306",
            "--output-format",
            "csv",
        ]

        with patch("sys.argv", test_args):
            with patch("argparse.ArgumentParser.parse_args") as mock_parse_args:
                # 创建模拟参数对象
                args = MagicMock()
                args.command = "describe-table"
                args.table = "test_table"
                args.username = "test-user"
                args.password = "test-pass"
                args.dbname = "test-db"
                args.driver = "mysql"
                args.host = "test-host"
                args.port = 3306
                args.output_format = "csv"

                mock_parse_args.return_value = args

                # 导入并执行main函数
                from tools.db_utils import main

                with patch("tools.db_utils.parser.parse_args", return_value=args):
                    main()

        # 验证结果
        mock_describe_table.assert_called_once_with(
            table_name="test_table",
            username="test-user",
            password="test-pass",
            dbname="test-db",
            driver="mysql",
            host="test-host",
            port=3306,
            output_format="csv",
        )


class TestDBIntegration(unittest.TestCase):
    """数据库集成测试

    注意：这些测试需要一个运行中的MySQL服务器
    要运行这些测试，请确保MySQL服务器正在运行，并设置环境变量：

    运行方式：
    DB_TEST_HOST=localhost DB_TEST_PORT=3306 python -m unittest tests.test_db_utils.TestDBIntegration
    """

    @classmethod
    def setUpClass(cls):
        """设置测试环境"""
        # 从环境变量获取数据库连接信息
        cls.username = os.environ.get("DB_TEST_USERNAME", "root")
        cls.password = os.environ.get("DB_TEST_PASSWORD", "root")
        cls.dbname = os.environ.get("DB_TEST_DBNAME", "test")
        cls.driver = os.environ.get("DB_TEST_DRIVER", "mysql")
        cls.host = os.environ.get("DB_TEST_HOST", "127.0.0.1")
        cls.port = int(os.environ.get("DB_TEST_PORT", 3306))

        # 尝试连接数据库
        try:
            # 创建测试表
            execute_query(
                query="""
                CREATE TABLE IF NOT EXISTS test_table (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    name VARCHAR(255),
                    value INT
                )
                """,
                username=cls.username,
                password=cls.password,
                dbname=cls.dbname,
                driver=cls.driver,
                host=cls.host,
                port=cls.port,
            )

            # 清空测试表
            execute_query(
                query="DELETE FROM test_table",
                username=cls.username,
                password=cls.password,
                dbname=cls.dbname,
                driver=cls.driver,
                host=cls.host,
                port=cls.port,
            )

            # 插入测试数据
            execute_query(
                query="""
                INSERT INTO test_table (name, value) VALUES
                ('test1', 100),
                ('test2', 200),
                ('test3', 300)
                """,
                username=cls.username,
                password=cls.password,
                dbname=cls.dbname,
                driver=cls.driver,
                host=cls.host,
                port=cls.port,
            )
        except Exception as e:
            raise unittest.SkipTest(f"无法连接到数据库服务器: {str(e)}")

    @classmethod
    def tearDownClass(cls):
        """清理测试环境"""
        try:
            # 删除测试表
            execute_query(
                query="DROP TABLE IF EXISTS test_table",
                username=cls.username,
                password=cls.password,
                dbname=cls.dbname,
                driver=cls.driver,
                host=cls.host,
                port=cls.port,
            )
        except Exception:
            pass

    def test_integration_execute_query(self):
        """集成测试：执行查询"""
        # 执行查询
        result = execute_query(
            query="SELECT * FROM test_table ORDER BY id",
            username=self.username,
            password=self.password,
            dbname=self.dbname,
            driver=self.driver,
            host=self.host,
            port=self.port,
            output_format="df",
        )

        # 验证结果
        self.assertEqual(len(result), 3)
        self.assertEqual(result.iloc[0]["name"], "test1")
        self.assertEqual(result.iloc[1]["name"], "test2")
        self.assertEqual(result.iloc[2]["name"], "test3")
        self.assertEqual(result.iloc[0]["value"], 100)
        self.assertEqual(result.iloc[1]["value"], 200)
        self.assertEqual(result.iloc[2]["value"], 300)

    def test_integration_execute_query_with_params(self):
        """集成测试：带参数执行查询"""
        # 执行查询
        result = execute_query(
            query="SELECT * FROM test_table WHERE value > :min_value ORDER BY id",
            params={"min_value": 150},
            username=self.username,
            password=self.password,
            dbname=self.dbname,
            driver=self.driver,
            host=self.host,
            port=self.port,
            output_format="df",
        )

        # 验证结果
        self.assertEqual(len(result), 2)
        self.assertEqual(result.iloc[0]["name"], "test2")
        self.assertEqual(result.iloc[1]["name"], "test3")
        self.assertEqual(result.iloc[0]["value"], 200)
        self.assertEqual(result.iloc[1]["value"], 300)

    def test_integration_list_tables(self):
        """集成测试：列出表"""
        # 列出表
        tables = list_tables(
            username=self.username,
            password=self.password,
            dbname=self.dbname,
            driver=self.driver,
            host=self.host,
            port=self.port,
        )

        # 验证结果
        self.assertIn("test_table", tables)

    def test_integration_describe_table(self):
        """集成测试：描述表结构"""
        # 描述表结构
        result = describe_table(
            table_name="test_table",
            username=self.username,
            password=self.password,
            dbname=self.dbname,
            driver=self.driver,
            host=self.host,
            port=self.port,
            output_format="df",
        )

        # 验证结果
        field_names = result["Field"].tolist()
        self.assertIn("id", field_names)
        self.assertIn("name", field_names)
        self.assertIn("value", field_names)


if __name__ == "__main__":
    unittest.main()
