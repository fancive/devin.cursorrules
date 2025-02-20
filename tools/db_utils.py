#!/usr/bin/env python3

import argparse
import sys
import json
import os
import pandas as pd
import sqlalchemy
from pathlib import Path
from sqlalchemy import create_engine, text
from typing import Optional, Dict, List, Any, Union
from dotenv import load_dotenv

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# 加载环境变量
try:
    # 获取项目根目录
    root_dir = Path(__file__).parent.parent
    # 环境变量文件路径
    env_file = root_dir / ".env"

    # 加载环境变量文件
    if load_dotenv(env_file):
        print(
            f"DEBUG: Successfully loaded environment variables from {env_file}",
            file=sys.stderr,
        )

        # 从环境变量获取数据库配置
        DEFAULT_USERNAME = os.environ.get("DB_USERNAME", "root")
        DEFAULT_PASSWORD = os.environ.get("DB_PASSWORD", "root")
        DEFAULT_DBNAME = os.environ.get("DB_DBNAME", "test")
        DEFAULT_DRIVER = os.environ.get("DB_DRIVER", "mysql")
        DEFAULT_HOST = os.environ.get("DB_HOST", "127.0.0.1")
        DEFAULT_PORT = int(os.environ.get("DB_PORT", "3306"))
    else:
        print(
            f"WARNING: No environment variables loaded from {env_file}", file=sys.stderr
        )
        # 使用默认配置
        DEFAULT_USERNAME = "root"
        DEFAULT_PASSWORD = "root"
        DEFAULT_DBNAME = "test"
        DEFAULT_DRIVER = "mysql"
        DEFAULT_HOST = "127.0.0.1"
        DEFAULT_PORT = 3306
except Exception as e:
    print(f"WARNING: Failed to load environment variables: {str(e)}", file=sys.stderr)
    # 使用默认配置
    DEFAULT_USERNAME = "root"
    DEFAULT_PASSWORD = "root"
    DEFAULT_DBNAME = "test"
    DEFAULT_DRIVER = "mysql"
    DEFAULT_HOST = "127.0.0.1"
    DEFAULT_PORT = 3306


def create_connection_string(
    username: str = DEFAULT_USERNAME,
    password: str = DEFAULT_PASSWORD,
    dbname: str = DEFAULT_DBNAME,
    driver: str = DEFAULT_DRIVER,
    host: str = DEFAULT_HOST,
    port: int = DEFAULT_PORT,
) -> str:
    """
    创建数据库连接字符串

    Args:
        username: 数据库用户名
        password: 数据库密码
        dbname: 数据库名称
        driver: 数据库驱动 (mysql, postgresql, etc.)
        host: 数据库主机
        port: 数据库端口

    Returns:
        str: 数据库连接字符串
    """
    if driver == "mysql":
        return f"mysql+pymysql://{username}:{password}@{host}:{port}/{dbname}"
    elif driver == "postgresql":
        return f"postgresql://{username}:{password}@{host}:{port}/{dbname}"
    else:
        raise ValueError(f"Unsupported database driver: {driver}")


def execute_query(
    query: str,
    connection_string: Optional[str] = None,
    params: Optional[Dict[str, Any]] = None,
    username: str = DEFAULT_USERNAME,
    password: str = DEFAULT_PASSWORD,
    dbname: str = DEFAULT_DBNAME,
    driver: str = DEFAULT_DRIVER,
    host: str = DEFAULT_HOST,
    port: int = DEFAULT_PORT,
    output_format: str = "csv",
) -> Union[str, pd.DataFrame]:
    """
    执行SQL查询并返回结果

    Args:
        query: SQL查询语句
        connection_string: 数据库连接字符串，如果提供则使用此连接字符串
        params: 查询参数
        username: 数据库用户名
        password: 数据库密码
        dbname: 数据库名称
        driver: 数据库驱动 (mysql, postgresql, etc.)
        host: 数据库主机
        port: 数据库端口
        output_format: 输出格式 (csv, json, df)

    Returns:
        查询结果，格式由output_format决定
    """
    try:
        # 如果没有提供连接字符串，则创建一个
        if connection_string is None:
            connection_string = create_connection_string(
                username, password, dbname, driver, host, port
            )

        print(f"DEBUG: Connecting to database using: {driver} driver", file=sys.stderr)
        print(f"DEBUG: Host: {host}, Port: {port}, Database: {dbname}", file=sys.stderr)

        # 创建数据库引擎
        engine = create_engine(connection_string)

        # 执行查询
        with engine.connect() as connection:
            if params:
                result = connection.execute(text(query), params)
            else:
                result = connection.execute(text(query))

            # 获取列名
            columns = result.keys()

            # 获取所有行
            rows = result.fetchall()

            # 创建DataFrame
            df = pd.DataFrame(rows, columns=columns)

            # 根据输出格式返回结果
            if output_format.lower() == "csv":
                return df.to_csv(index=False)
            elif output_format.lower() == "json":
                return df.to_json(orient="records")
            elif output_format.lower() == "df":
                return df
            else:
                raise ValueError(f"Unsupported output format: {output_format}")

    except Exception as e:
        print(f"ERROR: Failed to execute query: {str(e)}", file=sys.stderr)
        raise


def list_tables(
    connection_string: Optional[str] = None,
    username: str = DEFAULT_USERNAME,
    password: str = DEFAULT_PASSWORD,
    dbname: str = DEFAULT_DBNAME,
    driver: str = DEFAULT_DRIVER,
    host: str = DEFAULT_HOST,
    port: int = DEFAULT_PORT,
) -> List[str]:
    """
    列出数据库中的所有表

    Args:
        connection_string: 数据库连接字符串，如果提供则使用此连接字符串
        username: 数据库用户名
        password: 数据库密码
        dbname: 数据库名称
        driver: 数据库驱动 (mysql, postgresql, etc.)
        host: 数据库主机
        port: 数据库端口

    Returns:
        List[str]: 表名列表
    """
    try:
        # 如果没有提供连接字符串，则创建一个
        if connection_string is None:
            connection_string = create_connection_string(
                username, password, dbname, driver, host, port
            )

        # 创建数据库引擎
        engine = create_engine(connection_string)

        # 获取所有表名
        with engine.connect() as connection:
            if driver == "mysql":
                query = "SHOW TABLES"
            elif driver == "postgresql":
                query = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"
            else:
                raise ValueError(f"Unsupported database driver: {driver}")

            result = connection.execute(text(query))
            tables = [row[0] for row in result.fetchall()]

            return tables

    except Exception as e:
        print(f"ERROR: Failed to list tables: {str(e)}", file=sys.stderr)
        raise


def describe_table(
    table_name: str,
    connection_string: Optional[str] = None,
    username: str = DEFAULT_USERNAME,
    password: str = DEFAULT_PASSWORD,
    dbname: str = DEFAULT_DBNAME,
    driver: str = DEFAULT_DRIVER,
    host: str = DEFAULT_HOST,
    port: int = DEFAULT_PORT,
    output_format: str = "csv",
) -> Union[str, pd.DataFrame]:
    """
    描述表结构

    Args:
        table_name: 表名
        connection_string: 数据库连接字符串，如果提供则使用此连接字符串
        username: 数据库用户名
        password: 数据库密码
        dbname: 数据库名称
        driver: 数据库驱动 (mysql, postgresql, etc.)
        host: 数据库主机
        port: 数据库端口
        output_format: 输出格式 (csv, json, df)

    Returns:
        表结构信息，格式由output_format决定
    """
    try:
        # 如果没有提供连接字符串，则创建一个
        if connection_string is None:
            connection_string = create_connection_string(
                username, password, dbname, driver, host, port
            )

        # 创建数据库引擎
        engine = create_engine(connection_string)

        # 获取表结构
        with engine.connect() as connection:
            if driver == "mysql":
                query = f"DESCRIBE {table_name}"
            elif driver == "postgresql":
                query = f"""
                SELECT 
                    column_name, 
                    data_type, 
                    character_maximum_length, 
                    column_default, 
                    is_nullable
                FROM 
                    information_schema.columns
                WHERE 
                    table_name = '{table_name}'
                """
            else:
                raise ValueError(f"Unsupported database driver: {driver}")

            result = connection.execute(text(query))

            # 获取列名
            columns = result.keys()

            # 获取所有行
            rows = result.fetchall()

            # 创建DataFrame
            df = pd.DataFrame(rows, columns=columns)

            # 根据输出格式返回结果
            if output_format.lower() == "csv":
                return df.to_csv(index=False)
            elif output_format.lower() == "json":
                return df.to_json(orient="records")
            elif output_format.lower() == "df":
                return df
            else:
                raise ValueError(f"Unsupported output format: {output_format}")

    except Exception as e:
        print(f"ERROR: Failed to describe table: {str(e)}", file=sys.stderr)
        raise


def main():
    """
    主函数，处理命令行参数并执行相应的操作
    """
    parser = argparse.ArgumentParser(description="Database Utility Tool")
    subparsers = parser.add_subparsers(dest="command", help="Command to execute")

    # 查询子命令
    query_parser = subparsers.add_parser("query", help="Execute SQL query")
    query_parser.add_argument("--query", required=True, help="SQL query to execute")
    query_parser.add_argument("--params", help="Query parameters in JSON format")
    query_parser.add_argument(
        "--username", default=DEFAULT_USERNAME, help="Database username"
    )
    query_parser.add_argument(
        "--password", default=DEFAULT_PASSWORD, help="Database password"
    )
    query_parser.add_argument("--dbname", default=DEFAULT_DBNAME, help="Database name")
    query_parser.add_argument(
        "--driver", default=DEFAULT_DRIVER, help="Database driver"
    )
    query_parser.add_argument("--host", default=DEFAULT_HOST, help="Database host")
    query_parser.add_argument(
        "--port", type=int, default=DEFAULT_PORT, help="Database port"
    )
    query_parser.add_argument(
        "--output-format", default="csv", help="Output format (csv, json)"
    )

    # 列出表子命令
    list_tables_parser = subparsers.add_parser(
        "list-tables", help="List all tables in the database"
    )
    list_tables_parser.add_argument(
        "--username", default=DEFAULT_USERNAME, help="Database username"
    )
    list_tables_parser.add_argument(
        "--password", default=DEFAULT_PASSWORD, help="Database password"
    )
    list_tables_parser.add_argument(
        "--dbname", default=DEFAULT_DBNAME, help="Database name"
    )
    list_tables_parser.add_argument(
        "--driver", default=DEFAULT_DRIVER, help="Database driver"
    )
    list_tables_parser.add_argument(
        "--host", default=DEFAULT_HOST, help="Database host"
    )
    list_tables_parser.add_argument(
        "--port", type=int, default=DEFAULT_PORT, help="Database port"
    )

    # 描述表子命令
    describe_table_parser = subparsers.add_parser(
        "describe-table", help="Describe table structure"
    )
    describe_table_parser.add_argument("--table", required=True, help="Table name")
    describe_table_parser.add_argument(
        "--username", default=DEFAULT_USERNAME, help="Database username"
    )
    describe_table_parser.add_argument(
        "--password", default=DEFAULT_PASSWORD, help="Database password"
    )
    describe_table_parser.add_argument(
        "--dbname", default=DEFAULT_DBNAME, help="Database name"
    )
    describe_table_parser.add_argument(
        "--driver", default=DEFAULT_DRIVER, help="Database driver"
    )
    describe_table_parser.add_argument(
        "--host", default=DEFAULT_HOST, help="Database host"
    )
    describe_table_parser.add_argument(
        "--port", type=int, default=DEFAULT_PORT, help="Database port"
    )
    describe_table_parser.add_argument(
        "--output-format", default="csv", help="Output format (csv, json)"
    )

    args = parser.parse_args()

    if args.command == "query":
        params = None
        if args.params:
            params = json.loads(args.params)

        result = execute_query(
            query=args.query,
            params=params,
            username=args.username,
            password=args.password,
            dbname=args.dbname,
            driver=args.driver,
            host=args.host,
            port=args.port,
            output_format=args.output_format,
        )
        print(result)

    elif args.command == "list-tables":
        tables = list_tables(
            username=args.username,
            password=args.password,
            dbname=args.dbname,
            driver=args.driver,
            host=args.host,
            port=args.port,
        )
        for table in tables:
            print(table)

    elif args.command == "describe-table":
        result = describe_table(
            table_name=args.table,
            username=args.username,
            password=args.password,
            dbname=args.dbname,
            driver=args.driver,
            host=args.host,
            port=args.port,
            output_format=args.output_format,
        )
        print(result)

    else:
        parser.print_help()


if __name__ == "__main__":
    main()
