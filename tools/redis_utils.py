#!/usr/bin/env python3

import argparse
import sys
import json
import redis
import os
from pathlib import Path
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

        # 从环境变量获取Redis配置
        DEFAULT_HOST = os.environ.get("REDIS_HOST", "localhost")
        DEFAULT_PORT = int(os.environ.get("REDIS_PORT", "6379"))
        DEFAULT_DB = int(os.environ.get("REDIS_DB", "0"))

        # 获取Redis密码，如果为空字符串则设为None
        password = os.environ.get("REDIS_PASSWORD", "")
        DEFAULT_PASSWORD = None if password == "" else password
    else:
        print(
            f"WARNING: No environment variables loaded from {env_file}", file=sys.stderr
        )
        # 使用默认配置
        DEFAULT_HOST = "localhost"
        DEFAULT_PORT = 6379
        DEFAULT_DB = 0
        DEFAULT_PASSWORD = None
except Exception as e:
    print(f"WARNING: Failed to load environment variables: {str(e)}", file=sys.stderr)
    # 使用默认配置
    DEFAULT_HOST = "localhost"
    DEFAULT_PORT = 6379
    DEFAULT_DB = 0
    DEFAULT_PASSWORD = None


def create_redis_client(
    host: str = DEFAULT_HOST,
    port: int = DEFAULT_PORT,
    db: int = DEFAULT_DB,
    password: Optional[str] = DEFAULT_PASSWORD,
    decode_responses: bool = True,
) -> redis.Redis:
    """
    创建Redis客户端连接

    Args:
        host: Redis主机
        port: Redis端口
        db: Redis数据库索引
        password: Redis密码
        decode_responses: 是否将响应解码为字符串

    Returns:
        redis.Redis: Redis客户端
    """
    try:
        client = redis.Redis(
            host=host,
            port=port,
            db=db,
            password=password,
            decode_responses=decode_responses,
        )
        # 测试连接
        client.ping()
        print(
            f"DEBUG: Successfully connected to Redis at {host}:{port}", file=sys.stderr
        )
        return client
    except Exception as e:
        print(f"ERROR: Failed to connect to Redis: {str(e)}", file=sys.stderr)
        raise


def get_value(
    key: str,
    host: str = DEFAULT_HOST,
    port: int = DEFAULT_PORT,
    db: int = DEFAULT_DB,
    password: Optional[str] = DEFAULT_PASSWORD,
) -> Optional[str]:
    """
    获取Redis键的值

    Args:
        key: Redis键
        host: Redis主机
        port: Redis端口
        db: Redis数据库索引
        password: Redis密码

    Returns:
        Optional[str]: 键的值，如果键不存在则返回None
    """
    try:
        client = create_redis_client(host, port, db, password)
        value = client.get(key)
        return value
    except Exception as e:
        print(f"ERROR: Failed to get value for key '{key}': {str(e)}", file=sys.stderr)
        raise


def set_value(
    key: str,
    value: str,
    expire: Optional[int] = None,
    host: str = DEFAULT_HOST,
    port: int = DEFAULT_PORT,
    db: int = DEFAULT_DB,
    password: Optional[str] = DEFAULT_PASSWORD,
) -> bool:
    """
    设置Redis键的值

    Args:
        key: Redis键
        value: 要设置的值
        expire: 过期时间（秒）
        host: Redis主机
        port: Redis端口
        db: Redis数据库索引
        password: Redis密码

    Returns:
        bool: 操作是否成功
    """
    try:
        client = create_redis_client(host, port, db, password)
        result = client.set(key, value, ex=expire)
        return result
    except Exception as e:
        print(f"ERROR: Failed to set value for key '{key}': {str(e)}", file=sys.stderr)
        raise


def delete_key(
    key: str,
    host: str = DEFAULT_HOST,
    port: int = DEFAULT_PORT,
    db: int = DEFAULT_DB,
    password: Optional[str] = DEFAULT_PASSWORD,
) -> int:
    """
    删除Redis键

    Args:
        key: Redis键
        host: Redis主机
        port: Redis端口
        db: Redis数据库索引
        password: Redis密码

    Returns:
        int: 删除的键数量
    """
    try:
        client = create_redis_client(host, port, db, password)
        result = client.delete(key)
        return result
    except Exception as e:
        print(f"ERROR: Failed to delete key '{key}': {str(e)}", file=sys.stderr)
        raise


def list_keys(
    pattern: str = "*",
    host: str = DEFAULT_HOST,
    port: int = DEFAULT_PORT,
    db: int = DEFAULT_DB,
    password: Optional[str] = DEFAULT_PASSWORD,
) -> List[str]:
    """
    列出匹配模式的Redis键

    Args:
        pattern: 键模式
        host: Redis主机
        port: Redis端口
        db: Redis数据库索引
        password: Redis密码

    Returns:
        List[str]: 匹配的键列表
    """
    try:
        client = create_redis_client(host, port, db, password)
        keys = client.keys(pattern)
        return keys
    except Exception as e:
        print(
            f"ERROR: Failed to list keys with pattern '{pattern}': {str(e)}",
            file=sys.stderr,
        )
        raise


def get_hash(
    key: str,
    field: Optional[str] = None,
    host: str = DEFAULT_HOST,
    port: int = DEFAULT_PORT,
    db: int = DEFAULT_DB,
    password: Optional[str] = DEFAULT_PASSWORD,
) -> Union[Dict[str, str], Optional[str]]:
    """
    获取Redis哈希表的字段值

    Args:
        key: Redis哈希表键
        field: 哈希表字段，如果为None则获取整个哈希表
        host: Redis主机
        port: Redis端口
        db: Redis数据库索引
        password: Redis密码

    Returns:
        Union[Dict[str, str], Optional[str]]:
            如果field为None，返回整个哈希表；
            否则返回指定字段的值，如果字段不存在则返回None
    """
    try:
        client = create_redis_client(host, port, db, password)
        if field is None:
            result = client.hgetall(key)
            return result
        else:
            result = client.hget(key, field)
            return result
    except Exception as e:
        print(f"ERROR: Failed to get hash for key '{key}': {str(e)}", file=sys.stderr)
        raise


def set_hash(
    key: str,
    field: str,
    value: str,
    host: str = DEFAULT_HOST,
    port: int = DEFAULT_PORT,
    db: int = DEFAULT_DB,
    password: Optional[str] = DEFAULT_PASSWORD,
) -> int:
    """
    设置Redis哈希表的字段值

    Args:
        key: Redis哈希表键
        field: 哈希表字段
        value: 要设置的值
        host: Redis主机
        port: Redis端口
        db: Redis数据库索引
        password: Redis密码

    Returns:
        int: 1表示新字段被创建，0表示字段被更新
    """
    try:
        client = create_redis_client(host, port, db, password)
        result = client.hset(key, field, value)
        return result
    except Exception as e:
        print(
            f"ERROR: Failed to set hash field '{field}' for key '{key}': {str(e)}",
            file=sys.stderr,
        )
        raise


def get_list(
    key: str,
    start: int = 0,
    end: int = -1,
    host: str = DEFAULT_HOST,
    port: int = DEFAULT_PORT,
    db: int = DEFAULT_DB,
    password: Optional[str] = DEFAULT_PASSWORD,
) -> List[str]:
    """
    获取Redis列表的元素

    Args:
        key: Redis列表键
        start: 起始索引
        end: 结束索引
        host: Redis主机
        port: Redis端口
        db: Redis数据库索引
        password: Redis密码

    Returns:
        List[str]: 列表元素
    """
    try:
        client = create_redis_client(host, port, db, password)
        result = client.lrange(key, start, end)
        return result
    except Exception as e:
        print(f"ERROR: Failed to get list for key '{key}': {str(e)}", file=sys.stderr)
        raise


def push_to_list(
    key: str,
    value: str,
    left: bool = False,
    host: str = DEFAULT_HOST,
    port: int = DEFAULT_PORT,
    db: int = DEFAULT_DB,
    password: Optional[str] = DEFAULT_PASSWORD,
) -> int:
    """
    向Redis列表添加元素

    Args:
        key: Redis列表键
        value: 要添加的值
        left: 是否从左侧添加
        host: Redis主机
        port: Redis端口
        db: Redis数据库索引
        password: Redis密码

    Returns:
        int: 操作后列表的长度
    """
    try:
        client = create_redis_client(host, port, db, password)
        if left:
            result = client.lpush(key, value)
        else:
            result = client.rpush(key, value)
        return result
    except Exception as e:
        print(
            f"ERROR: Failed to push to list for key '{key}': {str(e)}", file=sys.stderr
        )
        raise


def execute_redis_command(args):
    """
    执行Redis命令

    Args:
        args: 命令行参数
    """
    host = args.host
    port = args.port
    db = args.db
    password = args.password

    # 创建Redis客户端
    client = create_redis_client(host, port, db, password)

    # 获取命令和参数
    command = args.command
    command_args = args.args if hasattr(args, "args") else []

    try:
        # 执行命令
        if command == "get":
            if len(command_args) != 1:
                print("ERROR: GET command requires exactly one key", file=sys.stderr)
                return
            result = client.get(command_args[0])
            if result is not None:
                print(result)
            else:
                print("(nil)")

        elif command == "set":
            if len(command_args) < 2:
                print(
                    "ERROR: SET command requires at least key and value",
                    file=sys.stderr,
                )
                return
            key = command_args[0]
            value = command_args[1]

            # 检查是否有EX参数
            ex = None
            if len(command_args) > 3 and command_args[2].upper() == "EX":
                try:
                    ex = int(command_args[3])
                except ValueError:
                    print(
                        f"ERROR: Invalid expiration time: {command_args[3]}",
                        file=sys.stderr,
                    )
                    return

            result = client.set(key, value, ex=ex)
            print("OK" if result else "Failed")

        elif command == "del":
            if not command_args:
                print("ERROR: DEL command requires at least one key", file=sys.stderr)
                return
            result = client.delete(*command_args)
            print(f"Deleted {result} key(s)")

        elif command == "keys":
            if len(command_args) != 1:
                print(
                    "ERROR: KEYS command requires exactly one pattern", file=sys.stderr
                )
                return
            pattern = command_args[0]
            keys = client.keys(pattern)
            for key in keys:
                print(key)

        elif command == "hget":
            if len(command_args) != 2:
                print(
                    "ERROR: HGET command requires exactly key and field",
                    file=sys.stderr,
                )
                return
            key = command_args[0]
            field = command_args[1]
            result = client.hget(key, field)
            if result is not None:
                print(result)
            else:
                print("(nil)")

        elif command == "hgetall":
            if len(command_args) != 1:
                print(
                    "ERROR: HGETALL command requires exactly one key", file=sys.stderr
                )
                return
            key = command_args[0]
            result = client.hgetall(key)
            if result:
                for field, value in result.items():
                    print(f"{field}: {value}")
            else:
                print("(empty hash)")

        elif command == "hset":
            if len(command_args) != 3:
                print(
                    "ERROR: HSET command requires exactly key, field and value",
                    file=sys.stderr,
                )
                return
            key = command_args[0]
            field = command_args[1]
            value = command_args[2]
            result = client.hset(key, field, value)
            print(f"{'Created' if result == 1 else 'Updated'} field '{field}'")

        elif command == "lrange":
            if len(command_args) < 1 or len(command_args) > 3:
                print(
                    "ERROR: LRANGE command requires key and optionally start and stop",
                    file=sys.stderr,
                )
                return
            key = command_args[0]
            start = 0
            end = -1

            if len(command_args) > 1:
                try:
                    start = int(command_args[1])
                except ValueError:
                    print(
                        f"ERROR: Invalid start index: {command_args[1]}",
                        file=sys.stderr,
                    )
                    return

            if len(command_args) > 2:
                try:
                    end = int(command_args[2])
                except ValueError:
                    print(
                        f"ERROR: Invalid end index: {command_args[2]}", file=sys.stderr
                    )
                    return

            result = client.lrange(key, start, end)
            for i, value in enumerate(result, start=start):
                print(f"{i}: {value}")

        elif command == "lpush" or command == "rpush":
            if len(command_args) < 2:
                print(
                    f"ERROR: {command.upper()} command requires key and at least one value",
                    file=sys.stderr,
                )
                return
            key = command_args[0]
            values = command_args[1:]

            if command == "lpush":
                result = client.lpush(key, *values)
            else:
                result = client.rpush(key, *values)

            print(f"List length after push: {result}")

        else:
            print(f"ERROR: Unsupported command: {command}", file=sys.stderr)

    except Exception as e:
        print(
            f"ERROR: Failed to execute command '{command}': {str(e)}", file=sys.stderr
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Redis Command Line Tool")
    parser.add_argument("--host", default=DEFAULT_HOST, help="Redis host")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT, help="Redis port")
    parser.add_argument(
        "--db", type=int, default=DEFAULT_DB, help="Redis database index"
    )
    parser.add_argument("--password", help="Redis password")

    # 添加命令参数
    parser.add_argument(
        "command",
        help="Redis command to execute (get, set, del, keys, hget, hgetall, hset, lrange, lpush, rpush)",
    )
    parser.add_argument("args", nargs="*", help="Command arguments")

    args = parser.parse_args()

    execute_redis_command(args)
