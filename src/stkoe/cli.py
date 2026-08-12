"""stkoe 命令行入口

命令：
- ``stkoe serve [--host H] [--port P]``：前台运行 gRPC 服务（缺省取 stkoe.json 配置）
- ``stkoe config show``：查看生效配置
- ``stkoe config set --<key> <value> ...``：设置任意配置项（写入 stkoe.json，
  键名保持连字符形态，如 ``--grpc-host 0.0.0.0`` → ``"grpc-host": "0.0.0.0"``）
"""
from __future__ import annotations

import sys

from .args import parse_flags
from .jsonutil import dumps_str


def _print_json(obj) -> None:
    print(dumps_str(obj))


def _cmd_serve(raw: list[str]) -> int:
    from .grpc.server import serve

    kv = parse_flags(raw)
    host = kv.get("host")
    port = int(kv["port"]) if kv.get("port") else None
    srv = serve(host=host, port=port)
    print(f"stkoe gRPC listening on {srv.host}:{srv.port}")
    srv.wait()
    return 0


def _cmd_config(raw: list[str]) -> int:
    from .settings import config_path, load_config, save_config

    if not raw or raw[0] == "show":
        cfg = load_config()
        _print_json({"config_file": str(config_path()), **cfg.to_dict()})
        return 0
    if raw[0] == "set":
        kv = parse_flags(raw[1:])
        if not kv:
            print("用法: stkoe config set --<key> <value> ...")
            return 2
        path = save_config(kv)
        _print_json({"written": str(path), "set": kv})
        return 0
    print(f"未知 config 子命令: {raw[0]}")
    return 1


def _cmd_dispatch(source: str, raw: list[str]) -> int:
    """通用子命令：``stkoe <source> <action> <args...>``，走 Execute 同步分发"""
    from .grpc.dispatch import CommandError, dispatch

    action = raw[0] if raw and not raw[0].startswith("--") else ""
    args = raw[1:] if action else raw
    try:
        results = dispatch(source, action, args)
    except CommandError as e:
        print(e.message)
        return e.code
    except Exception as e:
        print(str(e))
        return 2
    for r in results:
        if r.kind == "table":
            print(f"<table {r.name}: {len(r.data)} 字节 IPC>")
        else:
            print(r.data)
    return 0


def _cmd_table(raw: list[str]) -> int:
    return _cmd_dispatch("table", raw)


def _cmd_dataset(raw: list[str]) -> int:
    return _cmd_dispatch("dataset", raw)


def _cmd_stat(raw: list[str]) -> int:
    return _cmd_dispatch("stat", raw)


def _cmd_task(raw: list[str]) -> int:
    return _cmd_dispatch("task", raw)


def _help() -> str:
    return (
        "用法: stkoe <command>\n"
        "  serve [--host H] [--port P]     运行 gRPC 服务（缺省取 stkoe.json 配置）\n"
        "  config show                     查看生效配置\n"
        "  config set --<key> <value> ...  设置任意配置项（写入 stkoe.json）\n"
        "  table <action> <args...>        table 命令（list/meta/add/set/get/delete，与 Execute 对齐）\n"
        "  dataset <action> <args...>      dataset 命令（add/get/meta/list/set/scan/delete）\n"
        "  stat <action> <args...>         stat 命令（scan/get/meta/list/delete）\n"
        "  task list [--state <state>]     任务列表（按创建时间倒序）"
    )


def main(argv: list[str] | None = None) -> int:
    args = list(sys.argv[1:] if argv is None else argv)
    if not args:
        print(_help())
        return 0
    cmd = args[0]
    if cmd in ("-h", "--help", "help"):
        print(_help())
        return 0
    if cmd == "serve":
        return _cmd_serve(args[1:])
    if cmd == "config":
        return _cmd_config(args[1:])
    if cmd == "table":
        return _cmd_table(args[1:])
    if cmd == "dataset":
        return _cmd_dataset(args[1:])
    if cmd == "stat":
        return _cmd_stat(args[1:])
    if cmd == "task":
        return _cmd_task(args[1:])
    print(f"未知命令: {cmd}\n{_help()}")
    return 1
