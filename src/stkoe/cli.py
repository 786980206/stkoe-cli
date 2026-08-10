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


def _help() -> str:
    return (
        "用法: stkoe <command>\n"
        "  serve [--host H] [--port P]     运行 gRPC 服务（缺省取 stkoe.json 配置）\n"
        "  config show                     查看生效配置\n"
        "  config set --<key> <value> ...  设置任意配置项（写入 stkoe.json）"
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
    print(f"未知命令: {cmd}\n{_help()}")
    return 1
