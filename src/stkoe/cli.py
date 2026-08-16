"""stkoe 命令行入口

命令：
- ``stkoe serve [--host H] [--port P] [--config <路径>]``：前台运行 gRPC 服务
  （缺省取 stkoe.json 配置；``--config`` 显式指定配置文件，等价于设 STKOE_CONFIG）
- ``stkoe config get``：查看生效配置
- ``stkoe config set --<key> <value> ...``：设置任意配置项（写入 stkoe.json，
  键名保持连字符形态，如 ``--grpc-host 0.0.0.0`` → ``"grpc-host": "0.0.0.0"``）
"""
from __future__ import annotations

import os
import sys

from .args import parse_flags
from .jsonutil import dumps_str


def _print_json(obj) -> None:
    print(dumps_str(obj))


def _apply_config_flag(kv: dict) -> None:
    """``--config <路径>``：serve 启动时指定配置文件（等价于设 STKOE_CONFIG 环境变量）"""
    if kv.get("config"):
        os.environ["STKOE_CONFIG"] = str(kv["config"])


def _cmd_serve(raw: list[str]) -> int:
    from .grpc.server import serve
    from .logutil import LOG

    kv = parse_flags(raw)
    _apply_config_flag(kv)
    host = kv.get("host")
    port = int(kv["port"]) if kv.get("port") else None
    srv = serve(host=host, port=port)
    print(f"stkoe gRPC listening on {srv.host}:{srv.port}")
    try:
        srv.wait()
    except KeyboardInterrupt:
        LOG.info("收到 Ctrl+C，正在停止服务...")
    finally:
        srv.stop()  # 优雅退出：停 gRPC + TaskManager（幂等）
    return 0


def _cmd_config(raw: list[str]) -> int:
    from .settings import config_path, load_config, save_config

    if not raw or raw[0] == "get":
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


def _cmd_index(raw: list[str]) -> int:
    return _cmd_dispatch("index", raw)


def _cmd_panel(raw: list[str]) -> int:
    return _cmd_dispatch("panel", raw)


def _cmd_stat(raw: list[str]) -> int:
    return _cmd_dispatch("stat", raw)


def _cmd_fieldset(raw: list[str]) -> int:
    return _cmd_dispatch("fieldset", raw)


def _cmd_sample(raw: list[str]) -> int:
    return _cmd_dispatch("sample", raw)


def _cmd_feature(raw: list[str]) -> int:
    return _cmd_dispatch("feature", raw)


def _cmd_factor(raw: list[str]) -> int:
    return _cmd_dispatch("factor", raw)


def _cmd_test(raw: list[str]) -> int:
    return _cmd_dispatch("test", raw)


def _cmd_task(raw: list[str]) -> int:
    return _cmd_dispatch("task", raw)


def _cmd_mock(raw: list[str]) -> int:
    return _cmd_dispatch("mock", raw)


def _cmd_graph(raw: list[str]) -> int:
    return _cmd_dispatch("graph", raw)


def _help() -> str:
    return (
        "用法: stkoe <command>\n"
        "  serve [--host H] [--port P] [--config <路径>]  运行 gRPC 服务（缺省取 stkoe.json 配置；--config 指定配置文件）\n"
        "  config get                     查看生效配置\n"
        "  config set --<key> <value> ...  设置任意配置项（写入 stkoe.json）\n"
        "  table <action> <args...>        table 命令（list/meta/add/set/get/delete，与 Execute 对齐）\n"
        "  index <action> <args...>        index 命令（add/get/meta/list/set/col/update/delete；独立资产）\n"
        "  panel <action> <args...>        panel 命令（add/get/meta/list/set/update/delete）\n"
        "  stat <action> <args...>         stat 命令（scan/get/meta/list/delete）\n"
        "  fieldset <action> <args...>     fieldset 命令（add/get/meta/list/set/update/delete/check/test）\n"
        "  sample <action> <args...>       sample 命令（add/get/meta/list/set/check/delete；无物化）\n"
        "  feature <action> <args...>     feature 命令（add/set/meta/list/delete/test；纯定义，无物化）\n"
        "  factor <action> <args...>      factor 命令（add/get/meta/list/set/check/update/delete；可物化）\n"
        "  test <action> <args...>        test 命令（add/get/meta/list/set/check/update/delete；因子测试数据集）\n"
        "  mock demo [--n-syms N] [--n-days N] 生成演示源表 index + m1（默认 300×500，写 index/ + table/，需 index add/table add 注册）\n"
        "  mock gen <name> --kind <kind>  参数化生成单张表（tdcal/common/index/feature/klday/m1）\n"
        "  task list [--state <state>]     任务列表（按创建时间倒序）\n"
        "  graph lineage [--node <type:name>] [--depth N]  血缘图（Cytoscape elements JSON）\n"
        "  graph nodes [--type <t>]        节点摘要列表\n"
        "  graph stats                     节点/边统计"
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
    if cmd == "index":
        return _cmd_index(args[1:])
    if cmd == "panel":
        return _cmd_panel(args[1:])
    if cmd == "stat":
        return _cmd_stat(args[1:])
    if cmd == "fieldset":
        return _cmd_fieldset(args[1:])
    if cmd == "sample":
        return _cmd_sample(args[1:])
    if cmd == "feature":
        return _cmd_feature(args[1:])
    if cmd == "factor":
        return _cmd_factor(args[1:])
    if cmd == "test":
        return _cmd_test(args[1:])
    if cmd == "task":
        return _cmd_task(args[1:])
    if cmd == "mock":
        return _cmd_mock(args[1:])
    if cmd == "graph":
        return _cmd_graph(args[1:])
    print(f"未知命令: {cmd}\n{_help()}")
    return 1
