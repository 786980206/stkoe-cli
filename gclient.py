#!/usr/bin/env python3
"""stkoe gRPC 测试客户端（单文件 REPL）

用法：
    python gclient.py [host:port]       # 缺省从 ~/.stkoe/stkoe.json 读 grpc-host/grpc-port

REPL 输入（前缀 : 命令形态）：
    h                                Health 探活
    e:config show                    Execute（source action args...，JSON/表格打印）
    e:table list
    e:table meta demo
    s:mock                           SubmitTask（提交后台任务，自动订阅到终态）
    s:table add demo
    c:<task_id> cancel               TaskControl（cancel / pause / resume）
    t:<task_id>                      SubscribeTask（回放订阅已提交任务）
    q / exit                         退出
"""
from __future__ import annotations

import io
import shlex
import sys

import grpc

from stkoe.grpc import stkoe_pb2, stkoe_pb2_grpc


def default_addr() -> str:
    """缺省地址：从配置读 grpc-host/grpc-port，无配置回退 127.0.0.1:9569"""
    try:
        from stkoe.settings import load_config

        cfg = load_config()
        return f"{cfg.grpc_host}:{cfg.grpc_port}"
    except Exception:
        return "127.0.0.1:9569"


# ---------- 输出 ----------

def _print_json(data: str) -> None:
    try:
        from stkoe.jsonutil import loads

        print("  json:", data)
    except Exception:
        print("  json:", data)


def _print_table(data: bytes) -> None:
    import pyarrow as pa
    import polars as pl

    reader = pa.ipc.open_stream(pa.BufferReader(pa.py_buffer(data)))
    table = reader.read_all()
    df = pl.from_arrow(table)
    print(df)


# ---------- RPC 分支 ----------

def do_health(stub) -> None:
    resp = stub.Health(stkoe_pb2.HealthRequest())
    print(f"  status={resp.status} version={resp.version}")


def do_execute(stub, words: list[str]) -> None:
    source, action, args = words[0], words[1] if len(words) > 1 else "", words[2:]
    for r in stub.Execute(stkoe_pb2.ExecuteRequest(source=source, action=action, args=args)):
        kind = r.WhichOneof("type")
        if kind == "header":
            print(f"  header: code={r.header.code} message={r.header.message!r}")
        elif kind == "json":
            print(f"  json[{r.json.name}]: {r.json.data}")
        elif kind == "table":
            print(f"  table[{r.table.name}]:")
            if r.table.meta:
                print(f"    meta: {r.table.meta}")
            _print_table(r.table.data)
        else:
            print(f"  unknown message: {r}")


def do_submit(stub, words: list[str]) -> None:
    source, action, args = words[0], words[1] if len(words) > 1 else "", words[2:]
    resp = stub.SubmitTask(stkoe_pb2.SubmitTaskRequest(source=source, action=action, args=args))
    if resp.header.code != 0:
        print(f"  header: code={resp.header.code} message={resp.header.message!r}")
        return
    task_id = resp.task_id
    print(f"  task_id={task_id}")
    do_subscribe(stub, [task_id])


def do_subscribe(stub, words: list[str]) -> None:
    if not words:
        print("  usage: t:<task_id>")
        return
    task_id = words[0]
    for r in stub.SubscribeTask(stkoe_pb2.SubscribeTaskRequest(task_id=task_id, replay=True)):
        kind = r.WhichOneof("type")
        if kind == "header":
            print(f"  header: code={r.header.code} message={r.header.message!r}")
        elif kind == "event":
            ev = r.event
            tail = f" data={ev.data}" if ev.data else ""
            print(f"  [{ev.seq}] state={ev.state} progress={ev.progress:.2f} "
                  f"msg={ev.message!r}{tail}")


def do_control(stub, words: list[str]) -> None:
    if len(words) < 2:
        print("  usage: c:<task_id> cancel|pause|resume")
        return
    task_id, action = words[0], words[1]
    resp = stub.TaskControl(stkoe_pb2.TaskControlRequest(task_id=task_id, action=action))
    print(f"  header: code={resp.header.code} message={resp.header.message!r}")


# ---------- REPL ----------

_HELP = """可用命令：
  h                                   Health 探活
  e:<source> <action> [args...]       Execute（如 e:config show）
  s:<source> <action> [args...]       SubmitTask（自动订阅到终态）
  c:<task_id> cancel|pause|resume     TaskControl
  t:<task_id>                         SubscribeTask
  q / exit                            退出"""


def repl(stub) -> None:
    print("stkoe gRPC client. 输入 h 查看帮助，q 退出。")
    while True:
        try:
            line = input("stkoe> ").strip()
        except (EOFError, KeyboardInterrupt):
            print()
            return
        if not line:
            continue
        if line in ("q", "quit", "exit"):
            return
        if line in ("help", "?"):
            print(_HELP)
            continue
        if line == "h":
            do_health(stub)
            continue

        prefix, _, rest = line.partition(":")
        rest = rest.strip()
        if prefix == "h" and not rest:
            do_health(stub)
            continue
        try:
            words = shlex.split(rest) if rest else []
        except ValueError as e:
            print(f"  parse error: {e}")
            continue
        try:
            if prefix == "e":
                do_execute(stub, words)
            elif prefix == "s":
                do_submit(stub, words)
            elif prefix == "t":
                do_subscribe(stub, words)
            elif prefix == "c":
                do_control(stub, words)
            else:
                print(f"  unknown prefix {prefix!r}（用 e:/s:/c:/t:/h）")
        except grpc.RpcError as e:
            print(f"  rpc error: {e.code()} {e.details()}")


def main(argv: list[str] | None = None) -> int:
    argv = argv if argv is not None else sys.argv[1:]
    addr = argv[0] if argv else default_addr()
    with grpc.insecure_channel(addr) as ch:
        stub = stkoe_pb2_grpc.StkoeServiceStub(ch)
        repl(stub)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
