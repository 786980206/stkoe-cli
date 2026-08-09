"""CLI 冒烟测试（新动词形态：table/dataset/stat 统一 add/meta/get/scan/del）"""
import sys
import time

import pytest

import orjson
import polars as pl
from typer.testing import CliRunner

import stkoe.data as data
from stkoe import __main__ as mainmod
from stkoe.data.cli import app
from stkoe.data.table import DependencyError
from stkoe.data.task import run_task

from conftest import make_df, write_single

runner = CliRunner()


@pytest.fixture(autouse=True)
def _reset_async_default():
    """v0.5.0 起所有操作默认同步（无全局异步模式），fixture 仅作占位防顺序耦合"""
    yield


def _setup_pair(root):
    write_single(root, "idx", pl.DataFrame({
        "date": ["2020-01-01", "2020-01-02"], "sym": ["a", "b"]
    }).with_columns(pl.col("date").str.strptime(pl.Date, "%Y-%m-%d")))
    write_single(root, "m1", pl.DataFrame({
        "date": ["2020-01-01", "2020-01-02"], "sym": ["a", "b"], "extra": [1, 2]
    }).with_columns(pl.col("date").str.strptime(pl.Date, "%Y-%m-%d")))
    data.table.scan("idx")
    data.table.scan("m1")


def test_cli_table_add_list_meta(root):
    write_single(root, "t1", make_df([("2020-01-01", "a", 1.0)]))
    r = runner.invoke(app, ["table", "scan", "t1"])
    assert r.exit_code == 0, r.stdout
    j = orjson.loads(r.stdout)
    assert j["name"] == "t1" and j["version_after"] == 1

    r = runner.invoke(app, ["table", "list"])
    assert r.exit_code == 0 and "t1" in r.stdout

    r = runner.invoke(app, ["table", "meta", "t1"])
    assert r.exit_code == 0, r.stdout
    j = orjson.loads(r.stdout.encode())
    assert j["name"] == "t1" and j["layout"] == "single"
    assert [c["name"] for c in j["columns"]] == ["date", "sym", "r"]

    r = runner.invoke(app, ["table", "meta", "nope"])
    assert r.exit_code != 0  # 未注册 → 报错


def test_cli_table_get(root):
    write_single(root, "t1", make_df([("2020-01-01", "a", 1.0), ("2020-01-02", "b", 2.0)]))
    data.table.scan("t1")
    r = runner.invoke(app, ["table", "get", "t1", "--where", "date>=2020-01-02"])
    assert r.exit_code == 0 and "b" in r.stdout


def test_cli_table_set_rename_del(root):
    write_single(root, "t1", make_df([("2020-01-01", "a", 1.0)]))
    data.table.scan("t1")
    r = runner.invoke(app, ["table", "set", "t1", "--display-name", "hi"])
    assert r.exit_code == 0 and orjson.loads(r.stdout)["display_name"] == "hi"
    r = runner.invoke(app, ["table", "rename", "t1", "t1b"])
    assert r.exit_code == 0 and orjson.loads(r.stdout)["name"] == "t1b"
    r = runner.invoke(app, ["table", "del", "t1b"])
    assert r.exit_code == 0 and orjson.loads(r.stdout) == {"deleted": "t1b"}
    r = runner.invoke(app, ["table", "list"])
    assert r.exit_code == 0 and "t1b" not in r.stdout
    assert (root / "tables" / "t1b" / "t1.parquet").exists()  # 数据文件保留


def test_cli_dataset(root):
    _setup_pair(root)
    r = runner.invoke(app, ["dataset", "add", "ds", "idx", "m1"])
    assert r.exit_code == 0, r.stdout
    assert orjson.loads(r.stdout)["name"] == "ds"

    r = runner.invoke(app, ["dataset", "list"])
    assert r.exit_code == 0 and "ds" in r.stdout

    r = runner.invoke(app, ["dataset", "list"])
    assert r.exit_code == 0, r.stdout
    j = orjson.loads(r.stdout)
    assert isinstance(j, list) and any(d["name"] == "ds" for d in j)

    r = runner.invoke(app, ["dataset", "meta", "ds"])
    assert r.exit_code == 0, r.stdout
    dj = orjson.loads(r.stdout)
    assert dj["name"] == "ds" and dj["index_table"] == "idx" and dj["keys"] == ["date", "sym"]

    r = runner.invoke(app, ["dataset", "get", "ds", "--where", "sym==b"])
    assert r.exit_code == 0 and "b" in r.stdout

    r = runner.invoke(app, ["dataset", "scan", "ds"])
    assert r.exit_code == 0 and orjson.loads(r.stdout)["changed"] is False

    r = runner.invoke(app, ["stat", "get", "ds"])
    assert r.exit_code == 0 and "field" in r.stdout

    r = runner.invoke(app, ["dataset", "del", "ds", "--force"])
    assert r.exit_code == 0
    assert not (root / "datasets" / "ds").exists()


def test_cli_stat(root):
    _setup_pair(root)
    runner.invoke(app, ["dataset", "add", "ds", "idx", "m1"])
    r = runner.invoke(app, ["stat", "add", "ds"])
    assert r.exit_code == 0, r.stdout
    r = runner.invoke(app, ["stat", "meta", "ds"])
    assert r.exit_code == 0
    j = orjson.loads(r.stdout.encode())
    assert j["target_type"] == "dataset" and j["groups"] == ["all"]
    r = runner.invoke(app, ["stat", "list"])
    assert r.exit_code == 0 and "ds" in r.stdout
    r = runner.invoke(app, ["stat", "del", "ds"])
    assert r.exit_code == 0
    assert not (root / "stats" / "ds").exists()


def test_main_direct(root, capsys):
    """python -m stkoe <命令>：单次执行"""
    write_single(root, "t1", make_df([("2020-01-01", "a", 1.0)]))
    rc = mainmod.main(["table", "scan", "t1"])
    assert rc == 0
    rc = mainmod.main(["table", "meta", "t1"])
    assert rc == 0
    assert "t1" in capsys.readouterr().out


def test_cli_task_clean(root):
    run_task("cli_clean", "obj", lambda c, k: None)
    r = runner.invoke(app, ["task", "clean"])
    assert r.exit_code == 0, r.stdout
    assert orjson.loads(r.stdout)["cleaned"] == 1
    assert not data.task_list(type="cli_clean")


def test_cli_task_stop_all(root):
    def _loop(conn, ctl):
        while True:
            ctl.check()
            time.sleep(0.02)

    h = run_task("cli_stopall", "obj", _loop, background=True)
    deadline = time.time() + 10
    while not data.task_list(status="running", type="cli_stopall") and time.time() < deadline:
        time.sleep(0.02)

    r = runner.invoke(app, ["task", "stop", "--all"])
    assert r.exit_code == 0, r.stdout
    assert orjson.loads(r.stdout) == {"stopped": 1, "cleaned": 1}

    assert not data.task_list(type="cli_stopall")


def test_main_repl(root, capsys, monkeypatch):
    """python -m stkoe：交互 REPL，提示符下执行 table xxx"""
    write_single(root, "t1", make_df([("2020-01-01", "a", 1.0)]))
    data.table.scan("t1")
    inputs = iter(["table list", "badcmd", "exit"])
    monkeypatch.setattr(mainmod, "_readline", lambda _prompt: next(inputs))
    rc = mainmod.main([])
    assert rc == 0
    out = capsys.readouterr().out
    assert "t1" in out and "No such command" in out


def test_main_repl_eof(root, capsys, monkeypatch):
    """REPL 遇 EOF/Ctrl+C 正常退出"""
    monkeypatch.setattr(mainmod, "_readline", lambda _prompt: None)
    rc = mainmod.main([])
    assert rc == 0


def test_completer_table_names(root):
    """补全器：table 子命令 + 已注册表名提示"""
    write_single(root, "t1", make_df([("2020-01-01", "a", 1.0)]))
    data.table.scan("t1")
    c = mainmod._Completer()
    docs = [
        ("", "tab"),
        ("table me", "me"),
        ("table meta t", "t"),
        ("table list", "list"),
    ]
    for text, word in docs:
        comps = list(c.get_completions(
            _FakeDocument(text, word), None))
        assert comps, f"no completions for {text!r}"


class _FakeDocument:
    """最小 document 替身（构造兼容补全器）"""

    def __init__(self, text_before_cursor: str, word: str):
        self.text_before_cursor = text_before_cursor
        self._word = word

    def get_word_before_cursor(self, WORD=True):
        return self._word


def test_readline_tty(monkeypatch):
    """TTY 路径：PromptSession 补全读行；DummyInput EOF → 返回 None"""
    from prompt_toolkit import PromptSession
    from prompt_toolkit.input import DummyInput
    from prompt_toolkit.output import DummyOutput
    from prompt_toolkit.shortcuts import CompleteStyle

    monkeypatch.setattr(sys.stdin, "isatty", lambda: True)
    monkeypatch.setattr(sys.stdout, "isatty", lambda: True)
    mainmod._session = PromptSession(
        input=DummyInput(),
        output=DummyOutput(),
        completer=mainmod._Completer(),
        complete_while_typing=True,
        complete_style=CompleteStyle.MULTI_COLUMN,
    )
    assert mainmod._readline(mainmod.PROMPT) is None


def test_dispatch_error(root):
    """错误命令返回退出码 1"""
    rc = mainmod._dispatch(["table", "meta", "nope"])
    assert rc == 1


def test_cli_del_sync_dependency_error(root, capsys):
    """table/dataset del 默认同步抛 DependencyError（含依赖结构）。

    v0.5.0 起所有操作默认同步（无全局异步模式），删除必须当场返回依赖信息，
    绝不落入后台任务。"""
    _setup_pair(root)
    r = runner.invoke(app, ["dataset", "add", "ds", "idx", "m1"])
    assert r.exit_code == 0, r.stdout

    r = runner.invoke(app, ["table", "del", "idx"])
    assert r.exit_code == 1
    assert isinstance(r.exception, DependencyError)
    assert "dependencies exist" in str(r.exception)
    deps = r.exception.dependents
    assert deps and deps[0]["obj_type"] == "dataset" and deps[0]["obj_name"] == "ds"
    # 表仍在（未被误删）
    r2 = runner.invoke(app, ["table", "list"])
    assert r2.exit_code == 0 and "idx" in r2.stdout

    r = runner.invoke(app, ["dataset", "del", "ds"])
    assert r.exit_code == 0, r.stdout
    r = runner.invoke(app, ["table", "del", "idx"])
    assert r.exit_code == 0, r.stdout


def test_cli_async_flag_returns_task_id(root):
    """--async 显式转后台：返回 task_id，task get 可拉取状态与结果（v0.5.0 统一模型）"""
    _setup_pair(root)
    r = runner.invoke(app, ["table", "scan", "idx", "--async"])
    assert r.exit_code == 0, r.stdout
    task_id = orjson.loads(r.stdout)["task_id"]

    # 轮询完成
    meta = None
    for _ in range(100):
        r2 = runner.invoke(app, ["task", "get", task_id])
        assert r2.exit_code == 0, r2.stdout
        meta = orjson.loads(r2.stdout)
        if meta["status"] in ("succeeded", "failed", "cancelled"):
            break
        time.sleep(0.05)
    assert meta["status"] == "succeeded", meta
    assert meta["result"] is not None  # 结果已持久化
    assert meta["result"]["name"] == "idx"


def test_cli_mock_gen(root):
    r = runner.invoke(app, ["mock", "gen", "mock_tdcal", "--kind", "tdcal"])
    assert r.exit_code == 0, r.stdout
    assert "mock_tdcal" in r.stdout
    assert "mock_tdcal" in {m.name for m in data.table.list()}