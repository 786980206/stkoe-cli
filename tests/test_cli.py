"""CLI 冒烟测试"""
import sys

import orjson
import polars as pl
from typer.testing import CliRunner

import stkoe.data as data
from stkoe import __main__ as mainmod
from stkoe.data.cli import app
from stkoe.data.task import run_task

from conftest import make_df, write_single

runner = CliRunner()


def test_cli_sniff_list(root):
    write_single(root, "t1", make_df([("2020-01-01", "a", 1.0)]))
    r = runner.invoke(app, ["table", "sniff", "t1"])
    assert r.exit_code == 0
    assert "t1" in r.stdout and "changed=True" in r.stdout

    r = runner.invoke(app, ["table", "list"])
    assert r.exit_code == 0 and "t1" in r.stdout


def test_cli_describe_json(root):
    write_single(root, "t1", make_df([("2020-01-01", "a", 1.0)]))
    data.sniff("t1")
    r = runner.invoke(app, ["table", "describe", "t1", "--json"])
    assert r.exit_code == 0
    j = orjson.loads(r.stdout.encode())
    assert j["name"] == "t1" and j["layout"] == "single" and j["row_count"] == 1


def test_cli_status_schema(root):
    write_single(root, "t1", make_df([("2020-01-01", "a", 1.0)]))
    data.sniff("t1")
    r = runner.invoke(app, ["table", "status", "t1"])
    assert r.exit_code == 0 and "consistent: True" in r.stdout
    r = runner.invoke(app, ["table", "schema", "t1"])
    assert r.exit_code == 0 and "date: Date" in r.stdout


def test_cli_select(root):
    write_single(root, "t1", make_df([("2020-01-01", "a", 1.0), ("2020-01-02", "b", 2.0)]))
    data.sniff("t1")
    r = runner.invoke(app, ["table", "select", "t1", "--where", "date>=2020-01-02"])
    assert r.exit_code == 0 and "b" in r.stdout


def test_cli_create_update_drop(root):
    r = runner.invoke(app, ["table", "create", "t2"])
    assert r.exit_code == 0 and "succeeded" in r.stdout
    r = runner.invoke(app, ["table", "update", "t2", "--display-name", "hi", "--bump"])
    assert r.exit_code == 0 and "v2" in r.stdout
    r = runner.invoke(app, ["table", "drop", "t2"])
    assert r.exit_code == 0 and "succeeded" in r.stdout
    r = runner.invoke(app, ["table", "list"])
    assert r.exit_code == 0 and "t2" not in r.stdout  # drop 后不再列出（数据目录还在，读时会隐式重注册）


def test_cli_dataset(root):
    # index 表：仅 date, sym（join 键由 index 定义）
    write_single(root, "t1", pl.DataFrame({
        "date": ["2020-01-01", "2020-01-02"], "sym": ["a", "b"]
    }).with_columns(pl.col("date").str.strptime(pl.Date, "%Y-%m-%d")))
    write_single(root, "m1", pl.DataFrame({
        "date": ["2020-01-01", "2020-01-02"], "sym": ["a", "b"], "extra": [1, 2]
    }).with_columns(pl.col("date").str.strptime(pl.Date, "%Y-%m-%d")))
    data.sniff("t1")
    data.sniff("m1")

    r = runner.invoke(app, ["dataset", "create", "ds", "t1", "m1", "--sync"])
    assert r.exit_code == 0, r.stdout
    assert "succeeded" in r.stdout

    r = runner.invoke(app, ["dataset", "list"])
    assert r.exit_code == 0 and "ds" in r.stdout

    r = runner.invoke(app, ["dataset", "list", "--json"])
    assert r.exit_code == 0, r.stdout
    j = orjson.loads(r.stdout)
    assert isinstance(j, list) and any(d["name"] == "ds" for d in j)

    r = runner.invoke(app, ["dataset", "describe", "ds", "--json"])
    assert r.exit_code == 0, r.stdout
    dj = orjson.loads(r.stdout)
    assert dj["name"] == "ds" and dj["index_table"] == "t1" and dj["keys"] == ["date", "sym"]

    r = runner.invoke(app, ["dataset", "status", "ds"])
    assert r.exit_code == 0 and "consistent:" in r.stdout and "True" in r.stdout

    r = runner.invoke(app, ["dataset", "select", "ds", "--where", "sym==b"])
    assert r.exit_code == 0 and "b" in r.stdout

    r = runner.invoke(app, ["stat", "select", "ds"])
    assert r.exit_code == 0 and "field" in r.stdout

    r = runner.invoke(app, ["dataset", "drop", "ds"])
    assert r.exit_code == 0 and "succeeded" in r.stdout


def test_main_direct(root, capsys):
    """python -m stkoe <命令>：单次执行"""
    write_single(root, "t1", make_df([("2020-01-01", "a", 1.0)]))
    rc = mainmod.main(["table", "sniff", "t1"])
    assert rc == 0
    rc = mainmod.main(["table", "describe", "t1", "--json"])
    assert rc == 0
    assert "t1" in capsys.readouterr().out


def test_cli_task_clean(root):
    run_task("cli_clean", "obj", lambda c, k: None)
    r = runner.invoke(app, ["task", "clean"])
    assert r.exit_code == 0, r.stdout
    assert "cleaned 1 finished task(s)" in r.stdout
    assert not data.task_list(type="cli_clean")


def test_cli_task_stop_all(root):
    def _loop(conn, ctl):
        while True:
            ctl.check()
            time.sleep(0.02)

    import time
    h = run_task("cli_stopall", "obj", _loop, background=True)
    deadline = time.time() + 10
    while not data.task_list(status="running", type="cli_stopall") and time.time() < deadline:
        time.sleep(0.02)

    r = runner.invoke(app, ["task", "stop", "--all"])
    assert r.exit_code == 0, r.stdout
    assert "stop requested: 1 running, cleaned 1 finished task(s)" in r.stdout

    # 已等待收尾并清理完成，不再残留任务记录
    assert not data.task_list(type="cli_stopall")


def test_main_repl(root, capsys, monkeypatch):
    """python -m stkoe：交互 REPL，提示符下执行 table xxx"""
    write_single(root, "t1", make_df([("2020-01-01", "a", 1.0)]))
    data.sniff("t1")
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
    data.sniff("t1")
    c = mainmod._Completer()
    docs = [
        ("", "tab"),
        ("table de", "de"),
        ("table describe t", "t"),
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
    rc = mainmod._dispatch(["table", "describe", "nope"])
    assert rc == 1
