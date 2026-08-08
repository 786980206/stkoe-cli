"""stkoe 命令行入口

- `python -m stkoe <命令>`：单次执行（如 `python -m stkoe table scan demo`）
- `python -m stkoe`：进入交互式 CLI（Tab 补全 + 实时提示），`exit`/`quit` 退出

REPL 下默认后台执行（返回 TaskHandle，可用 `task list/log` 观察）；
单次命令默认同步（直接打印结果）。
"""
import shlex
import sys

from prompt_toolkit import PromptSession
from prompt_toolkit.completion import Completer, Completion
from prompt_toolkit.shortcuts import CompleteStyle
from typer.main import get_command

from .data import dataset as dataset_mod
from .data import stat as stat_mod
from .data import table
from .data.cli import app
from .data.task import set_default_async

PROMPT = "stkoe> "
EXIT_WORDS = {"exit", "quit", "q", ":q", "bye"}
HELP_WORDS = {"help", "?", ":h"}

TOP_COMMANDS = ["table", "task", "dataset", "stat", "config", "mock", "help", "exit", "quit"]
TABLE_SUBCOMMANDS = ["add", "list", "meta", "get", "del", "rename", "set", "col", "scan"]
TASK_SUBCOMMANDS = ["list", "stop", "pause", "resume", "log", "clean"]
DATASET_SUBCOMMANDS = ["add", "list", "meta", "get", "scan", "del", "rename"]
STAT_SUBCOMMANDS = ["add", "list", "meta", "get", "scan", "del", "rename"]
# 第一个位置参数为表名/dataset 名的子命令（补全时提示已注册对象）
TABLE_NAME_SUBS = {"meta", "get", "del", "set", "col", "scan"}
DATASET_NAME_SUBS = {"meta", "get", "del", "scan"}
STAT_NAME_SUBS = {"meta", "get", "del", "scan", "add"}

_cli = None


def _dispatch(args: list[str]) -> int:
    """把 argv 分派给 typer CLI；返回进程退出码"""
    global _cli
    if _cli is None:
        _cli = get_command(app)
    try:
        rc = _cli.main(args=args, prog_name="stkoe", standalone_mode=False)
        return 0 if rc is None else int(rc)
    except SystemExit:
        return 0
    except Exception as e:
        print(f"错误: {type(e).__name__}: {e}")
        return 1


class _Completer(Completer):
    """上下文感知补全：顶层命令 → 子命令 → 已注册对象名"""

    def __init__(self):
        self._tables: list[str] | None = None
        self._datasets: list[str] | None = None
        self._stats: list[str] | None = None

    def _table_names(self) -> list[str]:
        if self._tables is None:
            try:
                self._tables = sorted(m.name for m in table.list())
            except Exception:
                self._tables = []
        return self._tables

    def _dataset_names(self) -> list[str]:
        if self._datasets is None:
            try:
                self._datasets = sorted(m.name for m in dataset_mod.list())
            except Exception:
                self._datasets = []
        return self._datasets

    def _stat_names(self) -> list[str]:
        if self._stats is None:
            try:
                self._stats = sorted(m.name for m in stat_mod.list())
            except Exception:
                self._stats = []
        return self._stats

    def _yield(self, words: list[str], word: str):
        for w in words:
            if w.startswith(word):
                yield Completion(w, start_position=-len(word))

    def get_completions(self, document, complete_event):
        text = document.text_before_cursor
        word = document.get_word_before_cursor(WORD=True)
        parts = text[: len(text) - len(word)].strip().split()
        if not parts:
            yield from self._yield(TOP_COMMANDS, word)
            return
        cmd = parts[0]
        if cmd == "table":
            if len(parts) == 1:
                yield from self._yield(TABLE_SUBCOMMANDS, word)
            elif len(parts) == 2 and parts[1] in TABLE_NAME_SUBS:
                yield from self._yield(self._table_names(), word)
        elif cmd == "task":
            if len(parts) == 1:
                yield from self._yield(TASK_SUBCOMMANDS, word)
        elif cmd == "dataset":
            if len(parts) == 1:
                yield from self._yield(DATASET_SUBCOMMANDS, word)
            elif len(parts) == 2 and parts[1] in DATASET_NAME_SUBS:
                yield from self._yield(self._dataset_names(), word)
        elif cmd == "stat":
            if len(parts) == 1:
                yield from self._yield(STAT_SUBCOMMANDS, word)
            elif len(parts) == 2 and parts[1] in STAT_NAME_SUBS:
                yield from self._yield(self._stat_names(), word)


_session = None


def _readline(prompt: str) -> str | None:
    """读取一行（TTY 下 Tab 补全 + 实时提示）；EOF/中断返回 None"""
    if not (sys.stdin.isatty() and sys.stdout.isatty()):
        try:
            return input(prompt)
        except (EOFError, KeyboardInterrupt):
            return None
    global _session
    if _session is None:
        _session = PromptSession(
            completer=_Completer(),
            complete_while_typing=True,
            complete_style=CompleteStyle.MULTI_COLUMN,
        )
    try:
        return _session.prompt(prompt)
    except (EOFError, KeyboardInterrupt):
        return None


def repl() -> int:
    """交互式 CLI 提示符（后台执行默认开启，`table scan --sync` 等可覆盖）"""
    set_default_async(True)
    print("stkoe 数据管理 CLI — 输入时 Tab 补全 / 实时提示")
    print(f"命令: {', '.join(TOP_COMMANDS)} | `table --help` 查看子命令 | `exit` 退出")
    print("REPL 下任务默认后台执行（返回 task_id，用 `task log <id>` 观察）")
    while True:
        line = _readline(PROMPT)
        if line is None:
            print()
            return 0
        line = line.strip()
        if not line:
            continue
        if line.lower() in EXIT_WORDS:
            return 0
        if line.lower() in HELP_WORDS:
            _dispatch(["--help"])
            continue
        try:
            _dispatch(shlex.split(line))
        except ValueError as e:
            print(f"语法错误: {e}")


def main(argv: list[str] | None = None) -> int:
    args = list(sys.argv[1:]) if argv is None else list(argv)
    if not args:
        return repl()
    return _dispatch(args)


if __name__ == "__main__":
    sys.exit(main())
