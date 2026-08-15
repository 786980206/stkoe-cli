"""结果存储：task/<task_id>/<name> 大结果文件（Arrow / Parquet / 临时文件）

Task 只保存 ``result_ref``（相对 data_dir 的路径），大结果不落地 Task 内存。
"""
from __future__ import annotations

from pathlib import Path


class ResultStore:
    def __init__(self, root: Path):
        self.root = root  # <data_dir>/task

    def dir(self, task_id: str) -> Path:
        return self.root / task_id

    def put(self, task_id: str, name: str, data: bytes) -> str:
        """写入结果文件，返回 result_ref（相对 data_dir）"""
        d = self.dir(task_id)
        d.mkdir(parents=True, exist_ok=True)
        p = d / name
        p.write_bytes(data)
        return f"task/{task_id}/{name}"

    def resolve(self, result_ref: str) -> Path:
        """把 result_ref 解析为绝对路径（相对 data_dir）"""
        return self.root.parent / result_ref

    def load(self, result_ref: str) -> bytes:
        return self.resolve(result_ref).read_bytes()
