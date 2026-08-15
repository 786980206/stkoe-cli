"""版本号生成：高精度时间戳（纳秒），带业务含义且单调递增。

版本号 = 变更时刻的 ``time.time_ns()`` 纳秒时间戳（int），可直接看出变更时间；
同一纳秒或时钟回拨时以上次版本 +1 兜底，保证严格单调（``version > required_version``
的水位线比较成立）。
"""
from __future__ import annotations

import time

_LAST: int = 0


def new_version() -> int:
    """生成下一个版本号（纳秒时间戳，单调兜底）。"""
    global _LAST
    v = time.time_ns()
    if v <= _LAST:
        v = _LAST + 1
    _LAST = v
    return v


__all__ = ["new_version"]
