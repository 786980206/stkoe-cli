"""stkoe 日志：统一 logger + serve 入口的日志配置

- ``LOG`` 为包级 logger（名 ``stkoe``），业务模块直接引用
- ``setup_logging()`` 在 ``stkoe serve`` 入口调用：默认 INFO 起步，
  输出 时间/级别/模块/消息；可用 ``STKOE_LOG_LEVEL`` 环境变量覆盖级别
"""
from __future__ import annotations

import logging
import os

LOG = logging.getLogger("stkoe")


def setup_logging() -> None:
    """配置控制台日志（幂等）：默认 INFO，``STKOE_LOG_LEVEL``（DEBUG/INFO/WARNING/...）可覆盖"""
    level = os.environ.get("STKOE_LOG_LEVEL", "INFO").upper()
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )