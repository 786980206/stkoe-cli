"""factor_tester 模块：因子测试数据集（tester add/update + stat testers，走 GraphService）。

任务版入口见 factor_tester/handlers.py（TaskHandler）；测试器见 factor_tester/tester.py。
"""
from .spec import FactorTesterSpec

__all__ = ["FactorTesterSpec"]
