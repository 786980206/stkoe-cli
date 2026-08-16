"""factor_test 模块：因子测试数据集（test add/scan + stat testers，走 GraphService）。

任务版入口见 factor_test/handlers.py（TaskHandler）；测试器见 factor_test/tester.py。
"""
from .spec import FactorTesterSpec

__all__ = ["FactorTesterSpec"]
