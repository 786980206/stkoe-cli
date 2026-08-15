"""mock 演示数据生成模块：stkoe mock 接口（替代 scripts/gen_example_data.py）

- ``stkoe mock demo``：生成 example.md 演示源表 index + m1
- ``stkoe mock gen <name> --kind <kind>``：参数化生成单张表
"""
from .gen import (INDUSTRIES, common, demo, demo_index, demo_m1, feature,
                  gen, index, klday, m1, resolve_data_dir, tdcal, write)

__all__ = [
    "INDUSTRIES", "tdcal", "common", "index", "m1", "feature", "klday",
    "demo", "demo_index", "demo_m1", "write", "gen", "resolve_data_dir",
]
