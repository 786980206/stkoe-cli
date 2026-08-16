"""sample 模块：样本池（add/get/meta/list/set/check/delete，无物化，走 GraphService）。

样本池 = fieldset 视图 ∩ 指定 index 键集合（`sample add <name> <fieldset> <index>`，
按 (symbol, datetime) 键筛选，不再支持公式过滤；过滤引擎已随公式废弃删除）。

任务版入口见 sample/handlers.py（TaskHandler）。
"""
