"""全局公共配置"""

from pathlib import Path

# 回测结果存放根目录
RESULTS_DIR: Path = Path(__file__).parent / "results"

# 默认回测结果 ID（按目录名取最新）
DEFAULT_RESULT_ID: str = "20260511112033"
