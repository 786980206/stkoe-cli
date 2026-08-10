from ..core import Operator
from dataclasses import dataclass
from typing import Any

@dataclass(frozen=True)    
class NothingOperator(Operator):
    def pipeline(self, LastOutput:Any) -> Any:
        """不做任何处理"""
        return LastOutput