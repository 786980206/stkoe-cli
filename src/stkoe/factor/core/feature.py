import warnings
from dataclasses import dataclass
from abc import ABC, abstractmethod
import polars as pl
from .builder import FeatureBuilder
from ...data.plugins.wsdata import get_feature_data
from typing import Union, Any

class Operator(ABC):

    def __repr__(self):
        return f"{self.__class__.__name__}()"
    
    def __or__(self, other) -> "Pipeline":
        return Pipeline([self]) | other
    
    def __gt__(self, other) -> "Pipeline":
        return Pipeline([self]) > other

    def __call__(self, LastOutput:Any) -> Any:
        return Pipeline([self])(LastOutput)

    @abstractmethod
    def pipeline(self, LastOutput:Any) -> Any:
        """管道处理"""

@dataclass(frozen=True)
class RenameOperator(Operator):
    name: str

    def pipeline(self, feature: "Feature") -> "Feature":
        """重命名特征"""
        return Feature(feature.data, self.name)

@dataclass(frozen=True)
class LazyFeatureOperator(Operator):
    spec: "FeatureSpec"

    def pipeline(self, data:pl.DataFrame|None=None) -> "Feature":    
        return self.spec.build(data)

@dataclass(frozen=True)
class Pipeline:
    steps: list[Operator]

    def __repr__(self):
        return f"Pipeline[{', '.join(map(str,self.steps))}]"

    def __or__(self, other: Union["Pipeline",Operator]) -> "Pipeline":
        if isinstance(other, Operator):
            return Pipeline(self.steps + [other])
        elif isinstance(other, Pipeline):
            return Pipeline(self.steps + other.steps)
         
    def __gt__(self, name: str) -> "Pipeline":
        return Pipeline(self.steps + [RenameOperator(name)])
    
    def __call__(self, LastOutput:Any|None=None) -> Any:
        return self.pipeline(LastOutput)
    
    def __and__(self, other:"Pipeline") -> "PipelineSetOperator":
        return PipelineSetOperator([self]) & other
        
    def pipeline(self, LastOutput:Any|None=None) -> "Feature":
        if len(self.steps) == 0: return LastOutput
        output = self.steps[0].pipeline(LastOutput)
        for step in self.steps[1:]:
            output = step.pipeline(output)
        return output

@dataclass(frozen=True)
class PipelineSetOperator(Operator):
    pipelines: list[Pipeline]
  
    def __and__(self, other: Pipeline) -> "PipelineSetOperator":
        return PipelineSetOperator(self.pipelines + [other])
    
    def pipeline(self) -> list["Feature"]:
        return [pipeline.pipline() for pipeline in self.pipelines]


@dataclass(frozen=True)
class Feature:
    """因子特征"""
    
    data: pl.DataFrame
    name: str

    def __repr__(self):
        return f"Feature({self.name})"
    
    def __or__(self, other: Pipeline|Operator) -> "Feature":
        """重载 | 运算符"""
        return other.pipeline(self)
    
    def __gt__(self, name: str) -> "Pipeline":
        return self | RenameOperator(name)

    @property
    def hash(self) -> int:
        """用特征之和代替简单的hash验证"""
        return self.data.select(pl.col("feature").sum()).to_series()[0]
       
    # def __and__(self, other: "Feature") -> "FeatureSet":
    #     """重载 & 运算符
    #     >>> fet & fet & ... -> fst[sym, date, fac_1, fac_2, ...]
    #     """
    #     if self.name == other.name: 
    #         warnings.warn(f"特征名称相同: {self} & {other}", UserWarning)
    #         return FeatureSet(self.data, {self.name:self.hash})
    #     data = self.data.rename({"feature":self.name}).join(other.data.rename({"feature":other.name}), on=["date", "sym"])
    #     hash = {self.name:self.hash, other.name:other.hash}
    #     return FeatureSet(data, hash)
    
    # def __add__(self, other: "Feature") -> "MultiFactorModel":
    #     """重载 + 运算符
    #     >>> fet + fet -> mfm[sym, date, fac_1, fac_2, ...]
    #     """
    #     data = self.data.rename({"feature":self.name}).join(other.data.rename({"feature":other.name}), on=["date", "sym"])
    #     hash = {self.name:self.hash, other.name:other.hash}
    #     return MultiFactorModel(data,hash)

    # def __gt__(self, name: str) -> "Feature":
    #     """重载 > 运算符: 重命名
    #     >>> fet > "new_name" -> fet[new_name]
    #     """
    #     return self | RenameOperator(name)


@dataclass(frozen=True)
class FeatureSpec:
    """因子特征定义"""
    name: str
    builder:FeatureBuilder
    pipeline:"Pipeline"
    description: str|None = None
    direction: int = 1  # 因子方向：1=值越大越好，-1=值越小越好（zoo 规范）

    def __or__(self, other: Pipeline|Operator) -> "Feature":
        """重载 | 运算符"""
        return LazyFeatureOperator(self) | other
    
    def __gt__(self, name: str) -> "Pipeline":
        return LazyFeatureOperator(self) | RenameOperator(name)
        
    def build(self, data: pl.DataFrame) -> Feature:
        data = self.builder.build(data)
        feature = Feature(data, self.name)
        return feature | self.pipeline

    def load(self) -> Feature:
        """尝试直接从数据库里面读取数据"""
        data = get_feature_data(self.name).rename({self.name:"feature"})
        return Feature(data, self.name)
       

if __name__ == "__main__":
    from .operator import Pipeline
    from .builder import MockBuilder
    factor = FeatureSpec(
        name="mock",
        description="mock factor",
        builder=MockBuilder(),
        pipeline=Pipeline([])
    )