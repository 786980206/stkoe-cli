from dataclasses import dataclass
from typing import List, Literal, Optional, Any, Union
from pydantic import BaseModel
from enum import Enum

# 每一次原始物理表的数据变化，本质上就是对某一个时间范围内的一批标的的指定字段数据的 upsert 或 delete。
# Table不存在事件更新，本质上是table的事件会影响到下游的数据更新。
@dataclass
class DataChangeEvent:
    # 影响的字段范围, None 表示影响所有字段
    field_scope: Optional[list[str]] = None
    # 影响的标的范围, None 表示影响所有标的
    symbol_scope: Optional[list[str]] = None
    # 影响的时间范围, None 表示影响所有时间
    datetime_scope: list[Any]
    # 影响的方式
    action: Literal["upsert", "delete"] = "upsert"


# ===== Node/Edge =====
@dataclass
class AssetNode:
    name: str
    display_name: str
    version: str
    version_list: dict[str, DataChangeEvent]
    description: Optional[str] = None
    tags: Optional[List[str]] = None
    materialized: bool = False
    valid: bool = False
    create_time: Optional[str] = None
    update_time: Optional[str] = None
    

# 字段节点本质上是原始表中的字段，衍生的 fieldset 当中的衍生字段，还有最后出现的因子, 这些本质上都是 color。
@dataclass
class ColumnsMeta:
    name: str
    data_type: str
    display_name: str
    description: str|None = None
    tags: Optional[List[str]] = None
    unit: Optional[str] = None

class FieldMeta(ColumnsMeta):
    formula: str 
    source_asset: AssetNode
    required_fields: List[str]
    window_size: int = 0
    vaild: bool = True
    engine: Literal["polars"] = "polars"


class TableNode(AssetNode):
    columns: dict[str,ColumnsMeta]

class IndexNode(AssetNode):
    symbol: ColumnsMeta
    datetime: ColumnsMeta
    columns: dict[str, ColumnsMeta]
    materialize_partition: Literal["daily", "monthly", "yearly"] = "yearly"

class PanelNode(AssetNode):
    columns: dict[str,ColumnsMeta]

class FieldsetNode(AssetNode):
    fields: dict[str,ColumnsMeta]

class SampleNode(AssetNode):
    ...

class FeatureNode(AssetNode, FieldMeta):
    ...

class FactorNode(ColumnsMeta):
    pipeline: List[Any]

class TesterNode(AssetNode):
    daterange: List[str]
    quantiles: int = 5
    periods: List[int] = [1, 5, 10]
    returns_col: str = "r"
    groupby_col: str = "ic"
    marketcp_col: str = "fv"
    rolling_window: int = 252

class ModelNode(AssetNode):
    ...

class StatNode(AssetNode):
    ...

@dataclass
class DependencyEdge:
    source: AssetNode|ColumnsMeta
    target: AssetNode|ColumnsMeta
    required_version: str
    meta: dict[str,Any] = {}



class TableHander:

    @classmethod
    def add(
        cls, 
        name:str,
        display_name:Optional[str] = None,
        **kwargs
    )-> TableNode:
        """创建一个节点"""
        if display_name is None: display_name = name
        # 构造 version, 当前时间戳
        version = ...
        # 构造其他属性, 数据写入图数据库
        ...
        # 返回节点
        return TableNode(...)

    @classmethod
    def delete(
        cls,
        table:TableNode,
    ) -> None:
        """删除节点和上游边: 当且仅当节点没有下游边时, 才能删除"""
        # 图数据查询是否存在下游依赖, 决定删除与否
        ...

    @classmethod
    def set(
        cls,
        table:TableNode,
        **kwargs
    )-> TableNode:
        """设置节点属性"""
        # 更新图数据库
        ...

    @classmethod
    def meta(
        cls,
        table:TableNode,
    ) -> TableNode:
        """获取节点属性"""
        # 查询图数据库
        ...

    @classmethod
    def get(
        cls,
        table:TableNode,
        **kwargs
    ) -> ...:
        """获取节点对应资产物理数据"""
        ...

    @classmethod
    def col(
        cls,
        table:TableNode,
        col:str,
        **kwargs
    ) -> ...:
        """修改节点列属性"""
        ...


class IndexHander:

    @classmethod
    def add(
        cls, 
        name:str,
        symbol_col:str = "sym",
        datetime_col:str = "date",
        display_name:Optional[str] = None,
        **kwargs
    )-> IndexNode:
        """创建一个节点"""
        # 校验 index 对应物理表中的 symbol_col, datetime_col 列数据是否唯一
        if display_name is None: display_name = name
        # 构造 version, 当前时间戳
        version = ...
        # 构造其他属性, 数据写入图数据库
        ...
        # 返回节点
        return IndexNode(...)

    @classmethod
    def delete(
        cls,
        index:IndexNode,
    ) -> None:
        """删除节点和上游边: 当且仅当节点没有下游边时, 才能删除"""
        # 图数据查询是否存在下游依赖, 决定删除与否
        ...

    @classmethod
    def set(
        cls,
        index:IndexNode,
        **kwargs
    )-> IndexNode:
        """设置节点属性"""
        # 更新图数据库
        ...

    @classmethod
    def meta(
        cls,
        index:IndexNode,
    ) -> IndexNode:
        """获取节点属性"""
        # 查询图数据库
        ...

    @classmethod
    def get(
        cls,
        index:IndexNode,
        **kwargs
    ) -> ...:
        """获取节点对应资产物理数据"""
        ...

    @classmethod
    def col(
        cls,
        index:IndexNode,
        col:str,
        **kwargs
    ) -> ...:
        """修改节点列属性"""
        ...


class PanelHandler:

    @classmethod
    def add(
        cls, 
        name:str,
        index:IndexNode, 
        tables:dict[TableNode, Literal["left_join","asof_join"]],
        display_name:str,
        description:str
    )-> "PanelNode":
        """创建一个节点和多条边"""
        # 构造 version, 当前时间戳
        version = ...
        # 构造 columns 属性: 多表合并注意重名
        ...
        # 构造节点其他属性, 数据写入图数据库
        ...
        # 构造边的属性, 数据写入图数据库, 注意直接写入 required_version
        ...
        # 返回节点
        return PanelNode(...)

    @classmethod
    def delete(
        cls,
        panel:PanelNode,
    ) -> None:
        """删除节点和上游边: 当且仅当节点没有下游边时，才能删除"""

    @classmethod
    def set(
        cls,
        panel:PanelNode,
        display_name:str,
        description:str
    )-> "PanelNode":
        """设置节点属性"""

    @classmethod
    def meta(
        cls,
        panel:PanelNode,
    ) -> "PanelNode":
        """获取节点属性"""

    @classmethod
    def get(
        cls,
        panel:PanelNode,
    ) -> ...:
        """获取节点数据: 当物化并且有效时才返回"""
        # 判断是否物化, 以及物化数据是否过期
        ...

    @classmethod
    def update(
        cls,
        panel:PanelNode,
    ) -> ...:
        """更新节点数据"""
        # 在图数据库上查询所有依赖，并且根据边上的 required version 和依赖的 version list 进行对比，找出那些积累的更新事件。
        ...
        # 因为 panel 本质上和 table 是 1:1 进行的数据映射，index 本质上也是 1:1 进行的数据映射，所以基本上积累的 data change event 可以原样输出。
        # 只需要进行简单的合并就好。比如说，多个 data change event 可以合并到一起，最终输出两类：一类是 upsert，一类是 delete 就可以了。
        ...
        upsert = DataChangeEvent(...)
        delete = DataChangeEvent(...)
        ...
        # 调用物化方法, 根据确定的更新范围，从上游重新提取指定数据，重新计算出目标数据，然后进行物化。
        cls.materialize(panel, upsert, delete)


    @classmethod
    def materialize(
        cls,
        panel:PanelNode,
        upsert:DataChangeEvent,
        delete:DataChangeEvent
    ) -> "PanelNode":
        """物化节点数据"""
        # 根据 upsert 事件去依赖的 index 和 table，提取指定范围的数据。提取完成后进行合并，合并完成后更新现有的 panel data 储存。
        ...
        # 根据 delete 事件，确定当前 panel data 存储的数据中哪些是需要删除的，然后进行删除。
        ...
        # 更新图上面的依赖关系主要是边的依赖关系：把边上面的 required version 跟 source 节点的 version 对齐，把当前节点的状态改为有效。


class FieldsetHandler:

    @classmethod
    def add(
        cls, 
        name:str,
        fieldset:PanelNode, 
        display_name:str,
        description:str,
    )-> "FieldsetNode":
        """创建一个节点和多条边"""
        # 构造 version, 当前时间戳
        version = ...
        # 构造节点其他属性, 数据写入图数据库
        ...
        # 构造边的属性, 数据写入图数据库, 注意直接写入 required_version
        ...
        # 返回节点
        return FieldsetNode(...)

    @classmethod
    def add_field(
        cls,
        fieldset:FieldsetNode,
        field_name:str,
        formula:str,
        **kwargs
    ) -> None:
        """添加字段"""
        ...

    @classmethod
    def delete(
        cls,
        fieldset:FieldsetNode,
    ) -> None:
        """删除节点和上游边: 当且仅当节点没有下游边时，才能删除"""

    @classmethod
    def delete_field(
        cls,
        fieldset:FieldsetNode,
        field_name:str
    ):
        ...

    @classmethod
    def set(
        cls,
        fieldset:FieldsetNode,
        display_name:str,
        description:str
    )-> "PanelNode":
        """设置节点属性"""

    @classmethod
    def set_field(
        cls,
        fieldset:FieldsetNode,
        field_name:str,
        **kwargs
    ):
        ...

    @classmethod
    def meta(
        cls,
        fieldset:FieldsetNode,
    ) -> "PanelNode":
        """获取节点属性"""
        ...

    @classmethod
    def meta_field(
        cls,
        fieldset:FieldsetNode,
        field_name:str
    ):
        ...

    @classmethod
    def get(
        cls,
        fieldset:FieldsetNode,
        fields_only:bool = False,
    ) -> ...:
        """获取节点数据: 当物化并且有效时才返回"""
        # 判断是否物化, 以及物化数据是否过期
        ...
        # Fieldset 指指挥物化衍生的字段数据，需要把物化的衍生字段产物根据 fields_only 决定是否和 dataset 进行拼接返回。

    @classmethod
    def on_change(
        cls,
        fieldset:FieldsetNode,
        event:Optional[DataChangeEvent] = None,
    ) -> ...:
        """根据积累的数据更新事件进行合并，并且输出适合于本handle的更新参数及方法。"""
        # 因为 panel 本质上和 table 是 1:1 进行的数据映射，index 本质上也是 1:1 进行的数据映射，所以基本上积累的 data change event 可以原样输出。
        # 只需要进行简单的合并就好。比如说，多个 data change event 可以合并到一起，最终输出两类：一类是 upsert，一类是 delete 就可以了。
        ...
        return {"upsert": DataChangeEvent(...), "delete" : DataChangeEvent(...)}

    @classmethod
    def materialize(
        cls,
        fieldset:FieldsetNode,
        upsert:Optional[DataChangeEvent] = None,
        delete:Optional[DataChangeEvent] = None,
    ) -> "FieldsetNode":
        """物化节点数据"""
        # 根据 upsert 事件去依赖的 index 和 table，提取指定范围的数据。提取完成后进行合并，合并完成后更新现有的 panel data 储存。
        ...
        # 根据 delete 事件，确定当前 panel data 存储的数据中哪些是需要删除的，然后进行删除。
        ...
        # 更新图上面的依赖关系主要是边的依赖关系：把边上面的 required version 跟 source 节点的 version 对齐，把当前节点的状态改为有效。



class SampleHandler:
    ...

class FeatureHandler:
    ...

class FactorHandler:
    ...

class TesterHandler:
    ...

class ModelHandler:
    ...

class StatHandler:
    ...

class GraphHandler:

    @classmethod
    def list(
        cls,
        asset_type:Literal["table", "index", "panel", "fieldset", "sample", "feature", "factor", "tester", "model", "stat"],
        **kwargs
    ) -> ...:
        """根据指定条件获取节点列表"""

    @classmethod
    def get(
        cls,
        asset:AssetNode,
    ) -> ...:
        """根据节点获取整个上下游的依赖信息。"""
        ...

    @classmethod
    def scan(
        cls,
    ) -> ...:
        """"""
    