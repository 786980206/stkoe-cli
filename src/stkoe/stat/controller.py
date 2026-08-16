"""STAT 模块：panel / table 目标的统计资产（StatController，async 接口）

定位：
- 统计是独立资产：与数据解耦（目标只管数据，统计归统计），产物写 ``stats/`` 目录
- 目标：table 或 panel；产物目录 ``stats/<target_type>/<target_name>/<kind>/``
- 分组文件：``all.parquet``（全量统计）+ 按目标索引分组逐分区文件
  （panel 索引 = 主键 keys；table = 非工具列），命名 ``<partition>.parquet``
- ``stat scan`` 生成/更新分组产物（幂等，重算覆盖）；``stat get`` 读文件
- **进图登记**：scan 成功后把产物登记为图内 ``Stat`` 节点
  （``stat:<target_type>/<target_name>/<kind>``）+ ``(Stat)-[:DEPENDS]->目标``
  边（role=target）——graph nodes/lineage/stats 可见；物理文件仍是唯一数据源，
  节点是登记镜像（``stat delete`` 与目标资产删除时级联清理）
"""
from __future__ import annotations

import asyncio
import shutil
from pathlib import Path

import polars as pl

from ..graph.model import node_id
from ..storage import calc_storage, calc_stats, detect_layout, disk_files, now, \
    row_count, scan, write_file
from ..table.errors import DEFAULT_IGNORE_COLS, TableNotFoundError
from .spec import StatFile, StatMeta, StatScanReport


class StatNotFoundError(FileNotFoundError):
    pass


class StatTargetError(ValueError):
    pass


def _ordered(files: tuple[StatFile, ...]) -> tuple[StatFile, ...]:
    """排序：all 分组恒在首，其余按分区名排序"""
    return tuple(sorted(files, key=lambda f: (f.partition != "all", f.partition)))


class StatController:
    """统计控制面：围绕目标（table/panel/test）生成/读取分组统计

    数据源走 GraphService（table_lazy/panel_lazy/test_data），统计产物只落盘 stats/ 目录。
    """

    def __init__(self, data_dir: Path | str | None = None,
                 ignore_cols: tuple[str, ...] = DEFAULT_IGNORE_COLS):
        from ..settings import load_config

        self.data_dir = Path(data_dir) if data_dir else Path(load_config().data_dir)
        self.root = self.data_dir / "stat"
        self.ignore_cols = set(ignore_cols)

    # ---------- 路径 / 分组解析 ----------

    def _kind_dir(self, target_type: str, target_name: str, kind: str) -> Path:
        return self.root / target_type / target_name / kind

    def _graph_service(self):
        from ..graph.service import GraphService

        return GraphService(data_dir=self.data_dir)

    def _register_graph_node(self, report: StatScanReport) -> None:
        """统计产物登记进图：``Stat`` 节点 + ``(Stat)-[:DEPENDS]->目标`` 边。

        - 节点 id ``stat:<target_type>/<target_name>/<kind>``，属性含目标引用/
          kind/分区清单/文件清单/时间——graph nodes/lineage/stats 可见；
        - 重复 scan 幂等：节点已存在则 patch（不重复登记）；
        - 物理文件仍是唯一数据源，节点是登记镜像（`stat delete` 同步删除；
          目标资产删除时由 ``GraphStore.delete_node`` 级联清理）。
        """
        svc = self._graph_service()
        try:
            store = svc.store
            nid = f"stat:{report.target_type}/{report.target_name}/{report.kind}"
            props = {
                "name": f"{report.target_type}/{report.target_name}/{report.kind}",
                "target_type": report.target_type,
                "target_name": report.target_name,
                "kind": report.kind,
                "partitions": list(report.partitions),
                "files": [{"partition": f.partition, "rel_path": str(f.rel_path),
                           "rows": f.rows, "size": f.size} for f in report.files],
                "updated_at": now(),
                "materialized": True,
            }
            if store.has_node(nid):
                store.patch_node(nid, **props)
            else:
                props["created_at"] = now()
                store.create_node(nid, "Stat", props)
            tgt = node_id(report.target_type, report.target_name)
            if store.has_node(tgt):
                store.create_edge(nid, tgt, "DEPENDS",
                                  {"role": "target", "required_version": 0})
        finally:
            svc.close()

    def _delete_graph_nodes(self, target_type: str, target_name: str,
                            kind: str | None = None) -> None:
        """删除图内 stat 登记节点（与物理产物目录删除同步）。"""
        svc = self._graph_service()
        try:
            prefix = f"stat:{target_type}/{target_name}"
            for n in svc.store.list_nodes("Stat"):
                nid = n.get("id") or ""
                if nid.startswith(prefix) and (kind is None or nid == f"{prefix}/{kind}"):
                    svc.store.delete_node(nid)
        finally:
            svc.close()

    def _index_cols(self, target_type: str, target_name: str) -> list[str]:
        """目标索引列：panel 用主键 keys；table 用非工具列（走 graph）"""
        svc = self._graph_service()
        try:
            if target_type == "table":
                cols = svc.table_meta(target_name)["columns"]
                return [c["name"] for c in cols if c["name"] not in self.ignore_cols]
            if target_type == "panel":
                return list(svc.panel_meta(target_name)["keys"])
        finally:
            svc.close()
        raise StatTargetError(f"unsupported stat target: {target_type}")

    def _partitions(self, target_type: str, target_name: str) -> list[str]:
        return ["all", *self._index_cols(target_type, target_name)]

    def _select_lf(self, target_type: str, target_name: str) -> pl.LazyFrame:
        """目标数据（lazy）：table 剔除工具列，panel 走实时 join 视图（走 graph）"""
        svc = self._graph_service()
        try:
            if target_type == "table":
                return svc.table_lazy(target_name, exclude_tool=True)
            if target_type == "panel":
                return svc.panel_lazy(target_name)
        finally:
            svc.close()
        raise StatTargetError(f"unsupported stat target: {target_type}")

    # ---------- tester 目标（因子测试器） ----------

    def _scan_tester_sync(self, target_name: str, kind: str,
                        on_progress=None) -> StatScanReport:
        """因子测试器扫描：运行 tester kind 并把命名产物写入
        ``stats/tester/<name>/<kind>/<output>.parquet``（数据源走 GraphService）。"""
        from ..factor_tester.spec import FactorTesterSpec
        from ..factor_tester.tester import run_tester
        from ..graph.errors import AssetNotFoundError

        svc = self._graph_service()
        try:
            tm = svc.tester_meta(target_name)
            data = svc.tester_data(target_name)
        except AssetNotFoundError:
            raise StatNotFoundError(f"tester 未注册: {target_name}") from None
        finally:
            svc.close()
        spec = FactorTesterSpec.from_dict(tm.get("spec") or {})
        outputs = run_tester(kind, data, spec)
        out_dir = self.root / "tester" / target_name / kind
        out_dir.mkdir(parents=True, exist_ok=True)
        files: list[StatFile] = []
        total = len(outputs)
        for i, (out_name, df) in enumerate(outputs.items(), start=1):
            if on_progress is not None:
                on_progress(i, total, f"{target_name}/{kind}: {out_name}")
            p = out_dir / f"{out_name}.parquet"
            write_file(df, p)
            files.append(StatFile(partition=out_name,
                                  rel_path=p.relative_to(self.root),
                                  rows=df.height, size=p.stat().st_size))
        files = list(_ordered(tuple(files)))
        report = StatScanReport(
            target_type="tester", target_name=target_name, kind=kind,
            partitions=tuple(f.partition for f in files), files=tuple(files))
        self._register_graph_node(report)
        return report

    # ---------- scan ----------

    def _scan_sync(self, target_type: str, target_name: str, kind: str = "coverage",
                   on_progress=None,
                   partitions: list[str] | None = None) -> StatScanReport:
        """计算全量 + 逐索引分组统计并写 ``stats/<type>/<name>/<kind>/<part>.parquet``

        ``on_progress(i, total, msg)`` 可选进度回调（worker 线程同步调用，逐分组）。
        ``partitions`` 给定时只算指定分区（如 ``["all", "date"]``，未知名报错）——
        粗桶大表（千万行 × 数十索引列）全量分区逐列分组统计的内存/耗时随分区数
        线性放大，按需只算常用分区（all + 索引列子集）可秒级完成。
        kind=storage 走存续统计分支（见 _scan_storage_sync）。

        计算全程 LazyFrame；**执行用 in-memory 引擎**（``collect()`` 后写结果，
        结果本身很小 = 组数 × 14 列）：实测（1852 万行 × 22 列，见变更记录）
        流式 ``sink_parquet`` 的 group_by 哈希表单分区峰值 ~8GB，且 polars 跨
        分区不释放内存（第 8 个分区即 OOM）；in-memory 引擎单分区 ~16s、
        临时结构随 collect 结束释放，串行 23 分区可稳定跑完。
        """
        if kind == "storage":
            return self._scan_storage_sync(target_type, target_name, on_progress)
        if target_type == "tester":
            return self._scan_tester_sync(target_name, kind, on_progress)
        parts = self._partitions(target_type, target_name)
        if partitions:
            unknown = [p for p in partitions if p not in parts]
            if unknown:
                raise StatTargetError(f"未知 stat 分区: {unknown}（可选 {parts}）")
            parts = partitions
        lf = self._select_lf(target_type, target_name)
        out_dir = self._kind_dir(target_type, target_name, kind)
        out_dir.mkdir(parents=True, exist_ok=True)
        files: list[StatFile] = []
        for i, p in enumerate(parts, start=1):
            if on_progress is not None:
                on_progress(i, len(parts), f"{target_type}/{target_name}: {p}")
            f = out_dir / f"{p}.parquet"
            df = calc_stats(lf, group_col=None if p == "all" else p).collect()
            write_file(df, f)
            files.append(StatFile(partition=p, rel_path=f.relative_to(self.root),
                                  rows=df.height, size=f.stat().st_size))
        files = list(_ordered(tuple(files)))
        report = StatScanReport(target_type=target_type, target_name=target_name,
                                kind=kind, partitions=tuple(f.partition for f in files),
                                files=tuple(files))
        self._register_graph_node(report)
        return report

    def _scan_storage_sync(self, target_type: str, target_name: str,
                           on_progress=None) -> StatScanReport:
        """存续统计（kind=storage）：按 hive 分区键/值聚合原始文件存储占用

        只对目标磁盘 parquet 做 stat（不读数据页），产出
        ``stats/<type>/<name>/storage/<part>.parquet``：
        ``all.parquet`` = ``partition_by=__all__ / partition_value=__all__`` 全表总量；
        每个分区键一个文件，按该键目录值各一行
        ``partition_by=<key> / partition_value=<value>``。
        """
        if target_type == "table":
            svc = self._graph_service()
            try:
                root = svc.data_dir / "table" / target_name
            finally:
                svc.close()
        elif target_type == "panel":
            raise StatTargetError("storage 统计不支持 panel（无物化目录）")
        else:
            raise StatTargetError(f"unsupported stat target: {target_type}")
        if not root.exists():
            raise StatNotFoundError(f"{target_type} 目录不存在: {root}")

        files = disk_files(root)
        parts = ["all", *detect_layout([f.rel_path for f in files])[1]]
        out_dir = self._kind_dir(target_type, target_name, "storage")
        out_dir.mkdir(parents=True, exist_ok=True)
        out_files: list[StatFile] = []
        items = [(f.rel_path, f.size) for f in files]
        for i, p in enumerate(parts, start=1):
            if on_progress is not None:
                on_progress(i, len(parts), f"{target_type}/{target_name}: {p}")
            df = calc_storage(items, group_key=None if p == "all" else p)
            f = out_dir / f"{p}.parquet"
            write_file(df, f)
            out_files.append(StatFile(partition=p, rel_path=f.relative_to(self.root),
                                      rows=df.height, size=f.stat().st_size))
        out_files = list(_ordered(tuple(out_files)))
        report = StatScanReport(target_type=target_type, target_name=target_name,
                                kind="storage", partitions=tuple(f.partition for f in out_files),
                                files=tuple(out_files))
        self._register_graph_node(report)
        return report

    # ---------- get ----------

    def _get_sync(self, target_type: str, target_name: str, kind: str = "coverage",
                  partition_by: str | None = None
                  ) -> pl.DataFrame | dict[str, pl.DataFrame]:
        """读取统计文件：不指定 partition_by → 返回全部 ``{分区: DataFrame}``；
        指定 → 返回单个分区 DataFrame"""
        out_dir = self._kind_dir(target_type, target_name, kind)
        if partition_by is not None:
            f = out_dir / f"{partition_by}.parquet"
            if not f.exists():
                raise StatNotFoundError(f"stat 分区文件不存在: {f.relative_to(self.root)}")
            return scan(f).collect()
        if not out_dir.exists():
            raise StatNotFoundError(f"stat 目录不存在: {out_dir.relative_to(self.root)}")
        files = _ordered(tuple(
            StatFile(partition=f.stem, rel_path=f.relative_to(self.root),
                     rows=0, size=f.stat().st_size)
            for f in out_dir.glob("*.parquet")
        ))
        out: dict[str, pl.DataFrame] = {}
        for f in files:
            p = out_dir / f"{f.partition}.parquet"
            out[f.partition] = scan(p).collect()
        return out

    # ---------- meta / list / delete ----------

    def _meta_sync(self, target_type: str, target_name: str, kind: str = "coverage"
                   ) -> StatMeta:
        out_dir = self._kind_dir(target_type, target_name, kind)
        if not out_dir.exists():
            raise StatNotFoundError(f"stat 目录不存在: {out_dir.relative_to(self.root)}")
        paths = sorted(out_dir.glob("*.parquet")) if out_dir.exists() else []
        files = tuple(
            StatFile(partition=p.stem, rel_path=p.relative_to(self.root),
                     rows=row_count(p), size=p.stat().st_size)
            for p in paths
        )
        ts = now()
        files = tuple(
            StatFile(partition=p.stem, rel_path=p.relative_to(self.root),
                     rows=row_count(p), size=p.stat().st_size)
            for p in paths
        )
        files = _ordered(files)
        return StatMeta(target_type=target_type, target_name=target_name, kind=kind,
                        partitions=tuple(f.partition for f in files), files=files,
                        created_at=ts if files else "", updated_at=ts if files else "")

    def _list_sync(self) -> list[StatMeta]:
        """全部已生成统计（按 target_type/target_name/kind）"""
        out: list[StatMeta] = []
        if not self.root.exists():
            return out
        for tdir in sorted(x for x in self.root.iterdir() if x.is_dir()):
            for ndir in sorted(x for x in tdir.iterdir() if x.is_dir()):
                for kdir in sorted(x for x in ndir.iterdir() if x.is_dir()):
                    paths = sorted(kdir.glob("*.parquet"))
                    if not paths:
                        continue
                    ts = now()
                    files = _ordered(tuple(
                        StatFile(partition=p.stem, rel_path=p.relative_to(self.root),
                                 rows=row_count(p), size=p.stat().st_size)
                        for p in paths
                    ))
                    out.append(StatMeta(target_type=tdir.name, target_name=ndir.name,
                                        kind=kdir.name,
                                        partitions=tuple(f.partition for f in files),
                                        files=files, created_at=ts, updated_at=ts))
        return out

    def _delete_sync(self, target_type: str, target_name: str,
                     kind: str | None = None) -> dict:
        """删除统计产物目录（kind 缺省删除该目标全部统计）"""
        base = self.root / target_type / target_name
        if not base.exists():
            raise StatNotFoundError(f"stat 目标不存在: {base.relative_to(self.root)}")
        target = base if kind is None else base / kind
        if not target.exists():
            raise StatNotFoundError(f"stat 目录不存在: {target.relative_to(self.root)}")
        shutil.rmtree(target)
        self._delete_graph_nodes(target_type, target_name, kind)
        return {"deleted": f"{target_type}/{target_name}" + (f"/{kind}" if kind else "")}

    # ---------- async 接口 ----------

    async def scan(self, target_type: str, target_name: str,
                   kind: str = "coverage", on_progress=None,
                   partitions: list[str] | None = None) -> StatScanReport:
        """生成/更新统计分组产物（幂等）；``on_progress`` 逐分组进度回调；
        ``partitions`` 给定时只算指定分区（按需扫描，见 ``_scan_sync``）。"""
        return await asyncio.to_thread(self._scan_sync, target_type, target_name,
                                       kind, on_progress, partitions)

    async def get(self, target_type: str, target_name: str,
                  kind: str = "coverage", partition_by: str | None = None
                  ) -> pl.DataFrame | dict[str, pl.DataFrame]:
        """读统计；不指定 partition_by 返回全部 ``{分区: DataFrame}``"""
        return await asyncio.to_thread(self._get_sync, target_type, target_name,
                                       kind, partition_by)

    async def meta(self, target_type: str, target_name: str,
                   kind: str = "coverage") -> StatMeta:
        """统计元数据（目标/kind/分组文件清单）"""
        return await asyncio.to_thread(self._meta_sync, target_type, target_name, kind)

    async def list(self) -> list[StatMeta]:
        """已生成统计列表"""
        return await asyncio.to_thread(self._list_sync)

    async def delete(self, target_type: str, target_name: str,
                     kind: str | None = None) -> dict:
        """删除统计产物（kind 缺省删除该目标全部）"""
        return await asyncio.to_thread(self._delete_sync, target_type, target_name, kind)


__all__ = ["StatController", "StatNotFoundError", "StatTargetError"]