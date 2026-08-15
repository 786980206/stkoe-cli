"""STAT 模块：dataset / table 目标的统计资产（StatController，async 接口）

定位：
- 统计是独立资产：与数据解耦（目标只管数据，统计归统计），产物写 ``stats/`` 目录
- 目标：table 或 dataset；产物目录 ``stats/<target_type>/<target_name>/<kind>/``
- 分组文件：``all.parquet``（全量统计）+ 按目标索引分组逐分区文件
  （dataset 索引 = 主键 keys；table = 非工具列），命名 ``<partition>.parquet``
- ``stat scan`` 生成/更新分组产物（幂等，重算覆盖）；``stat get`` 读文件
"""
from __future__ import annotations

import asyncio
import shutil
from pathlib import Path

import polars as pl

from ..table.controller import DEFAULT_IGNORE_COLS
from ..table.util import now
from .calc import calc_stats, calc_storage
from .spec import StatFile, StatMeta, StatScanReport


class StatNotFoundError(FileNotFoundError):
    pass


class StatTargetError(ValueError):
    pass


def _parquet_rows(p: Path) -> int:
    try:
        return pl.scan_parquet(p).select(pl.len()).collect().item()
    except Exception:
        return 0


def _ordered(files: tuple[StatFile, ...]) -> tuple[StatFile, ...]:
    """排序：all 分组恒在首，其余按分区名排序"""
    return tuple(sorted(files, key=lambda f: (f.partition != "all", f.partition)))


class StatController:
    """统计控制面：围绕目标（table/dataset）生成/读取分组统计

    复用一个 DatasetController 访问目标数据（dataset 实时 join 视图或物化数据）。
    """

    def __init__(self, data_dir: Path | str | None = None,
                 ignore_cols: tuple[str, ...] = DEFAULT_IGNORE_COLS):
        from ..dataset.controller import DatasetController

        self._dc = DatasetController(data_dir=data_dir, ignore_cols=ignore_cols)
        self.data_dir = self._dc.data_dir
        self.catalog = self._dc.catalog
        self.root = self.data_dir / "stats"
        self.ignore_cols = set(ignore_cols)

    # ---------- 路径 / 分组解析 ----------

    def _kind_dir(self, target_type: str, target_name: str, kind: str) -> Path:
        return self.root / target_type / target_name / kind

    def _graph_service(self):
        from ..graph.service import GraphService

        return GraphService(data_dir=self.data_dir)

    def _index_cols(self, target_type: str, target_name: str) -> list[str]:
        """目标索引列：panel（原 dataset）用主键 keys；table 用非工具列（走 graph）"""
        svc = self._graph_service()
        try:
            if target_type == "table":
                cols = svc.table_meta(target_name)["columns"]
                return [c["name"] for c in cols if c["name"] not in self.ignore_cols]
            if target_type in ("dataset", "panel"):
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
            if target_type in ("dataset", "panel"):
                return svc.panel_lazy(target_name)
        finally:
            svc.close()
        raise StatTargetError(f"unsupported stat target: {target_type}")

    # ---------- test 目标（因子测试器） ----------

    def _scan_test_sync(self, target_name: str, kind: str,
                        on_progress=None) -> StatScanReport:
        """因子测试器扫描：运行 tester kind 并把命名产物写入
        ``stats/test/<name>/<kind>/<output>.parquet``（数据源走 GraphService）。"""
        from ..factor_test.spec import FactorTesterSpec
        from ..factor_test.tester import run_tester
        from ..table.controller import TableNotFoundError

        svc = self._graph_service()
        try:
            tm = svc.test_meta(target_name)
            data = svc.test_data(target_name)
        except TableNotFoundError:
            raise StatNotFoundError(f"test 未注册: {target_name}")
        finally:
            svc.close()
        spec = FactorTesterSpec.from_dict(tm.get("spec") or {})
        outputs = run_tester(kind, data, spec)
        out_dir = self.root / "test" / target_name / kind
        out_dir.mkdir(parents=True, exist_ok=True)
        files: list[StatFile] = []
        total = len(outputs)
        for i, (out_name, df) in enumerate(outputs.items(), start=1):
            if on_progress is not None:
                on_progress(i, total, f"{target_name}/{kind}: {out_name}")
            p = out_dir / f"{out_name}.parquet"
            df.write_parquet(p)
            files.append(StatFile(partition=out_name,
                                  rel_path=p.relative_to(self.root),
                                  rows=df.height, size=p.stat().st_size))
        files = list(_ordered(tuple(files)))
        return StatScanReport(
            target_type="test", target_name=target_name, kind=kind,
            partitions=tuple(f.partition for f in files), files=tuple(files))

    # ---------- scan ----------

    def _scan_sync(self, target_type: str, target_name: str, kind: str = "coverage",
                   on_progress=None) -> StatScanReport:
        """计算全量 + 逐索引分组统计并写 ``stats/<type>/<name>/<kind>/<part>.parquet``

        ``on_progress(i, total, msg)`` 可选进度回调（worker 线程同步调用，逐分组）。
        kind=storage 走存续统计分支（见 _scan_storage_sync）。
        覆盖率统计全程 LazyFrame，写入走 ``sink_parquet``（流式），
        calc_stats 内部按 dtype 类别聚合再对窄结果 unpivot，内存与数据规模解耦。
        """
        if kind == "storage":
            return self._scan_storage_sync(target_type, target_name, on_progress)
        if target_type == "test":
            return self._scan_test_sync(target_name, kind, on_progress)
        parts = self._partitions(target_type, target_name)
        lf = self._select_lf(target_type, target_name)
        out_dir = self._kind_dir(target_type, target_name, kind)
        out_dir.mkdir(parents=True, exist_ok=True)
        files: list[StatFile] = []
        for i, p in enumerate(parts, start=1):
            if on_progress is not None:
                on_progress(i, len(parts), f"{target_type}/{target_name}: {p}")
            f = out_dir / f"{p}.parquet"
            calc_stats(lf, group_col=None if p == "all" else p).sink_parquet(f)
            files.append(StatFile(partition=p, rel_path=f.relative_to(self.root),
                                  rows=_parquet_rows(f), size=f.stat().st_size))
        files = list(_ordered(tuple(files)))
        return StatScanReport(target_type=target_type, target_name=target_name,
                              kind=kind, partitions=tuple(f.partition for f in files),
                              files=tuple(files))

    def _scan_storage_sync(self, target_type: str, target_name: str,
                           on_progress=None) -> StatScanReport:
        """存续统计（kind=storage）：按 hive 分区键/值聚合原始文件存储占用

        只对目标磁盘 parquet 做 stat（不读数据页），产出
        ``stats/<type>/<name>/storage/<part>.parquet``：
        ``all.parquet`` = ``partition_by=__all__ / partition_value=__all__`` 全表总量；
        每个分区键一个文件，按该键目录值各一行
        ``partition_by=<key> / partition_value=<value>``。
        """
        from ..table.util import detect_layout, disk_files

        if target_type == "table":
            svc = self._graph_service()
            try:
                root = svc.data_dir / "tables" / target_name
            finally:
                svc.close()
        elif target_type in ("dataset", "panel"):
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
            df.lazy().sink_parquet(f)
            out_files.append(StatFile(partition=p, rel_path=f.relative_to(self.root),
                                      rows=df.height, size=f.stat().st_size))
        out_files = list(_ordered(tuple(out_files)))
        return StatScanReport(target_type=target_type, target_name=target_name,
                              kind="storage", partitions=tuple(f.partition for f in out_files),
                              files=tuple(out_files))

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
            return pl.read_parquet(f)
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
            out[f.partition] = pl.read_parquet(p)
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
                     rows=_parquet_rows(p), size=p.stat().st_size)
            for p in paths
        )
        ts = now()
        files = tuple(
            StatFile(partition=p.stem, rel_path=p.relative_to(self.root),
                     rows=_parquet_rows(p), size=p.stat().st_size)
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
                                 rows=_parquet_rows(p), size=p.stat().st_size)
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
        return {"deleted": f"{target_type}/{target_name}" + (f"/{kind}" if kind else "")}

    # ---------- async 接口 ----------

    async def scan(self, target_type: str, target_name: str,
                   kind: str = "coverage", on_progress=None) -> StatScanReport:
        """生成/更新统计分组产物（幂等）；``on_progress`` 逐分组进度回调"""
        return await asyncio.to_thread(self._scan_sync, target_type, target_name,
                                       kind, on_progress)

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