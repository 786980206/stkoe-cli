import polars as pl
from dataclasses import dataclass
from ..core import FactorTester, FactorTesterSpec
from ..core.funcs import *
from .base import NumeralTickFormatter, hv

def plot_fac_ic_hist(data:pl.DataFrame, title: str = "原始IC分布(d{dno})", dno:int=10):
    """原始IC分布图"""
    g = data.hvplot.hist(
        y=f"IC(d{dno})",
        bins=100, 
        hover="vline",
        hover_tooltips=[
            (f"IC(d{dno})", f"@IC(d{dno}){{0.00}}"),
            ("频次", "@Count")
        ]
    )
    g = g * hv.VLine(data[f"IC(d{dno})"].mean()).opts(color='green',line_width=2,line_dash='dashed')
    return g.opts(title=title.format(dno=dno))

def plot_fac_ic(data:pl.DataFrame, title: str = "原始IC序列(d{dno})", dno=10, ma=22, ma2=251):
    """原始IC序列"""
    data = data.with_columns([
        pl.col(f"IC(d{dno})").rolling_mean(ma).alias(f"MA(IC,{ma})"),
        pl.col(f"IC(d{dno})").rolling_mean(ma2).alias(f"MA(IC,{ma2})"),
    ])  
    ic = data.hvplot.line(
        x="date",
        y=f"IC(d{dno})",
        label=f"IC(d{dno})",
        yformatter=NumeralTickFormatter(format="0.00"),
        hover="vline",
        hover_cols="all",        
        hover_tooltips=[
            ("日期", "@date{%Y-%m-%d}"),
            (f"IC(d{dno})", f"@IC(d{dno}){{0.00}}"),
            (f"MA(IC,{ma})", f"@MA(IC,{ma}){{0.00}}"),
            (f"MA(IC,{ma2})", f"@MA(IC,{ma2}){{0.00}}"),
        ],
        alpha=0.5,
        color='gray'
    )
    ma = data.hvplot.line(x="date", y=f"MA(IC,{ma})", label=f"MA(IC,{ma})", hover=False)
    ma2 = data.hvplot.line(x="date", y=f"MA(IC,{ma2})", label=f"MA(IC,{ma2})", hover=False)
    zero = hv.HLine(0).opts(color="green", line_width=2, line_dash="dashed")
    g = (ma * ma2 * zero * ic).opts(legend_position="bottom_left",title=title.format(dno=dno))
    return g

def plot_fac_ic_cums(data:pl.DataFrame, title: str = "原始IC累计"):
    """累计IC序列""" 
    y = [x for x in data.columns if x.startswith("IC(d")]
    g = data.hvplot.line(
        x="date",
        y=y,
        group_label=f"累计IC",
        yformatter=NumeralTickFormatter(format="0.00"),
        # hover="vline",
        hover_cols="all",        
        hover_tooltips=[
            ("累计IC", "@累计IC"),
            ("日期", "@date{%Y-%m-%d}"),
            ("IC", "@value{0.00}")
        ],
    ).opts(legend_position="top_left", title=title)
    return g

def plot_fac_ic_month(data:pl.DataFrame, title: str = "原始IC月度平均"):
    """月度IC热力图"""
    data = data.unpivot(index="month", variable_name="periods").with_columns(
        year = pl.col("month").dt.year(),
        month = pl.col("month").dt.month(),
    )
    g = data.hvplot.heatmap(
        x="month",
        y="year",
        C="value", 
        col="periods", 
        title=title,
        subplots=True, 
        colorbar=False,
        toolbar=None,
        xformatter= '%d月',
        yformatter= '%d年',
        hover_tooltips=[
            ("年月", "@year年@month月"),
            ("IC", "@value{0.00}"),
        ],
        # cmap=["#2ca25f", "#f7f7f7", "#de2d26"],
        cmap='coolwarm',
        clim=(-0.2,0.2)
        ).opts(
            toolbar=None,
            plot_size=(200,200),
        )
    return g

@dataclass(frozen=True, slots=True)
class ICTestResults:
    spec: FactorTesterSpec
    ic_date: pl.DataFrame
    gic_date: pl.DataFrame
    rank_ic_date: pl.DataFrame
    rank_gic_date: pl.DataFrame

    def plot_ic_date(self, ma:int=22, ma2:int=251, title: str = "原始IC序列(d{dno})"):
        return hv.Layout([plot_fac_ic(self.ic_date, title=title, dno=dno, ma=ma, ma2=ma2) for dno in self.spec.periods])
    
    def plot_rank_ic_date(self, ma:int=22, ma2:int=251, title: str = "RankIC序列(d{dno})"):
        return hv.Layout([plot_fac_ic(self.rank_ic_date, title=title, dno=dno, ma=ma, ma2=ma2) for dno in self.spec.periods])    
    
    def plot_gic_date(self, ma:int=22, ma2:int=251, title: str = "分组调整原始IC序列(d{dno})"):
        return hv.Layout([plot_fac_ic(self.gic_date, title=title, dno=dno, ma=ma, ma2=ma2) for dno in self.spec.periods])
    
    def plot_rank_gic_date(self, ma:int=22, ma2:int=251, title: str = "分组调整RankIC序列(d{dno})"):
        return hv.Layout([plot_fac_ic(self.rank_gic_date, title=title, dno=dno, ma=ma, ma2=ma2) for dno in self.spec.periods])
    
    def plot_ic_date_hist(self, title: str = "原始IC分布图(d{dno})"):
        return hv.Layout([plot_fac_ic_hist(self.ic_date, title=title, dno=dno) for dno in self.spec.periods])
    
    def plot_gic_date_hist(self, title: str = "分组调整原始IC分布图(d{dno})"):
        return hv.Layout([plot_fac_ic_hist(self.gic_date, title=title, dno=dno) for dno in self.spec.periods])
    
    def plot_rank_ic_date_hist(self, title: str = "RankIC分布图(d{dno})"):
        return hv.Layout([plot_fac_ic_hist(self.rank_ic_date, title=title, dno=dno) for dno in self.spec.periods])
    
    def plot_rank_gic_date_hist(self, title: str = "分组调整RankIC分布图(d{dno})"):
        return hv.Layout([plot_fac_ic_hist(self.rank_gic_date, title=title, dno=dno) for dno in self.spec.periods])
    
    def _calc_ic_date_cums_by_condition(self, data:pl.DataFrame, condition:pl.Expr|str|None=None):
        data = data if condition is None else data.filter(condition) if isinstance(condition,pl.Expr) else data.query(condition)
        return data.with_columns([pl.col(f"IC(d{no})").cum_sum() for no in self.spec.periods]).sort("date")
    
    def calc_ic_date_cums_all(self):
        return self.calc_ic_date_cums_by_condition()
    
    def plot_ic_date_cums_all(self, title: str = "原始IC累计"):
        return plot_fac_ic_cums(self.calc_ic_date_cums_all(), title=title)
    
    def calc_ic_date_cums_by_condition(self, condition:pl.Expr|str|None=None):
        return self._calc_ic_date_cums_by_condition(self.ic_date, condition)
    
    def plot_ic_date_cums_by_condition(self, condition:pl.Expr|str|None=None, title: str = "原始IC累计"):
        return plot_fac_ic_cums(self.calc_ic_date_cums_by_condition(condition), title=title)

    def calc_gic_date_cums_all(self):
        return self.calc_gic_date_cums_by_condition()
    
    def plot_gic_date_cums_all(self, title: str = "分组调整原始IC累计"):
        return plot_fac_ic_cums(self.calc_gic_date_cums_all(), title=title)
    
    def calc_gic_date_cums_by_condition(self, condition:pl.Expr|str|None=None):
        return self._calc_ic_date_cums_by_condition(self.gic_date, condition)
    
    def plot_gic_date_cums_by_condition(self, condition:pl.Expr|str|None=None, title: str = "分组调整原始IC累计"):
        return plot_fac_ic_cums(self.calc_gic_date_cums_by_condition(condition), title=title)
    
    def calc_rank_ic_date_cums_all(self):
        return self.calc_rank_ic_date_cums_by_condition()
    
    def plot_rank_ic_date_cums_all(self, title: str = "RankIC累计"):
        return plot_fac_ic_cums(self.calc_rank_ic_date_cums_all(), title=title)
    
    def calc_rank_ic_date_cums_by_condition(self, condition:pl.Expr|str|None=None):
        return self._calc_ic_date_cums_by_condition(self.rank_ic_date, condition)
    
    def plot_rank_ic_date_cums_by_condition(self, condition:pl.Expr|str|None=None, title: str = "RankIC累计"):
        return plot_fac_ic_cums(self.calc_rank_ic_date_cums_by_condition(condition), title=title)

    def calc_rank_gic_date_cums_all(self):
        return self.calc_rank_gic_date_cums_by_condition()
    
    def plot_rank_gic_date_cums_all(self, title: str = "分组调整RankIC累计"):
        return plot_fac_ic_cums(self.calc_rank_gic_date_cums_all(), title=title)
    
    def calc_rank_gic_date_cums_by_condition(self, condition:pl.Expr|str|None=None):
        return self._calc_ic_date_cums_by_condition(self.rank_gic_date, condition)
    
    def plot_rank_gic_date_cums_by_condition(self, condition:pl.Expr|str|None=None, title: str = "分组调整RankIC累计"):
        return plot_fac_ic_cums(self.calc_rank_gic_date_cums_by_condition(condition), title=title)  

    def _calc_ic_month(self, ic_date:pl.DataFrame):
        return ic_date.with_columns([
            pl.col("date").dt.truncate("1mo").dt.date().alias("month")
        ]).group_by("month").agg([pl.col(f"IC(d{no})").mean() for no in self.spec.periods]).sort("month")

    def calc_ic_month(self):
        return self._calc_ic_month(self.ic_date)

    def plot_ic_month(self, title: str = "原始IC月度平均"):
        return plot_fac_ic_month(self.calc_ic_month(), title=title)

    def calc_gic_month(self):
        return self._calc_ic_month(self.gic_date)

    def plot_gic_month(self, title: str = "分组调整原始IC月度平均"):
        return plot_fac_ic_month(self.calc_gic_month(), title=title)

    def calc_rank_ic_month(self):
        return self._calc_ic_month(self.rank_ic_date)
    
    def plot_rank_ic_month(self, title: str = "RankIC月度平均"):
        return plot_fac_ic_month(self.calc_rank_ic_month(), title=title)

    def calc_rank_gic_month(self):
        return self._calc_ic_month(self.rank_gic_date)

    def plot_rank_gic_month(self, title: str = "分组调整RankIC月度平均"):
        return plot_fac_ic_month(self.calc_rank_gic_month(), title=title)

def ICTest(tester: FactorTester) -> ICTestResults:
    """IC Test
    - 原始IC序列: `ic_date = PearsonCorr(factor, dno)`
    - 分组调整IC序列: `gic_date = PearsonCorr( f(factor - mean(factor)|date, group), dno)`
    - Rank IC 序列: `rank_ic_date = SpearmanCorr(factor, dno)`
    - 分组调整 Rank IC 序列: `rank_gic_date = SpearmanCorr( f(factor - mean(factor)|date, group), dno)`
    """
    # 筛选截面样本
    factor_data = tester.factor_data.filter( c.sample==1 )

    # 原始 IC 序列
    ic_date = factor_data.group_by(["date"]).agg([pl.corr("factor",f"d{no}", method="pearson").alias(f"IC(d{no})") for no in tester.spec.periods]).sort("date")
    # 分组调整 IC 序列
    gic_date = factor_data.with_columns([
        (pl.col(f"d{no}")-pl.col(f"d{no}").mean()).over("date","group") for no in tester.spec.periods
    ]).group_by(["date"]).agg([pl.corr("factor", f"d{no}", method="pearson").alias(f"IC(d{no})") for no in tester.spec.periods])

    # Rank IC 序列
    rank_ic_date = factor_data.group_by(["date"]).agg([pl.corr("factor",f"d{no}", method="spearman").alias(f"IC(d{no})") for no in tester.spec.periods]).sort("date")
    # 分组调整 Rank IC 序列
    rank_gic_date = factor_data.with_columns([
        (pl.col(f"d{no}")-pl.col(f"d{no}").mean()).over("date","group") for no in tester.spec.periods
    ]).group_by(["date"]).agg([pl.corr("factor", f"d{no}", method="spearman").alias(f"IC(d{no})") for no in tester.spec.periods])

    return ICTestResults(
        spec=tester.spec,
        ic_date=ic_date.sort("date"),
        gic_date=gic_date.sort("date"),
        rank_ic_date=rank_ic_date.sort("date"),
        rank_gic_date=rank_gic_date.sort("date"),
    )

