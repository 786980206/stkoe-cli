from itertools import product
import polars as pl
from dataclasses import dataclass
from ..core import FactorTester, FactorTesterSpec
from ..core.funcs import *
from .base import NumeralTickFormatter, hv

def plot_rtn_stat(data, title="原始收益统计"):
    """原始收益统计"""
    y = reversed([x for x in data.columns if x.startswith("E(d")])
    g = data.hvplot.bar(
        x="factor_quantile",
        y=y,
        title=title,
        hover="vline",
        yformatter=NumeralTickFormatter(format="0.00%"),
        hover_tooltips=[
            ("分组", "@factor_quantile"),
            ("滞后", "@Variable"),
            ("收益", "@value{0.00%}"),
        ],
        rot=90, 
    )
    return g

def plot_rtn_diff(data: pl.DataFrame, dno:int = 10, ma=22, title: str = "原始收益价差序列(d{dno})"):
    """原始收益价差序列"""
    data = data.with_columns(
        lower = pl.col(f"E(Δd{dno})") - 3 * pl.col(f"SE(Δd{dno})"),
        upper = pl.col(f"E(Δd{dno})") + 3 * pl.col(f"SE(Δd{dno})"),
        ma = pl.col(f"E(Δd{dno})").rolling_mean(ma)
    )    
    line = data.hvplot.line(
        x='date',
        y=f"E(Δd{dno})",
        label=f"E(Δd{dno})",
        yformatter=NumeralTickFormatter(format="0.00%"),        
        hover="vline",
        hover_cols="all",
        hover_tooltips=[
            ("日期", "@date{%Y-%m-%d}"),
            (f"E(Δd{dno})", f"@E(Δd{dno}){{0.00%}}"),
            (f"MA({ma})", f"@ma{{0.00%}}"),
        ],
    )
    band = data.hvplot.area(x='date',y='lower',y2='upper',alpha=0.3,color='gray',label=f"E(Δd{dno})±3σ",hover=False)
    ma_line = data.hvplot.line(x='date', y=f"ma", label=f"MA(Δd{dno},{ma})",hover=False)
    zero = hv.HLine(0).opts(color="green", line_width=2, line_dash="dashed")
    g = (line * band * ma_line * zero).opts(legend_position="bottom_left", title=title.format(dno=dno))
    return g

def plot_rtn_cums(data:pl.DataFrame, dno:int = 10, title: str = "累计收益序列(d{dno})"):
    """累计收益序列"""
    y = reversed([x for x in data.columns if x.startswith(f"CR(d{dno},")])
    g = data.hvplot.line(
        x="date",
        y=y,
        group_label="分组",
        title=title.format(dno=dno),
        # hover="vline",
        yformatter=NumeralTickFormatter(format="0%"),
        hover_tooltips=[
            ("分组", "@分组"),
            ("日期", "@date{%Y-%m-%d}"),
            ("收益", "@value{0.00%}")
        ],
    ).opts(legend_position="top_left")
    return g

def plot_fac_rets(data:pl.DataFrame, title: str = "因子累计收益序列"):
    """因子累计收益序列"""
    y = [x for x in data.columns if x.startswith(f"FR(d")]
    g = data.hvplot.line(
        x="date",
        y=y,
        group_label="因子收益",
        title=title,
        # hover="vline",
        yformatter=NumeralTickFormatter(format="0%"),
        hover_tooltips=[
            ("因子收益", "@因子收益"),
            ("日期", "@date{%Y-%m-%d}"),
            ("收益", "@value{0.00%}")
        ],
    ).opts(legend_position="top_left")
    return g

@dataclass(frozen=True, slots=True)
class BucketReturnsTestResults:
    spec: FactorTesterSpec
    rtn_date: pl.DataFrame
    exr_date: pl.DataFrame
    gbr_date: pl.DataFrame
    _idx_stat: list[pl.Expr]

    def calc_rtn_stat_all(self):    
        return self.rtn_date.group_by(["factor_quantile"]).agg( self._idx_stat ).sort("factor_quantile")

    def plot_rtn_stat_all(self, title:str = "原始收益统计"):
        return plot_rtn_stat(self.calc_rtn_stat_all(), title=title) 

    def calc_rtn_stat_by_condition(self, condition:pl.Expr|str|None=None):
        data = self.rtn_date if condition is None else self.rtn_date.filter(condition) if isinstance(condition,pl.Expr) else self.rtn_date.query(condition)
        return data.group_by(["factor_quantile"]).agg( self._idx_stat ).sort("factor_quantile")

    def plot_rtn_stat_by_condition(self, condition:pl.Expr|str|None = None, title:str = "原始收益统计"):
        return plot_rtn_stat(self.calc_rtn_stat_by_condition(condition=condition), title=title)         
    
    def calc_rtn_stat_by_year(self):
        return self.rtn_date.with_columns(year=pl.col("date").dt.year()).group_by(["year", "factor_quantile"]).agg( self._idx_stat ).sort("year","factor_quantile")
    
    def plot_rtn_stat_by_year(self):
        return hv.Layout([plot_rtn_stat(data, title=f"{year[0]}年").opts(responsive=True) for year, data in self.calc_rtn_stat_by_year().group_by("year")]).cols(1)

    def calc_rtn_stat_by_month(self, year:int):
        return self.rtn_date.filter( pl.col("date").dt.year() == year ).with_columns(month=pl.col("date").dt.month()).group_by(["month", "factor_quantile"]).agg( self._idx_stat ).sort("month", "factor_quantile")    
    
    def plot_rtn_stat_by_month(self, year:int):
        return hv.Layout([plot_rtn_stat(data, title=f"{year}年{month[0]}月").opts(responsive=True) for month, data in self.calc_rtn_stat_by_month(year=year).group_by("month")]).cols(4)    
        
    def calc_exr_stat_all(self):    
        return self.exr_date.group_by(["factor_quantile"]).agg( self._idx_stat ).sort("factor_quantile")

    def plot_exr_stat_all(self, title:str = "超额收益统计"):
        return plot_rtn_stat(self.calc_exr_stat_all(), title=title)

    def calc_exr_stat_by_condition(self, condition:pl.Expr|str|None=None):
        data = self.exr_date if condition is None else self.exr_date.filter(condition) if isinstance(condition,pl.Expr) else self.exr_date.query(condition)
        return data.group_by(["factor_quantile"]).agg( self._idx_stat ).sort("factor_quantile")

    def plot_exr_stat_by_condition(self, condition:pl.Expr|str|None = None, title:str = "超额收益统计"):
        return plot_rtn_stat(self.calc_exr_stat_by_condition(condition=condition), title=title)       

    def calc_exr_stat_by_year(self):
        return self.exr_date.with_columns(year=pl.col("date").dt.year()).group_by(["year", "factor_quantile"]).agg( self._idx_stat ).sort("year","factor_quantile")    

    def plot_exr_stat_by_year(self):
        return hv.Layout([plot_rtn_stat(data, title=f"{year[0]}年").opts(responsive=True) for year, data in self.calc_exr_stat_by_year().group_by("year")]).cols(4)

    def calc_exr_stat_by_month(self, year:int):
        return self.exr_date.filter( pl.col("date").dt.year() == year ).with_columns(month=pl.col("date").dt.month()).group_by(["month", "factor_quantile"]).agg( self._idx_stat ).sort("month", "factor_quantile")

    def plot_exr_stat_by_month(self, year:int):
        return hv.Layout([plot_rtn_stat(data, title=f"{year}年{month[0]}月").opts(responsive=True) for month, data in self.calc_exr_stat_by_month(year=year).group_by("month")]).cols(4)    

    def calc_gbr_stat_all(self):
        return self.gbr_date.group_by(["factor_quantile", "group"]).agg( self._idx_stat ).sort("group", "factor_quantile")

    def calc_gbr_stat_by_condition(self, condition:pl.Expr|str|None=None):
        data = self.gbr_date if condition is None else self.gbr_date.filter(condition) if isinstance(condition,pl.Expr) else self.gbr_date.query(condition)
        return data.group_by(["group", "factor_quantile"]).agg( self._idx_stat ).sort("group", "factor_quantile")

    def calc_gbr_stat_by_year(self):
        return self.gbr_date.with_columns(year=pl.col("date").dt.year()).group_by(["group", "year", "factor_quantile"]).agg( self._idx_stat ).sort("group", "year", "factor_quantile")
    
    def calc_gbr_stat_by_month(self, year:int):
        return self.gbr_date.filter( pl.col("date").dt.year() == year ).with_columns(month=pl.col("date").dt.month()).group_by(["group", "month", "factor_quantile"]).agg( self._idx_stat ).sort("group", "month", "factor_quantile")

    def plot_gbr_stat_all(self, title:str = "行业分组收益统计"):
        data = self.calc_gbr_stat_all()
        return hv.Layout([plot_rtn_stat(data.filter(pl.col("group") == g), title=f"{g} - {title}").opts(responsive=True) for g in data["group"].unique().sort()]).cols(4)

    def plot_gbr_stat_by_condition(self, condition:pl.Expr|str|None = None, title:str = "行业分组收益统计"):
        data = self.calc_gbr_stat_by_condition(condition)
        return hv.Layout([plot_rtn_stat(data.filter(pl.col("group") == g), title=f"{g} - {title}").opts(responsive=True) for g in data["group"].unique().sort()]).cols(4)

    def plot_gbr_stat_by_year(self):
        years = self.gbr_date["date"].dt.year().unique()
        layouts = []
        for year in years:
            data = self.calc_gbr_stat_by_condition(pl.col("date").dt.year() == year)
            group_plots = [plot_rtn_stat(data.filter(pl.col("group") == g), title=f"{g} - {year[0]}年").opts(responsive=True) for g in data["group"].unique().sort()]
            layouts.append(hv.Layout(group_plots).cols(4))
        return hv.Layout(layouts).cols(1)
      
    def calc_rtn_diff_all(self):
        return self.calc_rtn_diff_by_condition()
    
    def plot_rtn_diff_all(self, ma:int=22, title:str = "原始收益价差序列(d{dno})"):
        return hv.Layout([plot_rtn_diff(self.calc_rtn_diff_all(), dno=dno, ma=ma, title=title).opts(responsive=True) for dno in self.spec.periods]).cols(4)

    def calc_rtn_diff_by_condition(self,condition:pl.Expr|str|None=None):
        """原始收益价差: 基于原始收益序列`rtn_date`, 计算最大`qn`最小`q1`分箱在不同滞后区间上的未来收益价差`Δdn`序列与其标准误`SE(Δdn)`序列"""
        # 条件筛选
        rtn_date = self.rtn_date if condition is None else self.rtn_date.filter(condition) if isinstance(condition,pl.Expr) else self.rtn_date.query(condition)
        # 计算价差
        min_n, max_n = 1, self.spec.quantiles
        E_idx = [f"E(d{n})" for n in self.spec.periods]
        SE_idx = [f"SE(d{n})" for n in self.spec.periods]
        rtn_diff = (
            rtn_date.filter(pl.col("factor_quantile").is_in([min_n, max_n]))
            .pivot(values = E_idx + SE_idx, index=["date"], on="factor_quantile")
            .select([
                pl.col("date"),
                # E(收益价差) = E(qmax) - E(q1)
                *[(pl.col(f"E(d{n})_{max_n}") - pl.col(f"E(d{n})_{min_n}")).alias(f"E(Δd{n})") for n in self.spec.periods],
                # SE(收益价差) = sqrt( SE(qmax)^2 + SE(q1)^2 ), 严格来说如果 qmax q1 不独立, 应该为 sqrt( SE(qmax)^2 + SE(q1)^2 - 2*Cov(qmax, q1) )
                *[(pl.col(f"SE(d{n})_{max_n}").pow(2) + pl.col(f"SE(d{n})_{min_n}").pow(2)).sqrt().alias(f"SE(Δd{n})") for n in self.spec.periods]
            ])
        )
        return rtn_diff.sort("date")

    def plot_rtn_diff_by_condition(self, condition:pl.Expr|str|None = None, ma:int=22, title:str = "原始收益价差序列(d{dno})"):
        return hv.Layout([plot_rtn_diff(self.calc_rtn_diff_by_condition(condition), dno=dno, ma=ma, title=title) for dno in self.spec.periods]).cols(4)

    def calc_gbr_diff_all(self):
        return self.calc_gbr_diff_by_condition()
    
    def calc_gbr_diff_by_condition(self,condition:pl.Expr|str|None=None):
        """分组收益价差: 基于分组收益序列`gbr_date`, 计算最大`qn`最小`q1`分箱在不同滞后区间上的未来收益价差`Δdn`序列与其标准误`SE(Δdn)`序列"""
        # 条件筛选
        gbr_date = self.gbr_date if condition is None else self.gbr_date.filter(condition) if isinstance(condition,pl.Expr) else self.gbr_date.query(condition)
        # 计算价差        
        min_n, max_n = 1, self.spec.quantiles
        E_idx = [f"E(d{n})" for n in self.spec.periods]
        SE_idx = [f"SE(d{n})" for n in self.spec.periods]        
        gbr_diff = (
            gbr_date.filter(pl.col("factor_quantile").is_in([min_n, max_n]))
            .pivot(values=E_idx + SE_idx, index=["date","group"], on="factor_quantile")
            .select([pl.col("date"), pl.col("group"),
                # E(收益价差) = E(qmax) - E(q1)
                *[(pl.col(f"E(d{n})_{max_n}") - pl.col(f"E(d{n})_{min_n}")).alias(f"E(Δd{n})") for n in self.spec.periods],
                # SE(收益价差) = sqrt( SE(qmax)^2 + SE(q1)^2 ), 严格来说如果 qmax q1 不独立, 应该为 sqrt( SE(qmax)^2 + SE(q1)^2 - 2*Cov(qmax, q1) )
                *[(pl.col(f"SE(d{n})_{max_n}").pow(2) + pl.col(f"SE(d{n})_{min_n}").pow(2)).sqrt().alias(f"SE(Δd{n})") for n in self.spec.periods]
            ])
        )
        return gbr_diff.sort("date","group")

    def plot_gbr_diff_all(self, ma:int=22, title:str = "行业收益价差序列(d{dno})"):
        data = self.calc_gbr_diff_all()
        return hv.Layout([plot_rtn_diff(data.filter(pl.col("group") == g), dno=dno, ma=ma, title=f"{g} - {title}").opts(responsive=True) for g in data["group"].unique().sort() for dno in self.spec.periods]).cols(4)

    def plot_gbr_diff_by_condition(self, condition:pl.Expr|str|None = None, ma:int=22, title:str = "行业收益价差序列(d{dno})"):
        data = self.calc_gbr_diff_by_condition(condition)
        return hv.Layout([plot_rtn_diff(data.filter(pl.col("group") == g), dno=dno, ma=ma, title=f"{g} - {title}").opts(responsive=True) for g in data["group"].unique().sort() for dno in self.spec.periods]).cols(4)

    def _calc_gbr_cums_by_condition(self, condition:pl.Expr|str|None=None):
        gbr_date = self.gbr_date if condition is None else self.gbr_date.filter(condition) if isinstance(condition,pl.Expr) else self.gbr_date.query(condition)
        E_idx = [f"E(d{no})" for no in self.spec.periods]
        cum_rets = gbr_date.sort("date","group","factor_quantile").pivot(values = E_idx, index=["date","group"], on="factor_quantile").with_columns([
            pl.all().exclude("date","group") + 1,
            *[pl.row_index().mod(no).alias(f"i{no}") for no in self.spec.periods]
        ]).with_columns([
            pl.col(f"E(d{no})_{q}").cum_prod().over("group", f"i{no}") for no, q in product(self.spec.periods, range(1, self.spec.quantiles+1))
        ])
        cum_rets = pl.concat([cum_rets[["date","group"]]] + [
            cum_rets.pivot(values = f"E(d{no})_{q}", index=["date","group"], on=f"i{no}").fill_null(strategy="forward").select(pl.concat_list(pl.all().exclude("date","group")).list.mean().alias(f"CR(d{no},q{q})")) - 1
            for no, q in product(self.spec.periods, range(1, self.spec.quantiles+1))
        ], how="horizontal")
        return cum_rets.sort("date","group")

    def calc_gbr_cums_all(self):
        return self._calc_gbr_cums_by_condition()

    def calc_gbr_cums_by_condition(self, condition:pl.Expr|str|None = None):
        return self._calc_gbr_cums_by_condition(condition)

    def plot_gbr_cums_all(self, title: str = "行业累计收益序列(d{dno})"):
        data = self.calc_gbr_cums_all()
        return hv.Layout([plot_rtn_cums(data.filter(pl.col("group") == g), dno=dno, title=f"{g} - {title}").opts(responsive=True) for g in data["group"].unique().sort() for dno in self.spec.periods]).cols(4)

    def plot_gbr_cums_by_condition(self, condition:pl.Expr|str|None = None, title: str = "行业累计收益序列(d{dno})"):
        data = self.calc_gbr_cums_by_condition(condition)
        return hv.Layout([plot_rtn_cums(data.filter(pl.col("group") == g), dno=dno, title=f"{g} - {title}").opts(responsive=True) for g in data["group"].unique().sort() for dno in self.spec.periods]).cols(4)

    def calc_fac_rets_all(self):
        return self.calc_fac_rets_by_condition()
    
    def plot_fac_rets_all(self, title: str = "因子累计收益序列(分层多空)"):
        return plot_fac_rets(self.calc_fac_rets_all(), title = title)

    def calc_fac_rets_by_condition(self, condition:pl.Expr|str|None=None):
        """计算因子收益序列: 使用多空投资组合方式, 多头持有`qmax`, 空头持有`q1`"""
        # 条件筛选
        rtn_date = self.rtn_date if condition is None else self.rtn_date.filter(condition) if isinstance(condition,pl.Expr) else self.rtn_date.query(condition)
        # 计算累计
        min_n, max_n = 1, self.spec.quantiles
        E_idx = [f"E(d{no})" for no in self.spec.periods]
        # 累计收益序列
        cum_rets = rtn_date.sort("date","factor_quantile").pivot(values = E_idx, index=["date"], on="factor_quantile").select([
            pl.col("date"),
            *[( pl.col(f"E(d{no})_{max_n}") - pl.col(f"E(d{no})_{min_n}") ).alias(f"CR(Δd{no})") + 1 for no in self.spec.periods],
            *[pl.row_index().mod(no).alias(f"i{no}") for no in self.spec.periods]
        ]).with_columns([
            pl.col(f"CR(Δd{no})").cum_prod().over(f"i{no}") for no in self.spec.periods
        ])
        # 多组合平均
        cum_rets = pl.concat([cum_rets[["date"]]] + [
            cum_rets.pivot(values = f"CR(Δd{no})", index=["date"], on=f"i{no}").fill_null(strategy="forward").select(pl.concat_list(pl.all().exclude("date")).list.mean().alias(f"FR(d{no})")) - 1
            for no in self.spec.periods
        ], how="horizontal")     
        return cum_rets

    def plot_fac_rets_by_condition(self, condition:pl.Expr|str|None = None,title:str = "因子累计收益序列(分层多空)"):
        return plot_fac_rets(self.calc_fac_rets_by_condition(condition=condition), title=title)    

    def calc_fac_rets_by_year(self):
        years = self.rtn_date["date"].dt.year().unique()
        return pl.concat([
            self.calc_fac_rets_by_condition(pl.col("date").dt.year() == year).with_columns(year=pl.lit(year))
            for year in years
        ], how="vertical").sort("year", "date")

    def plot_fac_rets_by_year(self):
        return hv.Layout([
            plot_fac_rets(data, title=f"{year[0]}年因子累计收益序列(分层多空)").opts(responsive=True)
            for year, data in self.calc_fac_rets_by_year().group_by("year")
        ]).cols(1)

    def _calc_rtn_cums_by_condition(self,data:pl.DataFrame,condition:pl.Expr|str|None=None):
        """累计收益计算
        1. 获取每个分组`no`未来`n`日的收益序列`E(dn)_qno`;
        2. 按照间隔日期`n`, 分别计算开始于`t1`至`tn`的`n`个不同的累计净值序列`NV(dn,qno)@t1`至`NV(dn,qno-)@tn`;
        * `NV(dn,qno)@t1 = cumprod( E(dn,qno,t1) + 1, E(dn,qno,tn+1) + 1, E(dn,qno,t2n+1) + 1, ... ) - 1`
        * `NV(d2,qno)@t2 = cumprod( E(dn,qno,t2) + 1, E(dn,qno,tn+2) + 1, E(dn,qno,t2n+2) + 1, ... ) - 1`
        * ...
        * `NV(d2,qno)@tn = cumprod( E(dn,qno,tn) + 1, E(dn,qno,tn+n) + 1, E(dn,qno,tnn+n) + 1, ... ) - 1`
        3. 使用上述计算出来的`n`组累计净值序列, 先前值填充, 然后在每个时间点上进行平均后减去1, 得到`CR(dn,qno)`序列;
        * `CR(dn,qno,tn) = ( NV(dn,qno,tn)@t1 + NV(dn,qno,tn)@t2 + ... + NV(dn,qno,tn)@tn ) / n - 1`
        4. 汇总所有`no`个分组的`n`个滞后市场的累计收益序列`CR(dn,qno)`, 共计`count(n)*no`个累计收益序列;
        """
        # 条件筛选
        rtn_date = data if condition is None else data.filter(condition) if isinstance(condition,pl.Expr) else data.query(condition)
        # 计算累计
        E_idx = [f"E(d{no})" for no in self.spec.periods]
        # 累计收益序列
        cum_rets = rtn_date.sort("date","factor_quantile").pivot(values = E_idx, index=["date"], on="factor_quantile").with_columns([
            pl.all().exclude("date") + 1,
            *[pl.row_index().mod(no).alias(f"i{no}") for no in self.spec.periods]
        ]).with_columns([
            pl.col(f"E(d{no})_{q}").cum_prod().over(f"i{no}") for no, q in product(self.spec.periods, range(1, self.spec.quantiles+1))
        ])
        cum_rets = pl.concat([cum_rets[["date"]]] + [
            cum_rets.pivot(values = f"E(d{no})_{q}", index=["date"], on=f"i{no}").fill_null(strategy="forward").select(pl.concat_list(pl.all().exclude("date")).list.mean().alias(f"CR(d{no},q{q})")) - 1
            for no, q in product(self.spec.periods, range(1, self.spec.quantiles+1))
        ], how="horizontal")
        return cum_rets.sort("date") 

    def calc_rtn_cums_all(self):
        return self.calc_rtn_cums_by_condition()

    def plot_rtn_cums_all(self, title: str = "累计收益序列(d{dno})"):
        return hv.Layout([plot_rtn_cums(self.calc_rtn_cums_all(),dno=dno, title=title).opts(responsive=True) for dno in self.spec.periods]).cols(4)
    
    def calc_rtn_cums_by_condition(self, condition:pl.Expr|str|None = None):
        return self._calc_rtn_cums_by_condition(self.rtn_date, condition)
    
    def plot_rtn_cums_by_condition(self, condition:pl.Expr|str|None = None,title: str = "累计收益序列(d{dno})"):
        return hv.Layout([plot_rtn_cums(self.calc_rtn_cums_by_condition(condition),dno=dno, title=title).opts(responsive=True) for dno in self.spec.periods]).cols(4)

    def calc_rtn_cums_by_year(self):
        return pl.concat([self.calc_rtn_cums_by_condition(pl.col("date").dt.year() == year) for year in self.rtn_date["date"].dt.year().unique()], how="vertical").with_columns(pl.col("date").dt.year().alias("year"))

    def plot_rtn_cums_by_year(self, dno:int = 10):        
        return hv.Layout([plot_rtn_cums(data, title=f"{year[0]}年(d{dno})", dno=dno).opts(responsive=True) for year, data in self.calc_rtn_cums_by_year().group_by("year")]).cols(4).opts(shared_axes=False)
    
    def calc_exr_cums_all(self):
        return self.calc_exr_cums_by_condition()

    def plot_exr_cums_all(self, title: str = "累计超额序列(d{dno})"):
        return hv.Layout([plot_rtn_cums(self.calc_exr_cums_all(),dno=dno, title=title).opts(responsive=True) for dno in self.spec.periods]).cols(4)
    
    def calc_exr_cums_by_condition(self, condition:pl.Expr|str|None = None):
        return self._calc_rtn_cums_by_condition(self.exr_date, condition)
    
    def plot_exr_cums_by_condition(self, condition:pl.Expr|str|None = None,title: str = "累计超额序列(d{dno})"):
        return hv.Layout([plot_rtn_cums(self.calc_exr_cums_by_condition(condition),dno=dno, title=title).opts(responsive=True) for dno in self.spec.periods]).cols(4)

    def calc_exr_cums_by_year(self):
        return pl.concat([self.calc_exr_cums_by_condition(pl.col("date").dt.year() == year) for year in self.exr_date["date"].dt.year().unique()], how="vertical").with_columns(pl.col("date").dt.year().alias("year"))

    def plot_exr_cums_by_year(self, dno:int = 10):        
        return hv.Layout([plot_rtn_cums(data, title=f"{year[0]}年(d{dno})", dno=dno).opts(responsive=True) for year, data in self.calc_exr_cums_by_year().group_by("year")]).cols(4).opts(shared_axes=False)    
   
def BucketReturnsTest(tester: FactorTester) -> BucketReturnsTestResults:
    """收益数据计算
    - 原始收益序列: 按日期`date`和因子分箱`factor_quantile`计算的未来收益`dn`的均值`E(dn)`和标准误`SE(dn)`;
    - 原始收益统计: 基于原始收益序列, 按因子分箱`factor_quantile`计算的`E(dn)`的均值`mean(E(dn))`和标准误`std(E(dn))`;
    - 超额收益序列:
        1. 计算超额: `dn_t - E(dn_t)`
        2. 计算均值和标准误: `mean(dn_t - E(dn_t))`和`std(dn_t - E(dn_t))`;
    - 超额收益统计: 基于超额收益序列, 按因子分箱`factor_quantile`计算的超额收益的均值和标准误;
    - 分组收益序列:
        1. 计算分组超额: `dn_t - E(dn_t|group)`
        2. 计算均值和标准误: `mean(dn_t - E(dn_t|group))`和`std(dn_t - E(dn_t|group))`;
    """
    # 筛选截面样本
    factor_data = tester.factor_data.filter( c.sample==1 )

    # 日度统计指标
    idx_date = sum([[
        pl.col(f"d{n}").mean().alias(f"E(d{n})"),
        (pl.col(f"d{n}").std() / pl.col(f"d{n}").count().sqrt()).alias(f"SE(d{n})"),
    ] for n in tester.spec.periods],[])
    
    # 1.原始收益
    rtn_date = factor_data.group_by(["date", "factor_quantile"]).agg( idx_date )
    
    # 2.超额收益
    exr_date = factor_data.with_columns([
        (pl.col(f"d{n}") - pl.col(f"d{n}").mean().over("date")).alias(f"d{n}")
        for n in tester.spec.periods
    ]).group_by(["date", "factor_quantile"]).agg( idx_date )
    
    # 3.分组超额
    gbr_date = factor_data.with_columns([
        (pl.col(f"d{n}") - pl.col(f"d{n}").mean().over("date","group")).alias(f"d{n}")
        for n in tester.spec.periods
    ]).group_by(["date", "factor_quantile", "group"]).agg( idx_date )
    
    # 4.收益统计
    _idx_stat = sum([[
            pl.col(f"E(d{n})").mean().alias(f"E(d{n})"),
            (pl.col(f"E(d{n})").std() / pl.col(f"SE(d{n})").count().sqrt()).alias(f"SE(d{n})"),
        ] for n in tester.spec.periods],[])

    return BucketReturnsTestResults(
        spec=tester.spec,
        rtn_date=rtn_date.sort("date", "factor_quantile"),
        exr_date=exr_date.sort("date", "factor_quantile"),
        gbr_date=gbr_date.sort("date", "group", "factor_quantile"),
        _idx_stat=_idx_stat
    )


def _calc_factor_returns(
    tester: FactorTester,
    weight: pl.Expr,
    group_adjust: bool = False,
    return_weight: bool = False,
    calc_expr: pl.Expr|None = None,
) -> pl.DataFrame:
    """根据指定权重计算因子收益"""
    # 筛选截面样本
    factor_data = tester.factor_data.filter( c.sample==1 )
    # 提起计算
    if not calc_expr is None: factor_data = factor_data.with_columns( calc_expr )
    # 计算权重
    factor_data = factor_data.with_columns( 
        # 判断分组调整
        weight.over(pl.col("date") if not group_adjust else [pl.col("date"),pl.col("group")]).alias("weight")
    ).with_columns(
        # 权重归一
        ( pl.col("weight") / pl.col("weight").abs().sum() ).over("date").alias("weight")
    )
    if return_weight: return factor_data
    # 计算因子收益
    fac_rets = factor_data.select(["date", "sym"] + [(pl.col("weight") * pl.col(f"d{no}")).alias(f"d{no}") for no in tester.spec.periods])
    # 单日收益
    fac_rets = fac_rets.group_by("date").agg([pl.col(f"d{no}").sum().alias(f"FR(d{no})") for no in tester.spec.periods])
    # 累计收益
    fac_cret = fac_rets.sort("date").with_columns([
        pl.all().exclude("date") + 1,
        *[pl.row_index().mod(no).alias(f"i{no}") for no in tester.spec.periods]
    ]).with_columns([
        pl.col(f"FR(d{no})").cum_prod().over(f"i{no}") for no in tester.spec.periods
    ])
    fac_cret = pl.concat([fac_cret[["date"]]] + [
        fac_cret.pivot(values = f"FR(d{no})", index=["date"], on=f"i{no}").fill_null(strategy="forward").select(pl.concat_list(pl.all().exclude("date")).list.mean().alias(f"FR(d{no})")) - 1
        for no in tester.spec.periods
    ], how="horizontal").sort("date")

    return fac_rets, fac_cret

@dataclass(frozen=True, slots=True)
class FactorReturnsTestResults:
    spec: FactorTesterSpec
    因子加权多空中性日度收益: pl.DataFrame
    因子加权多空中性累计收益: pl.DataFrame
    多空等权多空中性日度收益: pl.DataFrame
    多空等权多空中性累计收益: pl.DataFrame
    因子加权原始方向日度收益: pl.DataFrame
    因子加权原始方向累计收益: pl.DataFrame
    个股等权原始方向日度收益: pl.DataFrame
    个股等权原始方向累计收益: pl.DataFrame
    个股等权多空中性日度收益: pl.DataFrame
    个股等权多空中性累计收益: pl.DataFrame
    多空等权原始方向日度收益: pl.DataFrame
    多空等权原始方向累计收益: pl.DataFrame
    行业中性因子加权多空中性日度收益: pl.DataFrame
    行业中性因子加权多空中性累计收益: pl.DataFrame
    行业中性多空等权多空中性日度收益: pl.DataFrame
    行业中性多空等权多空中性累计收益: pl.DataFrame
    行业中性因子加权原始方向日度收益: pl.DataFrame
    行业中性因子加权原始方向累计收益: pl.DataFrame
    行业中性个股等权原始方向日度收益: pl.DataFrame
    行业中性个股等权原始方向累计收益: pl.DataFrame
    行业中性个股等权多空中性日度收益: pl.DataFrame
    行业中性个股等权多空中性累计收益: pl.DataFrame
    行业中性多空等权原始方向日度收益: pl.DataFrame
    行业中性多空等权原始方向累计收益: pl.DataFrame

    def plot_fac_rets(self, title: str = "因子累计收益序列", **kwargs):
        """因子累计收益序列"""
        return hv.Layout([
            plot_fac_rets(self.行业中性因子加权多空中性累计收益, title="行业中性因子加权多空中性累计收益[W=F-F̄; D=±(F-F̄)]").opts(width=450),
            plot_fac_rets(self.行业中性因子加权原始方向累计收益, title="行业中性因子加权原始方向累计收益[W=F; D=±(F)]").opts(width=450),
            plot_fac_rets(self.因子加权多空中性累计收益, title="因子加权多空中性累计收益[W=F-F̄; D=±(F-F̄)]").opts(width=450),
            plot_fac_rets(self.因子加权原始方向累计收益, title="因子加权原始方向累计收益[W=F; D=±(F)]").opts(width=450),
            plot_fac_rets(self.行业中性多空等权多空中性累计收益, title="行业中性多空等权多空中性累计收益[W=-1/N,+1/M; D=±(F-F̄)]").opts(width=450),
            plot_fac_rets(self.行业中性多空等权原始方向累计收益, title="行业中性多空等权原始方向累计收益[W=-1/N,+1/M; D=±(F)]").opts(width=450),
            plot_fac_rets(self.多空等权多空中性累计收益, title="多空等权多空中性累计收益[W=-1/N,+1/M; D=±(F-F̄)]").opts(width=450),
            plot_fac_rets(self.多空等权原始方向累计收益, title="多空等权原始方向累计收益[W=-1/N,+1/M; D=±(F)]").opts(width=450),
            plot_fac_rets(self.行业中性个股等权多空中性累计收益, title="行业中性个股等权多空中性累计收益[W=±1/N; D=±(F-MED(F))]").opts(width=450),
            plot_fac_rets(self.行业中性个股等权原始方向累计收益, title="行业中性个股等权原始方向累计收益[W=±1/N; D=±(F)]").opts(width=450),
            plot_fac_rets(self.个股等权多空中性累计收益, title="个股等权多空中性累计收益[W=±1/N; D=±(F-MED(F))]").opts(width=450),
            plot_fac_rets(self.个股等权原始方向累计收益, title="个股等权原始方向累计收益[W=±1/N; D=±(F)]").opts(width=450),
        ]).cols(4).opts(shared_axes=False)


def FactorReturnsTest(tester: FactorTester) -> FactorReturnsTestResults:
    """因子日度收益计算: 按照指定条件构建每日因子组合, 并计算因子组合的收益表现, 
    >>> # `factor = -1, 1, 3, 9`
    >>> factor_data = pl.DataFrame({"date":[1,1,1,1], "factor":[-1,1,3,9]})

    >>> # 1. 因子加权多空中性: 多空方向取决于因子值是否超过均值, 多空权重之和一定为`0`, 个股权重正比于因子绝对值;
    >>> # `w = factor - E(factor)`
    >>> # `w = w / sum(abs(w))`
    >>> # `w = -4/12, -2/12, 0/12, 6/12`
    >>> _calc_factor_returns(tester=None, pl.col("factor") - pl.col("factor").mean(), return_weight=True, factor_data=factor_data)
   
    >>> # 2. 多空等权多空中性: 多空方向取决于因子值是否超过均值, 多空权重之和一定为`0`, 个股权重在多空内部相等;
    >>> # `w = sign(factor - E(factor))`
    >>> # `w(factor>E(factor)) = w / count(factor|factor>E(factor))`
    >>> # `w(factor<E(factor)) = w / count(factor|factor<E(factor))`
    >>> # `w = -1/4, -1/4, 0, 2/4`
    >>> fd = factor_data.with_columns(factor=( pl.col("factor") - pl.col("factor").mean() ).over("date"))
    >>> _calc_factor_returns(tester=None, (pl.col("factor").sign() / pl.col("factor").count()).over(pl.col("factor").sign()), return_weight=True, factor_data=factor_data)

    >>> # 3. 因子加权原始方向: 多空方向取决于因子值方向, 多空权重之和可能不为`0`, 个股权重正比于因子绝对值;
    >>> # `w = factor`
    >>> # `w = w / sum(abs(w))`
    >>> # `w = -1/14, 1/14, 3/14, 9/14`
    >>> _calc_factor_returns(tester=None, pl.col("factor"), return_weight=True, factor_data=factor_data)

    >>> # 4. 个股等权原始方向: 多空方向取决于因子值方向, 多空权重之和可能不为`0`, 个股权重在组合整体相等;
    >>> # `w = sign(factor)`
    >>> # `w = w / sum(abs(w))`
    >>> # `w = -1/4, 1/4, 1/4, 1/4`
    >>> _calc_factor_returns(tester=None, pl.col("factor").sign(), return_weight=True, factor_data=factor_data)

    >>> # 5. 个股等权多空中性: 多空方向取决于因子值是否超过中位数, 多空权重之和一定为`0`, 个股权重在组合整体相等;
    >>> # `w = sign(factor - median(factor))`
    >>> # `w = w / sum(abs(w))`
    >>> # `w = -1/4, -1/4, 1/4, 1/4`
    >>> _calc_factor_returns(tester=None, (pl.col("factor") - pl.col("factor").median()).sign(), return_weight=True, factor_data=factor_data)

    >>> # 6. 多空等权原始方向: 多空方向取决于因子值方向, 多空权重之和一定为`0`, 个股权重在多空内部相等;
    >>> # `w = sign(factor)`
    >>> # `w(factor>0) = w / count(factor|factor>0)`
    >>> # `w(factor<0) = w / count(factor|factor<0)`
    >>> # `w = -3/6, 1/6, 1/6, 1/6`
    >>> _calc_factor_returns(tester=None, (pl.col("factor").sign() / pl.col("factor").count()).over(pl.col("factor").sign()), return_weight=True, factor_data=factor_data)
    """
    因子加权多空中性日度收益, 因子加权多空中性累计收益 = _calc_factor_returns(tester, weight = pl.col("factor") - pl.col("factor").mean(), group_adjust=False)
    多空等权多空中性日度收益, 多空等权多空中性累计收益 = _calc_factor_returns(
                                                            tester = tester, 
                                                            weight = (pl.col("factor").sign() / pl.col("factor").count()).over(pl.col("factor").sign()),
                                                            group_adjust=False,
                                                            calc_expr = ( pl.col("factor") - pl.col("factor").mean() ).over("date").alias("factor"))
    因子加权原始方向日度收益, 因子加权原始方向累计收益 = _calc_factor_returns(tester, weight = pl.col("factor"), group_adjust=False)
    个股等权原始方向日度收益, 个股等权原始方向累计收益 = _calc_factor_returns(tester, weight = pl.col("factor").sign(), group_adjust=False)
    个股等权多空中性日度收益, 个股等权多空中性累计收益 = _calc_factor_returns(tester, weight = (pl.col("factor") - pl.col("factor").median()).sign(), group_adjust=False)
    多空等权原始方向日度收益, 多空等权原始方向累计收益 = _calc_factor_returns(tester, weight = (pl.col("factor").sign() / pl.col("factor").count()).over(pl.col("factor").sign()), group_adjust=False)

    行业中性因子加权多空中性日度收益, 行业中性因子加权多空中性累计收益 = _calc_factor_returns(tester, weight = pl.col("factor") - pl.col("factor").mean(), group_adjust=True)
    行业中性多空等权多空中性日度收益, 行业中性多空等权多空中性累计收益 = _calc_factor_returns(
                                                            tester = tester, 
                                                            weight = (pl.col("factor").sign() / pl.col("factor").count()).over(pl.col("factor").sign()),
                                                            group_adjust=True,
                                                            calc_expr = ( pl.col("factor") - pl.col("factor").mean() ).over("date").alias("factor"))                           
    行业中性因子加权原始方向日度收益, 行业中性因子加权原始方向累计收益 = _calc_factor_returns(tester, weight = pl.col("factor"), group_adjust=True)
    行业中性个股等权原始方向日度收益, 行业中性个股等权原始方向累计收益 = _calc_factor_returns(tester, weight = pl.col("factor").sign(), group_adjust=True)
    行业中性个股等权多空中性日度收益, 行业中性个股等权多空中性累计收益 = _calc_factor_returns(tester, weight = (pl.col("factor") - pl.col("factor").median()).sign(), group_adjust=True)
    行业中性多空等权原始方向日度收益, 行业中性多空等权原始方向累计收益 = _calc_factor_returns(tester, weight = (pl.col("factor").sign() / pl.col("factor").count()).over(pl.col("factor").sign()), group_adjust=True)

    return FactorReturnsTestResults(
        spec=tester.spec,
        因子加权多空中性日度收益=因子加权多空中性日度收益.sort("date"),
        因子加权多空中性累计收益=因子加权多空中性累计收益.sort("date"),
        多空等权多空中性日度收益=多空等权多空中性日度收益.sort("date"),
        多空等权多空中性累计收益=多空等权多空中性累计收益.sort("date"),
        因子加权原始方向日度收益=因子加权原始方向日度收益.sort("date"),
        因子加权原始方向累计收益=因子加权原始方向累计收益.sort("date"),
        个股等权原始方向日度收益=个股等权原始方向日度收益.sort("date"),
        个股等权原始方向累计收益=个股等权原始方向累计收益.sort("date"),
        个股等权多空中性日度收益=个股等权多空中性日度收益.sort("date"),
        个股等权多空中性累计收益=个股等权多空中性累计收益.sort("date"),
        多空等权原始方向日度收益=多空等权原始方向日度收益.sort("date"),
        多空等权原始方向累计收益=多空等权原始方向累计收益.sort("date"),
        行业中性因子加权多空中性日度收益=行业中性因子加权多空中性日度收益.sort("date"),
        行业中性因子加权多空中性累计收益=行业中性因子加权多空中性累计收益.sort("date"),
        行业中性多空等权多空中性日度收益=行业中性多空等权多空中性日度收益.sort("date"),
        行业中性多空等权多空中性累计收益=行业中性多空等权多空中性累计收益.sort("date"),
        行业中性因子加权原始方向日度收益=行业中性因子加权原始方向日度收益.sort("date"),
        行业中性因子加权原始方向累计收益=行业中性因子加权原始方向累计收益.sort("date"),
        行业中性个股等权原始方向日度收益=行业中性个股等权原始方向日度收益.sort("date"),
        行业中性个股等权原始方向累计收益=行业中性个股等权原始方向累计收益.sort("date"),
        行业中性个股等权多空中性日度收益=行业中性个股等权多空中性日度收益.sort("date"),
        行业中性个股等权多空中性累计收益=行业中性个股等权多空中性累计收益.sort("date"),
        行业中性多空等权原始方向日度收益=行业中性多空等权原始方向日度收益.sort("date"),
        行业中性多空等权原始方向累计收益=行业中性多空等权原始方向累计收益.sort("date"),
    )

if __name__ == "__main__":
    from wsdata import WSData
    from wsdata.utils.cache import SetCache, GetCache
    from ..zoo.size.log_market_cap import factor
    factor = factor.build(WSData.query("from sf_base").pl())
    tester = FactorTester(factor,FactorTesterSpec()).perpare_data(sf_base=WSData.query("from sf_base where date>='2020-01-01'").pl(), groupby=pl.col("/inc/sw2021"))
    SetCache(tester,"tester")
    tester = GetCache("tester")

