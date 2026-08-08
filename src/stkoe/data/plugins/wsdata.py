from wsdata import WSData, pl
from wsdata import get_cnstk_tdcal, get_cnstk_bicif, get_cnstk_klday, get_cnstk_incls
from functools import lru_cache


@lru_cache
def get_feature_data(name: str) -> pl.DataFrame:
    name = WSData(f"SELECT CONCAT_WS('.', table_catalog, table_schema, table_name) FROM information_schema.tables WHERE table_name = '{name}' LIMIT 1").pl().item()
    return WSData(f"from {name} order by date, sym").pl()


@lru_cache
def get_common_data() -> pl.DataFrame:
    return WSData('select date, sym, "/inc/sw2021" as ic from cnstk_fazoo.fzb.fzb_base').pl()


@lru_cache
def get_cnstk_tdday() -> pl.DataFrame:
    return WSData('select date from cnstk_lfreq.idx.cnstk_tdday order by date').pl()
