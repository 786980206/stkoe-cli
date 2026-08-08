import polars as pl
import datetime
import yaml
import shutil
from functools import reduce
from . import STKOE_LOCAL_DATA, logger, ResponseData, SYS_COLS
from .dataset import describe as describe_dataset, select as select_dataset

def create(field_name, dataset, formula=None, **meta_input):
    """新建指标"""
    dataset_meta_ret = describe_dataset(dataset)
    if not dataset_meta_ret.success: return dataset_meta_ret

    # 创建 field 文件夹
    field_folder = STKOE_LOCAL_DATA / "fields" / field_name
    field_folder.mkdir(parents=True, exist_ok=True)

    # 生成 meta 信息
    now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    meta = {
        "display_name": field_name,
        "description": "can be modified by user",
        "data_type": None,
        "unit": None,
        "tags": [],
        "dataset": dataset,
        "formula": formula,
        "materialized": False,
        "materialized_time": None,
        "create_time": now,
        "update_time": now,
    } | meta_input

    # 生成配置文件
    meta_file = field_folder / "_meta.yaml"
    with open(meta_file, 'w', encoding='utf-8') as f:
        yaml.dump(meta, f, allow_unicode=True, default_flow_style=False, sort_keys=False)
    logger.debug( f"Field [{field_name}] created successfully" )
    return ResponseData(data=meta)    

if __name__ == "__main__":
    create("test_field", "test_success")