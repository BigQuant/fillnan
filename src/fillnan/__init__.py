"""fillnan package.

fill nan
"""

import structlog
from bigmodule import I

# 需要安装的第三方依赖包
# from bigmodule import R
# R.require("requests>=2.0", "isort==5.13.2")

# metadata
# 模块作者
author = "BigQuant"
# 模块分类
category = "数据处理"
# 模块显示名
friendly_name = "缺失数据填充"
# 文档地址, optional
doc_url = "https://bigquant.com/wiki/"
# 是否自动缓存结果
cacheable = True

logger = structlog.get_logger()

def run(
    input_data: I.port("输入数据源", specific_type_name="DataSource"),  # type: ignore
    features: I.port("特征列表", specific_type_name="列表|DataSource") = None,  # type: ignore
    fill_value: I.str("填充值，支持数值/mean/median，默认填充0.0") = "0.0",  # type: ignore
)->[
    I.port("输出数据", "data")  # type: ignore
]:
    """

    数据缺失处理，对所有列的NaN按平均值(mean)，中位数(median)填充，也可指定值。

    """

    import dai

    if not features:
        logger.error("特征列不能为空")
        return

    df = input_data.read()
    features = features.read()

    if fill_value not in ("mean", "median"):
        try:
            fill_value = float(fill_value)
        except Exception:
            logger.warning("fill_value转换float出错，使用默认值0.0")
            fill_value = 0.0

    if fill_value == "mean":
        means = df[features].mean()
        df[features] = df[features].fillna(means)
    elif fill_value == "median":
        medians = df[features].median()
        df[features] = df[features].fillna(medians)
    else:
        df[features] = df[features].fillna(fill_value)

    data = dai.DataSource.write_bdb(df)
    return I.Outputs(data=data)


def post_run(outputs):
    """后置运行函数"""
    return outputs
