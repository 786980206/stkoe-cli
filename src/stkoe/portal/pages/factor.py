import panel as pn
import pickle
ret = pickle.load(open(r"E:/DataCenter/wslib/src/wsdata/der/factor_zoo/ret.pkl", "rb"))
# print(ret)
# 累计收益率序列
rtn_cums_all = pn.Column(ret.plot_rtn_cums_all(), scroll=True)

