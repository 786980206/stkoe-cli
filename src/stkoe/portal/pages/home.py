import pandas as pd
from ..components.table import PerspectiveTable

# data = get_cnstk_klday().head(100000).to_pandas()
data = pd.DataFrame({"A":[1] * 1000, "B":[2] * 1000})

chart1 = PerspectiveTable(data, )

chart2 = PerspectiveTable(data)

chart3 = PerspectiveTable(data)
