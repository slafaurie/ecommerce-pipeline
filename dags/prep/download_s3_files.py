from common.data_model import DataModel
from prep.prep_utils.constants import FilePath


DataModel.set_mode(local=False)
df = DataModel.read_dataframe(FilePath.ZONE, FilePath.ORDERS, "csv")
print(df.head())