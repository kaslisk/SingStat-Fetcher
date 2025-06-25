from file_manager import *
from client import *
from cleaner import *

table = fetch_table_data("M183761")
cleaned = to_df(table)
save_csv(cleaned, "M183761")