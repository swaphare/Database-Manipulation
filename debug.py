import mini_project2
import pandas as pd
# from pandas.util import assert_frame_equal
normalized_database_filename = 'normalized.db'
data_filename = 'data.csv'
# mini_project2.step7_create_productcategory_table(data_filename, normalized_database_filename)
data = pd.read_csv("step7.csv")
conn = mini_project2.create_connection(normalized_database_filename)
df = pd.read_sql_query("""SELECT * FROM ProductCategory""", conn)
print(df)
print(data)
