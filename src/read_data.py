import pandas as pd
import re

df_MovieLens = pd.read_csv('./data/MovieLens.csv',index_col=0)
df_IMDB = pd.read_csv('./data/IMDB_top_250.csv',index_col=0)
df_Douban = pd.read_csv('./data/Douban_top_250.csv',index_col=0)
df_Yahoo = pd.read_csv('./data/Yahoo_top_500.csv',index_col=0)

# The codes below are dealing with the NaN issues in csv format.
for column_name in df_Yahoo:
    if df_Yahoo[column_name].dtype=='object':
        df_Yahoo[column_name] = df_Yahoo[column_name].fillna('')
    else:
        df_Yahoo[column_name] = df_Yahoo[column_name].fillna(0)

for column_name in df_IMDB:
    if df_IMDB[column_name].dtype=='object':
        df_IMDB[column_name] = df_IMDB[column_name].fillna('')
    else:
        df_IMDB[column_name] = df_IMDB[column_name].fillna(0)

for column_name in df_Douban:
    if df_Douban[column_name].dtype=='object':
        df_Douban[column_name] = df_Douban[column_name].fillna('')
    else:
        df_Douban[column_name] = df_Douban[column_name].fillna(0)

