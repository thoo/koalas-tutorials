# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:percent
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.2'
#       jupytext_version: 1.2.4
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

import os
import sys

# %%
import databricks.koalas as ks

# %%
import numpy as np
import pandas as pd


# %% [markdown]
# ### Read CSV File.

# %%
# location of data
data_path = os.path.join("data", "nyc_restaurant_inspection_results_sample1.csv")

# %%
# import to kolas df
df = ks.read_csv(data_path)

# %%
# import to pandas df
pddf = pd.read_csv(data_path)

# %% [markdown]
# ### Memory usage

# %%
print("koalas memory usage is {m} bytes.".format(m=sys.getsizeof(df)))
print(
    "pandas memory usage is {m:.2f} kilobytes.".format(m=sys.getsizeof(pddf) / 10 ** 3)
)

# %% [markdown]
# ##  Selecting Rows and Columns
# ### Using __loc__

# %%
df.loc[90:100, "DBA"]

# %% [markdown]
# ### Difference between pandas and koalas in __iloc__ usage
#
# __iloc__ in koalas  does not allow the beginning of the row index to be assigned.
#
# i.e. __df.iloc[0:10,1]__ or __df.iloc[20:30,1:4]__ will not work in koalas.
#
# However, __df.iloc[:10,1]__ or __df.iloc[:30,1:4]__ will work.

# %%
# In koalas the above selection won't work.
df.iloc[:10, 1:4]

# %%
df.loc[:5, ["INSPECTION DATE", "Census Tract"]]

# %%
(
    df.loc[
        (df["Census Tract"] > 10000) & (df["BORO"] == "Brooklyn"),
        ["INSPECTION DATE", "Census Tract", "BORO"],
    ]
).head()

# %% [markdown]
# ## Column Manipulations

# %% [markdown]
# ### Change column type

# %%
df["INSPECTION DATE"] = df["INSPECTION DATE"].astype(str)

# %% [markdown]
# ### Creating New Columns
#  Using __DataFrame.assign__, a new column can be created but it will also generate the new dataframe where the new column is attached to the previous dataframe. In the following, we convert *inspection_date* column from __str__ to __datetime__ column.

# %%
df_new = df.assign(
    inspection_date_dt=lambda x: ks.to_datetime(
        x["INSPECTION DATE"], format="%m/%d/%Y", errors="coerce"
    )
)
df_new.head(3)

# %%
df_new["inspection_date_dt"].head()

# %% [markdown]
# ### Filter By Datetime

# %%
(df_new.loc[df_new["inspection_date_dt"].dt.year > 2017].head())

# %%
df_new["BORO"].value_counts()

# %% [markdown]
# ### Currently index to list is not available. So the workaround is to convert the index to a column and then convert to a list.
# We have about 59 different descriptions for `cuisine_description` (`['American', 'Chinese', 'Pizza', 'Italian',...` ) and we are going to keep the top five descriptions and replace the reset with 'other'.

# %%
# 53 unique cuisine description.
# ks.unique is not working right now.
df_new["CUISINE DESCRIPTION"].value_counts().shape

# %%
top5_cuisines = (
    df_new["CUISINE DESCRIPTION"]
    .value_counts()
    .head(5)
    .reset_index()
    .iloc[:, 0]
    .tolist()
)
print(top5_cuisines)


# %%
# Function has to have return type hint. This is different from pandas.
def replace_cuisines(x, list2exclude) -> str:
    if x not in list2exclude:
        x = "other"
    return x


# %%
# add new column and reasign to the previous dataframe df_new
df_new = df_new.assign(
    cuisine_mod=df_new["CUISINE DESCRIPTION"].apply(
        replace_cuisines, args=(top5_cuisines,)
    )
)

# %%
df_new.head()

# %%
table = df.pivot_table(
    values=r"SCORE", columns="CUISINE DESCRIPTION", index=["BORO"], aggfunc="sum"
)
table

# %% [markdown]
# ## Save File

# %%
# Write a file to a folder named 'data'.
df_new.to_csv(os.path.join("data", "sample_mod.csv"))

# %% [markdown]
# ## Merge, Join & Concatenate

# %%
file1 = os.path.join("data", "nyc_restaurant_inspection_results_sample1.csv")
file2 = os.path.join("data", "nyc_restaurant_inspection_results_sample2.csv")
df1 = ks.read_csv(file1)
df2 = ks.read_csv(file2)

print("df1 dimension = {}".format(df1.shape))
print("df2 dimension = {}".format(df2.shape))

# %%
join_df = df1.append(df2, ignore_index=True)

# %%
print(join_df.shape)

# %%
df1.iloc[:10, :3].join(df2.iloc[:10, 2:4], rsuffix="_right")

# %%
