# export_csv_splits.py

import dask.dataframe as dd

input_path = "/d/hpc/projects/FRI/bigdata/students/gk40784/bigdata_hw5_output/partitioned_parquet"
ddf = dd.read_parquet(input_path, engine="pyarrow")
ddf["year"] = ddf["year"].astype("int64")
#pre = ddf[ddf["year"].between(2016, 2019)]
#during = ddf[ddf["year"].between(2020, 2021)]
post = ddf[ddf["year"].between(2022, 2025)]

#pre.to_csv("pre_pandemic.csv", single_file=True, index=False)
#during.to_csv("during_pandemic.csv", single_file=True, index=False)
post.to_csv("post_pandemic.csv", single_file=True, index=False)
