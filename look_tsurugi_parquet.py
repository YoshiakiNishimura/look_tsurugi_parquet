import pandas as pd
import glob
import os

directory_path = "result/test_table"
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.expand_frame_repr', False)
parquet_files = sorted(glob.glob(os.path.join(directory_path, "*.parquet")))
data_frames = []
for file in parquet_files:
    df = pd.read_parquet(file)
    data_frames.append(df)
combined_data = pd.concat(data_frames, ignore_index=True)
sorted_data = combined_data.sort_values(by='id', ascending=True)
sorted_data_hex = sorted_data.copy()
sorted_data_hex['id'] = sorted_data['id'].apply(lambda x: f"{x:#010x}")
sorted_data_hex['name'] = sorted_data['name'].apply(lambda x: f"{x:#010x}")
sorted_data_hex['note'] = sorted_data['note'].apply(lambda x: f"{x:#010x}")
print(sorted_data_hex)
