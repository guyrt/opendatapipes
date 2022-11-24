import argparse
import numpy as np
import pandas as pd
import os

import logging

parser = argparse.ArgumentParser()
parser.add_argument("--raw_clarity_data", type=str)
parser.add_argument("--clean_clarity_data", type=str)
args = parser.parse_args()

uri = args.raw_clarity_data
output_uri = args.clean_clarity_data


logging.info(f'URI is {uri}')

# instantiate file system using datastore URI
# fs = AzureMachineLearningFileSystem(uri)

pandas_frames = []
# for path in fs.ls():
#     logging.info(f'Reading from {path}')
#     with fs.open(path) as f:
#         pandas_frames.append(pd.read_csv(f, header=0))

arr = os.listdir(uri)

for filename in arr:
    logging.info(f"reading file: {filename}")
    with open(os.path.join(uri, filename), "r") as handle:
        pandas_frames.append(pd.read_csv(handle, header=0))

logging.info(f'concating {len(pandas_frames)} frames')
full_df = pd.concat(pandas_frames)

# pull out EGV events only
egv_events = full_df[full_df['Event Type'] == "EGV"]
egv_events = egv_events[['Timestamp (YYYY-MM-DDThh:mm:ss)', 'Source Device ID',
       'Glucose Value (mg/dL)', 'Transmitter Time (Long Integer)', 'Transmitter ID']]

egv_events = egv_events.drop_duplicates(keep='last', subset='Timestamp (YYYY-MM-DDThh:mm:ss)')
egv_events.columns = ['timestamp_str', 'source_device', 'glucose', 'transmittertime', 'transmitter_id']

# High is an over 400 event. Hopefully these don't happen, but if they do,
# we will set to null and try to interpolate.
egv_events['glucose'].replace('High', np.nan, inplace=True)

# interpolate gaps (linear) to get clean 5 mins 
egv_events['timestamp'] = pd.to_datetime(egv_events['timestamp_str'], format='%Y-%m-%dT%H:%M:%S')
egv_events['timestamp_diff'] = egv_events.timestamp.diff()

gaps = egv_events[egv_events.timestamp_diff >= np.timedelta64(6, 'm')].index

new_time_slots = []

for low, high in zip(gaps-1, gaps):
    start_event = egv_events.loc[low]
    end_event = egv_events.loc[high]
    start = start_event.timestamp
    end = end_event.timestamp
    start_time = start
    previous_start_time = start
    transmitter_id = start_event['transmitter_id'] if start_event['transmitter_id'] == end_event['transmitter_id'] else "Switch"
    while end - start_time >= np.timedelta64(5, 'm'):
        start_time += np.timedelta64(5, 'm')
        new_time_slots.append({
            'timestamp_str': str(start_time), 
            'source_device': 'Interpolate',
            'glucose': np.nan,
            'transmittertime': -1,
            'transmitter_id': transmitter_id,
            'timestamp': start_time,
            'timestamp_diff': start_time - previous_start_time
        })
        previous_start_time = start_time

# create basic rows here
new_data = pd.DataFrame(new_time_slots)
egv_events_with_interpolate = pd.concat([egv_events, new_data])
egv_events_with_interpolate.sort_values(by='timestamp', inplace=True)
egv_events_with_interpolate.set_index('timestamp').glucose.interpolate(method='cubic', inplace=True)
egv_events_with_interpolate.reset_index()

# assert all diffs < 6 mins
assert egv_events_with_interpolate.timestamp.diff().max() < np.timedelta64(6, 'm')

logging.info(f"Ready to save")

egv_events_with_interpolate = egv_events_with_interpolate.drop(columns='timestamp_diff')

with open(os.path.join(output_uri, "cleanClarityTimeseries.parquet"), "wb") as output_file:
    egv_events_with_interpolate.to_parquet(output_file)
