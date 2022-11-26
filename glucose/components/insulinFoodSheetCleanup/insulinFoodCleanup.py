#!/usr/bin/env python
# coding: utf-8

import argparse
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import pytz
import os

parser = argparse.ArgumentParser()
parser.add_argument("--raw_spreadsheet", type=str)
parser.add_argument("--insulin_injections", type=str)
parser.add_argument("--timezone_switches", type=str)
args = parser.parse_args()

uri = args.raw_spreadsheet
output_insulin = args.insulin_injections
output_timezones = args.timezone_switches

df = pd.read_excel(uri)
df = df[['Date', 'Time', 'Type', 'Units', 'Notes']]


def fix_timezones(s):
    if pd.isna(s):
        return s
    s = s.lower()
    if 'time zone' in s:
        return s.replace('time zone', '').strip().upper()
    return np.nan

df.Date = df.Date.interpolate(method='pad')

df["timezone"] = df.Notes.apply(fix_timezones)
df.at[0, 'timezone'] = 'PST' # fix first value

df['had_timezone'] = pd.isna(df.timezone) == False

df.timezone = df.timezone.interpolate(method='pad')

tz_correct = {
    'PST': 'America/Los_Angeles'
}

def build_timestamp(row):
    s = f"{row['Date'].strftime('%Y-%m-%d')}T{row['Time'].isoformat()}"
    tz = pytz.timezone(tz_correct.get(row['timezone'], row['timezone']))
    d = datetime.strptime(s, '%Y-%m-%dT%H:%M:00')
    return tz.localize(d)
    
df['timestamp'] = df.apply(build_timestamp, axis=1)


df_tz_points = df[df.had_timezone]
df_tz_points = df_tz_points[['timezone', 'timestamp']]

df = df.drop(columns='had_timezone')


# This is a sanity check that our values are in order. 
# I've caught a few data errors this way:
#  * wrong TZ info
#  * bad date format
#  * forgot to use 24 time.
assert df.timestamp.diff().min() > timedelta(0)

# Save outputs
with open(os.path.join(output_insulin, 'insulin_output.parquet'), 'wb') as insulin_output:
    df.to_parquet(insulin_output)

with open(os.path.join(output_timezones, 'timezone_changes.csv'), 'w') as tz_output:
    df_tz_points.to_csv(tz_output)
