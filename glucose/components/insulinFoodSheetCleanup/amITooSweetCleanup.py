from typing import Dict
import pandas as pd
import numpy as np
import pytz
from datetime import datetime
import re


def read_sheet(sheet_uri : str) -> Dict[str, pd.DataFrame]:
    sheet_names = ["glucose"]
    return {n: pd.read_excel(sheet_uri, engine='openpyxl') for n in sheet_names}


def cleanup_date(raw_date : np.dtype('O')) -> datetime | float:
    """
    My sheet is gross apparently. We have the following known formats:
    YYYY-DD-MM 00:00:00   (yes month/day reversed)
    MM/DD/YYYY
    MM:DD/YYYY (typo)
    """
    if pd.isna(raw_date):
        return raw_date
    date_s = str(raw_date)

    re1 = r'^(?P<year>\d{4})-(?P<day>\d{2})-(?P<month>\d{2}) 00:00:00$'
    m = re.match(re1, date_s)
    if not m:
        re2 = r'^(?P<month>\d{1,2})[:/](?P<day>\d{2})[:/](?P<year>\d{4})$'
        m = re.match(re2, date_s)
    if not m:
        raise Exception(f"Unable to convert date {raw_date}")

    return datetime(int(m.group('year')), int(m.group('month')), int(m.group('day')))


def clean_glucose_as_pandas(glucose_df : pd.DataFrame) -> pd.DataFrame:
    
    df = glucose_df[['Date', 'Time', 'Type', 'Units', 'Notes']]
    df.Date = df.Date.apply(cleanup_date)
    df.Date = df.Date.interpolate(method='pad')

    def fix_timezones(s):
        if pd.isna(s):
            return s
        s = s.lower()
        if 'time zone' in s:
            return s.replace('time zone', '').strip().upper()
        return np.nan

    df["timezone"] = df.Notes.apply(fix_timezones)
    df.at[0, 'timezone'] = 'PST' # fix first value

    df['had_timezone'] = pd.isna(df.timezone) == False

    df.timezone = df.timezone.interpolate(method='pad')

    tz_correct = {
        'PST': 'America/Los_Angeles',
        'CST': 'US/Central'
    }

    def build_timestamp(row):
        s = f"{row['Date'].strftime('%Y-%m-%d')}T{row['Time'].isoformat()}"
        tz = pytz.timezone(tz_correct.get(row['timezone'], row['timezone']))
        d = datetime.strptime(s, '%Y-%m-%dT%H:%M:00')
        return tz.localize(d)

    df['timestamp'] = df.apply(build_timestamp, axis=1)
    return df


if __name__ == "__main__":
    sheets = read_sheet("/tmp/local_copy.xlsx")
    glucose = clean_glucose_as_pandas(sheets['glucose'])
    import pdb; pdb.set_trace()
    a = 1

