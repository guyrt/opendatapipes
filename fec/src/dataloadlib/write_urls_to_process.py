import json
import datetime

daily_patterns = [
    {
        'pattern': 'https://cg-519a459a-0ea3-42c2-b7bc-fa1143481f74.s3-us-gov-west-1.amazonaws.com/bulk-downloads/electronic/{0}.zip',
        'blobpath': 'electronic/{0}.zip'
    },
    {
        'pattern': 'https://cg-519a459a-0ea3-42c2-b7bc-fa1143481f74.s3-us-gov-west-1.amazonaws.com/bulk-downloads/paper/{0}.zip',
        'blobpath': 'paper/{0}.zip'
    }   
]


year_patterns = [
    {
        'pattern': 'https://www.fec.gov/files/bulk-downloads/{0}/weball{1}.zip',
        'blobpath': 'weball/{0}.zip'
    },
    {
        'pattern': 'https://www.fec.gov/files/bulk-downloads/{0}/cn{1}.zip',
        'blobpath': 'candidates/{0}.zip'
    },
    {
        'pattern': 'https://www.fec.gov/files/bulk-downloads/{0}/ccl{1}.zip',
        'blobpath': 'candidatecommittee/{0}.zip'
    },
    {
        'pattern': 'https://www.fec.gov/files/bulk-downloads/{0}/webl{1}.zip',
        'blobpath': 'housesenate/{0}.zip'
    },
    {
        'pattern': 'https://www.fec.gov/files/bulk-downloads/{0}/cm{1}.zip',
        'blobpath': 'committee/{0}.zip'
    },
    {
        'pattern': 'https://www.fec.gov/files/bulk-downloads/{0}/webk{1}.zip',
        'blobpath': 'pacsummary/{0}.zip'
    },
    {
        'pattern': 'https://www.fec.gov/files/bulk-downloads/{0}/webk{1}.zip',
        'blobpath': 'pacsummary/{0}.zip'
    }
]


def write_daily(datepattern : str) -> None:
    """Expect a string like yyyymmdd and produce all FEC bulk data URLs to process."""
    messages = []
    for pattern in daily_patterns:
        message = json.dumps({
            'pattern': pattern['pattern'].format(datepattern),
            'blobpath': pattern['blobpath'].format(datepattern),
            'datepattern': datepattern
        })
        messages.append(message)
    return messages


def write_annual(year_pattern: str) -> None:
    year_pattern_short = year_pattern[2:]
    messages = []
    for pattern in year_patterns:
        message = json.dumps({
            'pattern': pattern['pattern'].format(year_pattern, year_pattern_short),
            'blobpath': pattern['blobpath'].format(year_pattern),
            'yearpattern': year_pattern
        })
        messages.append(message)
    return messages


def write_daily_range(start, end) -> None:
    messages = []
    sdate = datetime.datetime.strptime(start, '%Y%m%d')
    edate = datetime.datetime.strptime(end, '%Y%m%d')
    dates_in_range = (sdate + datetime.timedelta(days=x) for x in range((edate - sdate).days))
    for dt in dates_in_range:
        messages.extend(write_daily(dt.strftime('%Y%m%d')))
    return messages
