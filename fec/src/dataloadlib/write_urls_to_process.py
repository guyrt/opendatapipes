import json

daily_patterns = [
    'https://cg-519a459a-0ea3-42c2-b7bc-fa1143481f74.s3-us-gov-west-1.amazonaws.com/bulk-downloads/electronic/{0}.zip',
    'https://cg-519a459a-0ea3-42c2-b7bc-fa1143481f74.s3-us-gov-west-1.amazonaws.com/bulk-downloads/paper/{0}.zip'
]


year_patterns = [
    'https://www.fec.gov/files/bulk-downloads/{0}/weball{1}.zip',
    'https://www.fec.gov/files/bulk-downloads/{0}/cn{1}.zip',
    'https://www.fec.gov/files/bulk-downloads/{0}/ccl{1}.zip',
    # todo
]


def write_daily(datepattern : str, out_queue, force_override : bool = False) -> None:
    """Expect a string like yyyymmdd and produce all FEC bulk data URLs to process."""
    for pattern in daily_patterns:
        full_pattern = pattern.format(datepattern)
        message = json.dumps({
            'pattern': full_pattern,
            'override': force_override
        })
        out_queue.set(message)


def write_annual(year_pattern: str, out_queue) -> None:
    year_pattern_short = year_pattern[2:]
    for pattern in year_patterns:
        full_pattern = pattern.format(year_pattern, year_pattern_short)
        message = json.dumps({
            'pattern': full_pattern
        })
        out_queue.set(message)
