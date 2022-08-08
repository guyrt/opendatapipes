import pandas as pd
import json
import sys

try:
    from .blob_helpers import  get_service_client
except ImportError:
    from blob_helpers import get_service_client

# Produces JSON representation of FEC's per-version column headers. 
# Expects hard coded version xls file in path below. Takes output file as cmd argument.
# The file can be found at https://www.fec.gov/data/browse-data/?tab=bulk-data

connection_strings = json.loads(open("../local.settings.json", "r").read())
connection_string = connection_strings['Values']['FecDataStorageConnectionAppSetting']

outfile = sys.argv[1]

fec_map = dict()


def proc_row(row):
    version = row[0]
    form = row[1]
    if version not in fec_map:
        fec_map[version] = dict()

    if form not in fec_map[version]:
        fec_map[version][form] = []

    for x in range(2, len(row)):
        row_element = row[x]
        if not pd.isnull(row_element):
            if '-' in row_element:
                _, name = row_element.split('-', 1)
            else:
                name = row_element

            for c in "./(){}?":
                name = name.replace(c, "")
            if '\n' in name:
                name = name.split('\n')[0]
            name = name.replace("\u2026", "")

            name = name.replace(" ", "_")
            name = name.replace("-", "_")
            name = name.lower()
            if name in fec_map[version][form]:
                name = f'{name}_copy'
            fec_map[version][form].append(name)


df = pd.read_excel("../../metadata/e-filing headers all versions.xlsx", sheet_name="all versions", header=None)

df.apply(proc_row, axis=1)

strmap = json.dumps(fec_map, indent=2)

f = open(outfile, "w+")
f.write(strmap)
f.close()

# upload!
f_read = open(outfile, 'rb')
service_client = get_service_client(connection_string)
upload_client = service_client.get_blob_client(container='metadata', blob='rawparserdata.json')
upload_client.upload_blob(f_read, overwrite=True)
