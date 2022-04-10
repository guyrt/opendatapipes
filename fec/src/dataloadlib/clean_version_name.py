import pandas as pd
import json
import sys

# Produces JSON representation of FEC's per-version column headers. 
# Expects hard coded version xls file in path below. Takes output file as cmd argument.
# The file can be found at https://www.fec.gov/data/browse-data/?tab=bulk-data

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
            name = name.replace(".", "")
            name = name.replace("/", "")
            name = name.replace("(", "")
            name = name.replace(")", "")
            if '\n' in name:
                name = name.split('\n')[0]
            name = name.replace("\u2026", "")

            name = name.replace(" ", "_")
            name = name.replace("?", "")
            name = name.replace("-", "_")
            fec_map[version][form].append(name)


df = pd.read_excel("../../metadata/e-filing headers all versions.xlsx", sheet_name="all versions", header=None)

df.apply(proc_row, axis=1)

strmap = json.dumps(fec_map, indent=2)

f = open(outfile, "w")
f.write(strmap)
