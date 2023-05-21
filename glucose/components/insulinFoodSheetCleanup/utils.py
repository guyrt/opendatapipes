import json
from numpyencoder import NumpyEncoder


def save_as_json(rows, path):
    with open(path, 'w') as fh:
        for row in rows:
            for d in ['Date', 'Time']:
                if d in row:
                    del row[d]
            row['timestamp'] = str(row['timestamp'])
            
            d = json.dumps(row, cls=NumpyEncoder)
            fh.write(d)
            fh.write("\n")
