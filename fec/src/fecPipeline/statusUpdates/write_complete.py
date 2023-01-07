import argparse
import datetime
from os.path import join

parser = argparse.ArgumentParser()
parser.add_argument("--unzipped_fec_files", type=str)
parser.add_argument("--sa_status", type=str)
parser.add_argument("--sb_status", type=str)
parser.add_argument("--sc_status", type=str)
parser.add_argument("--sd_status", type=str)
parser.add_argument("--se_status", type=str)
parser.add_argument("--sh_status", type=str)

parser.add_argument("--status_out_path", type=str)

args = parser.parse_args()

input_file = join(args.unzipped_fec_files, "dates.txt")
datepatterns = [line.strip() for line in open(input_file, 'r')]

output_folder_uri = args.status_out_path

print(f"Running on output_folder {output_folder_uri} and date {datepatterns}")

for datepattern in datepatterns:
    output_uri = join(output_folder_uri, f"{datepattern}.txt")
    with open(output_uri, 'w') as fh:
        fh.write(f"Completed {str(datetime.datetime.now())}")
