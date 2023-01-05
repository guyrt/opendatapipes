import argparse
from os.path import join

parser = argparse.ArgumentParser()
parser.add_argument("--run_date", type=str)
parser.add_argument("--sa_status", type=str)
parser.add_argument("--sb_status", type=str)
parser.add_argument("--sc_status", type=str)
parser.add_argument("--sd_status", type=str)
parser.add_argument("--sh_status", type=str)

parser.add_argument("--status_out_path", type=str)

args = parser.parse_args()

datepattern = args.run_date
output_folder_uri = args.status_out_path

output_uri = join(output_folder_uri, f"{datepattern}.txt")
with open(output_uri, 'w') as fh:
    fh.write(f"Completed")
