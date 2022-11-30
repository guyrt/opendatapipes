import argparse
import datetime
import logging
import json
import requests
import shutil
import tempfile
import zipfile

from os import listdir, makedirs
from os.path import basename, dirname, join

from pyarrow import csv, parquet, string

output_delimiter = '\t'


class SchemaHandler(object):

    def __init__(self, definitions):
        self.feclookup = definitions
        self.schema_cache : dict[str, dict[str, list[str]]]= {}  # file_version => (line type => schema)

    def get_schema(self, version, linetype):
        linetype = linetype.upper()
        if linetype in ['H1', 'H2', 'H3', 'H4', 'H5', 'H6', 'H7']:
            # handle weird data bug
            linetype = 'S' + linetype
        
        versioned_formdata = self.feclookup['v' + version]
        i = len(linetype)

        while i >= 0 and linetype[:i] not in versioned_formdata:
            i -= 1

        if not linetype:
            raise Exception("Could not match linetype {0} on version {1}".format(linetype, version))

        final_key = linetype[:i]
        return final_key, versioned_formdata.get(final_key, dict())

    def get_schema_string(self, fileversion, clean_linetype):
        if fileversion not in self.schema_cache:
            self.schema_cache[fileversion] = {}

        if clean_linetype not in self.schema_cache[fileversion]:
            if clean_linetype == 'error':
                self.schema_cache[fileversion][clean_linetype] = output_delimiter.join(['clean_linetype', 'upload_date', 'linetype', 'error', 'filename']).encode()
            else:
                _, schema = self.get_schema(fileversion, clean_linetype)
                schema = list(schema)
                schema.insert(0, 'upload_date')
                schema.insert(0, 'clean_linetype')
                schema.append('filename')
                self.schema_cache[fileversion][clean_linetype] = output_delimiter.join(schema).encode()

        final_value = self.schema_cache[fileversion][clean_linetype]
        return final_value


class LowMemoryFecFileParser(object):
    """
    Given a file from the FEC, apply correct definitions.
    """

    def __init__(self, schema_handler, upload_date, line_aggregator):
        self.schema_handler = schema_handler
        self.upload_date = upload_date
        self.line_aggregator = line_aggregator
        self.schema_cache : dict[str, dict[str, list[str]]]= {}  # file_version => (line type => schema)

    def processfile(self, filehandle, filename):
        """
        Process all lines of a file and list of dictionaries, one per line.
        """
        first_line = filehandle.readline()
        first_line = first_line.replace('"', '').strip().split(chr(28))
        if first_line[0] != "HDR":
            raise Exception("Failed to parse: HDR expected on first line")

        fileversion = first_line[2].strip()

        in_comment = False

        for line in filehandle:
            line = line.strip()
            line = line.replace('"', '')

            if not line:
                continue
            
            if line == '[BEGINTEXT]':
                in_comment = True
                continue
            elif in_comment:
                if line == '[ENDTEXT]':
                    in_comment = False
                continue

            line : list[str] = line.split(chr(28))
            line = [l.replace(output_delimiter, ' ') for l in line]
            linetype = line[0]
            clean_linetype, schema = self.schema_handler.get_schema(fileversion, linetype)
            
            # Send the line to right place.
            if schema:
                if len(schema) < len(line):
                    line = line[:len(schema)]
                while len(line) < len(schema):
                    line.append('')

                line.insert(0, self.upload_date)
                line.insert(0, clean_linetype)
                line.append(basename(filename))
            else:
                clean_linetype = 'error'
                logging.info(f"Error row: {line}")
                line = [clean_linetype, self.upload_date, linetype, "NoSchema", filename]

            line_out = output_delimiter.join(line)
            self.write_line(clean_linetype, line_out, fileversion)

    def summarize_schema_cache(self):
        for file_version, cache in self.schema_cache.items():
            for line_type, final_cache in cache.items():
                cache_s = final_cache.decode().split(output_delimiter)
                cache_size = len(cache_s)
                logging.info(f"{file_version}, {line_type}, {cache_size}")

    def write_line(self, clean_linetype : str, line : str, fileversion : str):
        self.line_aggregator.write(clean_linetype, line, fileversion)

    def finalize(self):
        self.line_aggregator.finalize()


class LineAggregator(object):

    def __init__(self, schema_handler, dateprefix, converter_handler):
        self.schema_handler = schema_handler
        self.dateprefix = dateprefix
        self.file_pointers : dict[str, tempfile.TemporaryFile] = {}  # lineType -> fp
        self.file_sizes : dict[str, int] = {}  # lineType -> current file size 
        self.converter_handler = converter_handler

        self.file_size_limit = 1024 * 1024 * 200

    def write(self, clean_linetype : str, line : str, file_version: str):
        # Note that original_schema does not contain our added columns.
        # It's used in converter to force null types to behave. Our added columns already do.
        schema_str = self.schema_handler.get_schema_string(file_version, clean_linetype)
        if not self._get_file(file_version, clean_linetype):
            self._set_file(file_version, clean_linetype, schema_str)

        line = line + '\n'
        line = line.encode()
        self.file_sizes[clean_linetype] += self._get_file(file_version, clean_linetype).write(line)
        
        if self.file_sizes[clean_linetype] > self.file_size_limit:
            _, original_schema = self.schema_handler.get_schema(file_version, clean_linetype)
            self.converter_handler.convert(clean_linetype, self._get_file(file_version, clean_linetype), original_schema)
            self._set_file(file_version, clean_linetype, schema_str)

    def _get_file(self, file_version : str, clean_linetype : str):
        version_pointers = self.file_pointers.get(file_version)
        if not version_pointers:
            return None
        return version_pointers.get(clean_linetype)

    def _set_file(self, fileversion : str, clean_linetype : str, schema_str : str):
        if fileversion not in self.file_pointers:
            self.file_pointers[fileversion] = {}

        if clean_linetype in self.file_pointers[fileversion]:
            del self.file_pointers[fileversion][clean_linetype]

        local_file_handle = tempfile.TemporaryFile()

        self.file_sizes[clean_linetype] = local_file_handle.write(schema_str)
        local_file_handle.write('\n'.encode())
        self.file_pointers[fileversion][clean_linetype] = local_file_handle
        
        return local_file_handle

    def finalize(self):
        for fileversion, pointers in self.file_pointers.items():
            for clean_linetype, file_pointer in pointers.items():
                _, original_schema = self.schema_handler.get_schema(fileversion, clean_linetype)
                self.converter_handler.convert(clean_linetype, file_pointer, original_schema)
        self.file_pointers = {}
        self.file_sizes = {}


class ParquetConverter(object):

    def __init__(self, root_folder, date_pattern) -> None:
        self.root_folder = root_folder
        self.date_pattern = date_pattern
        self.counter = 0
        self.files = {}  # line type => filenames

    def convert(self, line_type: str, file_pointer, original_schema):
        """Convert a delimited file to parquet"""
        file_pointer.flush()
        file_pointer.seek(0)

        column_opts_dict = {}
        for col in original_schema:
            column_opts_dict[col] = string()

        try:
            df = csv.read_csv(
                file_pointer, 
                parse_options=csv.ParseOptions(delimiter=output_delimiter), 
                convert_options=csv.ConvertOptions(column_types=column_opts_dict)
            )
        except Exception:
            logging.info(f"Failed on {line_type}. Dumping")

            emh = open('/tmp/emergency_dump.csv', 'wb')
            file_pointer.seek(0)
            emh.write(file_pointer.read())
            emh.close()

            raise

        local_filename = join(self.root_folder, line_type, f'{self.date_pattern}_{self.counter}.snappy.parquet')
        self.counter += 1
        makedirs(dirname(local_filename), exist_ok=True)
        new_fp = open(local_filename, 'wb')

        parquet.write_table(df, new_fp, flavor='spark')
        new_fp.close()
        return local_filename   


    def consolidate(self):
        """For each line type, consolidate the folder to a single parquet file and remove nulls"""
        pass



def build_parser(fec_definitions, parquet_root, date_pattern):
    utc_timestamp = str(datetime.datetime.utcnow())
    schema_handler = SchemaHandler(fec_definitions)
    parquet_convert = ParquetConverter(parquet_root, date_pattern)
    line_aggregator = LineAggregator(schema_handler, date_pattern, parquet_convert)
    return LowMemoryFecFileParser(schema_handler, utc_timestamp, line_aggregator)


def upload(local_path):
    logging.info(f'Uploading {local_path} to {output_folder_uri}')
    for filename in listdir(local_path):
        output_uri = join(output_folder_uri, filename)
        with open(output_uri, 'wb') as out_file:
            with open(filename, 'rb') as in_file:
                out_file.write(in_file.read())


def get_tempdir(reason):
    td = tempfile.TemporaryDirectory()
    logging.info(f"Got {td.name} for {reason}")
    return td


def process_file(fec_definitions, files, datepattern, unzip_tempdir):
    parquet_folder = get_tempdir("parquet_folder")
    parser = build_parser(fec_definitions, parquet_folder.name, datepattern)

    logging.info(f"Starting to operate on {len(files)} files")
    try:
        for rawfilename in files:
            # run in blocks of 100 files, or otherwise watch for full disk. On full/100 then run convert/upload then cleanup/recreate
            # your temp space.
            filename = join(unzip_tempdir.name, rawfilename)
            fh = open(filename, 'r', errors='ignore')
            parser.processfile(fh, filename)
        parser.finalize()
        upload(parquet_folder.name)
    finally:
        logging.info("Cleaning up")
        parquet_folder.cleanup()
    

def download_file(url, local_folder_name):
    local_file_name = join(local_folder_name, "localzip.zip")
    with requests.get(url, stream=True) as r:
        with open(local_file_name, 'wb') as local_file:
            shutil.copyfileobj(r.raw, local_file)
    return local_file_name

# inputs:
# date to run on
# optional end date

# outputs:
# folder with files

parser = argparse.ArgumentParser()
parser.add_argument("--run_date", type=str)
parser.add_argument("--metadata_dataset", type=str)

parser.add_argument("--unzipped_fec_files", type=str)
args = parser.parse_args()

datepattern = args.run_date
local_metadata_dataset = args.metadata_dataset
output_folder_uri = args.unzipped_fec_files

logging.info(f"Working on {datepattern}")

#ds = Dataset.File.from_files((datastore, f'electronic/{datepattern}.zip'))
#downloaded_files = ds.download()

# todo - download file from FEC from the above address.
fec_url = f'https://cg-519a459a-0ea3-42c2-b7bc-fa1143481f74.s3-us-gov-west-1.amazonaws.com/bulk-downloads/electronic/{datepattern}.zip'
first_download_folder = get_tempdir("first_download")
downloaded_file_name = download_file(fec_url, first_download_folder.name)

unzip_tempdir = get_tempdir("raw_zip")
with zipfile.ZipFile(open(downloaded_file_name, 'rb')) as zipObj:
    zipObj.extractall(unzip_tempdir.name)

fec_definitions = json.loads(open(local_metadata_dataset, 'r').read())
files = listdir(unzip_tempdir.name)
process_file(fec_definitions, files, datepattern, unzip_tempdir)
unzip_tempdir.cleanup()
