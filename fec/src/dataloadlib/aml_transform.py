# This is used in a notebook - will eventually be pushed into git as a notebook.

import datetime
from os.path import join, dirname
from os import makedirs
import tempfile
from pyarrow import csv, parquet

output_delimiter = '\t'


class LowMemoryFecFileParser(object):
    """
    Given a file from the FEC, apply correct definitions.
    """

    def __init__(self, definitions, upload_date, line_aggregator):
        self.feclookup = definitions
        self.upload_date = upload_date
        self.line_aggregator = line_aggregator
        self.schema_cache : dict[str, dict[str, list[str]]]= {}  # file_version => (line type => schema)

    def getschema(self, version, linetype):
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
            clean_linetype, schema = self.getschema(fileversion, linetype)
            
            # Send the line to right place.
            if schema:
                if len(schema) < len(line):
                    line = line[:len(schema)]
                while len(line) < len(schema):
                    line.append('')

                line.insert(0, self.upload_date)
                line.insert(0, clean_linetype)
            else:
                clean_linetype = 'error'
                print(f"Error row: {line}")
                line = [clean_linetype, self.upload_date, linetype, "NoSchema", filename]

            line_out = output_delimiter.join(line)
            self.write_line(clean_linetype, line_out, self.get_schema_string(fileversion, clean_linetype))

    def get_schema_string(self, fileversion, clean_linetype):
        if fileversion not in self.schema_cache:
            self.schema_cache[fileversion] = {}

        if clean_linetype not in self.schema_cache[fileversion]:
            if clean_linetype == 'error':
                self.schema_cache[fileversion][clean_linetype] = output_delimiter.join(['clean_linetype', 'upload_date', 'linetype', 'error', 'filename']).encode()
            else:
                _, schema = self.getschema(fileversion, clean_linetype)
                schema = list(schema)
                schema.insert(0, 'upload_date')
                schema.insert(0, 'clean_linetype')
                self.schema_cache[fileversion][clean_linetype] = output_delimiter.join(schema).encode()

        final_value = self.schema_cache[fileversion][clean_linetype]
        return final_value

    def summarize_schema_cache(self):
        for file_version, cache in self.schema_cache.items():
            for line_type, final_cache in cache.items():
                cache_s = final_cache.decode().split(output_delimiter)
                cache_size = len(cache_s)
                print(f"{file_version}, {line_type}, {cache_size}")

    def write_line(self, clean_linetype : str, line : str, schema_string):
        self.line_aggregator.write(clean_linetype, line, schema_string)

    def finalize(self):
        self.line_aggregator.finalize()


class LineAggregator(object):

    def __init__(self, dateprefix, converter_handler):
        self.dateprefix = dateprefix
        self.file_pointers : dict[str, tempfile.TemporaryFile]= {}  # lineType -> fp
        self.file_sizes : dict[str, int] = {}  # lineType -> current file size 
        self.converter_handler = converter_handler

        self.file_size_limit = 1024 * 1024 * 200

    def write(self, clean_linetype : str, line : str, schema: str):
        if clean_linetype not in self.file_pointers:
            self._set_file(clean_linetype, schema)
        line = line + '\n'
        line = line.encode()
        self.file_sizes[clean_linetype] += self.file_pointers[clean_linetype].write(line)
        
        if self.file_sizes[clean_linetype] > self.file_size_limit:
            self.converter_handler.convert(clean_linetype, self.file_pointers[clean_linetype])
            self._set_file(clean_linetype, schema)

    def _set_file(self, clean_linetype, schema):
        if clean_linetype in self.file_pointers:
            del self.file_pointers[clean_linetype]

        local_file_handle = tempfile.TemporaryFile()

        self.file_sizes[clean_linetype] = local_file_handle.write(schema)
        local_file_handle.write('\n'.encode())
        self.file_pointers[clean_linetype] = local_file_handle
        
        return local_file_handle

    def finalize(self):
        for clean_linetype, file_pointer in self.file_pointers.items():
            self.converter_handler.convert(clean_linetype, file_pointer)
        self.file_pointers = {}
        self.file_sizes = {}


class ParquetConverter(object):

    def __init__(self, root_folder, date_pattern) -> None:
        self.root_folder = root_folder
        self.date_pattern = date_pattern
        self.counter = 0

    def convert(self, line_type: str, file_pointer):
        """Convert a delimited file to parquet"""
        print(f"Converting {line_type}")
        file_pointer.flush()
        file_pointer.seek(0)
        try:
            df = csv.read_csv(file_pointer, parse_options=csv.ParseOptions(delimiter=output_delimiter))
        except Exception:
            print(f"Failed on {line_type}. Dumping")

            emh = open('/tmp/emergency_dump.csv', 'wb')
            file_pointer.seek(0)
            emh.write(file_pointer.read())
            emh.close()

            raise

        local_filename = join(self.root_folder, line_type, f'{self.date_pattern}_{self.counter}.snappy.parquet')
        self.counter += 1
        makedirs(dirname(local_filename), exist_ok=True)
        new_fp = open(local_filename, 'wb')

        parquet.write_table(df, new_fp)
        new_fp.close()
        return local_filename   

def build_parser(fec_definitions, parquet_root, date_pattern):
    utc_timestamp = str(datetime.datetime.utcnow())
    parquet_convert = ParquetConverter(parquet_root, date_pattern)
    line_aggregator = LineAggregator(date_pattern, parquet_convert)
    return LowMemoryFecFileParser(fec_definitions, utc_timestamp, line_aggregator)


def upload(local_path):
    remote_path = f'amluploads/electronic/'
    print(f'Uploading {local_path} to {remote_path}')
    output_datastore.upload(local_path, remote_path, overwrite=True, show_progress=False)


def get_tempdir(reason):
    td = tempfile.TemporaryDirectory()
    print(f"Got {td.name} for {reason}")
    return td

def process_file(files, datepattern, unzip_tempdir):
    parquet_folder = get_tempdir("parquet_folder")
    parser = build_parser(fec_definitions, parquet_folder.name, datepattern)

    print(f"Starting to operate on {len(files)} files")
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
        print("Cleaning up")
        parquet_folder.cleanup()
    